import json
import hashlib
import hmac
import logging
import signal
import time
import uuid
from datetime import datetime
from typing import Dict, Optional, Tuple, Union
from dataclasses import dataclass

from confluent_kafka import Consumer, Producer, KafkaError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class FlutterwaveTokeniserConfig:
    bootstrap_servers: str = "localhost:29092"
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str = "PLAIN"
    sasl_username: str = ""
    sasl_password: str = ""

    input_topic: str = "flutter_raw"
    output_topic: str = "flutter_tokenised"
    dlq_topic: str = "flutter_dlq"

    consumer_group: str = "flutter_tokeniser_group"

    hmac_secret: str = "CHANGE_ME_IN_PRODUCTION"
    max_cache_size: int = 100_000
    max_poll_interval_ms: int = 300000


class FlutterwaveTokeniser:
    """
    Flutterwave Tokeniser Service
    Consumes raw Flutterwave webhook messages from 'flutter_raw' topic
    Tokenises PAN (first 6 + last 4) and sensitive customer data
    Produces tokenised transactions to 'flutter_tokenised' topic
    """

    def __init__(self, config: FlutterwaveTokeniserConfig = None):
        self.config = config or FlutterwaveTokeniserConfig()
        self.consumer = None
        self.producer = None
        self.running = False

        # Token caches
        self.card_token_cache: Dict[str, str] = {}  # PAN → token
        self.customer_token_cache: Dict[str, str] = {}  # customer email/phone → token
        self.ref_token_cache: Dict[str, str] = {}  # transaction reference → token

        self.stats = {
            'total_consumed': 0,
            'total_tokenised': 0,
            'total_dlq': 0,
            'total_errors': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'card_tokens': 0,
            'customer_tokens': 0,
            'tokenisation_total_ms': 0.0,
        }

    # Setup

    def setup(self):
        self._setup_consumer()
        self._setup_producer()
        logger.info("Flutterwave Tokeniser ready")

    def _setup_consumer(self):
        config = {
            'bootstrap.servers': self.config.bootstrap_servers,
            'group.id': self.config.consumer_group,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'max.poll.interval.ms': self.config.max_poll_interval_ms,
            'session.timeout.ms': 30000,
            'heartbeat.interval.ms': 10000,
        }
        if self.config.security_protocol != "PLAINTEXT":
            config['security.protocol'] = self.config.security_protocol
            config['sasl.mechanism'] = self.config.sasl_mechanism
            config['sasl.username'] = self.config.sasl_username
            config['sasl.password'] = self.config.sasl_password

        self.consumer = Consumer(config)
        self.consumer.subscribe([self.config.input_topic])
        logger.info(f"Subscribed to: {self.config.input_topic}")

    def _setup_producer(self):
        config = {
            'bootstrap.servers': self.config.bootstrap_servers,
            'acks': 'all',
            'retries': 3,
            'retry.backoff.ms': 500,
            'enable.idempotence': True,
            'compression.type': 'snappy',
            'linger.ms': 20,
            'batch.size': 32768,
        }
        if self.config.security_protocol != "PLAINTEXT":
            config['security.protocol'] = self.config.security_protocol
            config['sasl.mechanism'] = self.config.sasl_mechanism
            config['sasl.username'] = self.config.sasl_username
            config['sasl.password'] = self.config.sasl_password

        self.producer = Producer(config)
        logger.info("Producer ready")

    # Card Validation

    def validate_pan(self, first6: str, last4: str) -> Tuple[bool, Optional[str]]:
        """Validate Flutterwave card details (partial PAN)"""
        if not first6 or not last4:
            return False, "First 6 or last 4 digits missing"

        if not first6.isdigit() or len(first6) != 6:
            return False, f"BIN must be 6 digits, got {len(first6)}"

        if not last4.isdigit() or len(last4) != 4:
            return False, f"Last4 must be 4 digits, got {len(last4)}"

        return True, None

    def validate_customer_email(self, email: str) -> Tuple[bool, Optional[str]]:
        """Validate customer email format"""
        if not email:
            return True, None  # Email is optional

        if '@' not in email or '.' not in email:
            return False, f"Invalid email format: {email}"

        return True, None

    # Tokenisation

    def _cache_key(self, value: str, prefix: str = "flutter") -> str:
        """Derive non-reversible cache key"""
        return hmac.new(
            self.config.hmac_secret.encode(),
            f"{prefix}:{value}".encode(),
            hashlib.sha256
        ).hexdigest()

    def tokenise_card(self, first6: str, last4: str, flw_ref: str = None) -> Tuple[Optional[str], bool]:
        """
        Tokenise Flutterwave card data (partial PAN)
        Format: FLW_{first6}_TOKEN_{hash}_{last4}
        Preserves BIN and last4 for analytics
        """
        if not first6 or not last4:
            return None, False

        composite_key = f"{first6}:{last4}:{flw_ref}" if flw_ref else f"{first6}:{last4}"
        cache_key = self._cache_key(composite_key, "card")

        if cache_key in self.card_token_cache:
            self.stats['cache_hits'] += 1
            return self.card_token_cache[cache_key], True

        self.stats['cache_misses'] += 1
        start = time.perf_counter()

        # Generate deterministic token for the middle part
        token_hash = hmac.new(
            self.config.hmac_secret.encode(),
            composite_key.encode(),
            hashlib.sha256
        ).hexdigest()[:12].upper()

        # Format: FLW_{BIN}_TOKEN_{HASH}_{LAST4}
        token = f"FLW_{first6}_TOKEN_{token_hash}_{last4}"

        # Evict oldest 20% if cache is full
        if len(self.card_token_cache) >= self.config.max_cache_size:
            evict_count = self.config.max_cache_size // 5
            for key in list(self.card_token_cache.keys())[:evict_count]:
                del self.card_token_cache[key]

        self.card_token_cache[cache_key] = token
        self.stats['card_tokens'] += 1
        self.stats['tokenisation_total_ms'] += (time.perf_counter() - start) * 1000

        return token, False

    def tokenise_customer(self, customer_id: str, email: str = None, phone: str = None) -> Tuple[Optional[str], bool]:
        """
        Tokenise customer identifier
        Format: FLW_CUST_{hash}
        """
        if not customer_id and not email and not phone:
            return None, False

        composite_key = f"{customer_id}:{email}:{phone}" if customer_id else f"{email}:{phone}"
        cache_key = self._cache_key(composite_key, "customer")

        if cache_key in self.customer_token_cache:
            return self.customer_token_cache[cache_key], True

        start = time.perf_counter()

        token_hash = hmac.new(
            self.config.hmac_secret.encode(),
            composite_key.encode(),
            hashlib.sha256
        ).hexdigest()[:16].upper()

        token = f"FLW_CUST_{token_hash}"

        if len(self.customer_token_cache) >= self.config.max_cache_size:
            evict_count = self.config.max_cache_size // 5
            for key in list(self.customer_token_cache.keys())[:evict_count]:
                del self.customer_token_cache[key]

        self.customer_token_cache[cache_key] = token
        self.stats['customer_tokens'] += 1
        self.stats['tokenisation_total_ms'] += (time.perf_counter() - start) * 1000

        return token, False

    def tokenise_reference(self, ref: str) -> Tuple[Optional[str], bool]:
        """
        Tokenise transaction reference for tracking
        Format: FLW_REF_{hash}
        """
        if not ref:
            return None, False

        cache_key = self._cache_key(ref, "ref")

        if cache_key in self.ref_token_cache:
            return self.ref_token_cache[cache_key], True

        start = time.perf_counter()

        token_hash = hmac.new(
            self.config.hmac_secret.encode(),
            ref.encode(),
            hashlib.sha256
        ).hexdigest()[:12].upper()

        token = f"FLW_REF_{token_hash}"

        if len(self.ref_token_cache) >= self.config.max_cache_size:
            evict_count = self.config.max_cache_size // 5
            for key in list(self.ref_token_cache.keys())[:evict_count]:
                del self.ref_token_cache[key]

        self.ref_token_cache[cache_key] = token
        self.stats['tokenisation_total_ms'] += (time.perf_counter() - start) * 1000

        return token, False

    # Message Parsing

    def parse_flutterwave_message(self, raw_message: Union[str, Dict]) -> Optional[Dict]:
        """
        Parse Flutterwave webhook message.
        Handles both JSON string and dict input.
        """
        try:
            # Handle both string and dict input
            if isinstance(raw_message, str):
                tx = json.loads(raw_message)
            elif isinstance(raw_message, dict):
                tx = raw_message
            else:
                logger.error(f"Unknown message type: {type(raw_message)}")
                return None

            # Extract webhook_payload if present (from test data)
            if 'webhook_payload' in tx:
                tx = tx['webhook_payload']

            # Check if it's a Flutterwave webhook
            event = tx.get('event', tx.get('event_type', ''))
            data = tx.get('data', tx)

            # Extract card details
            card_data = data.get('card', {})
            customer_data = data.get('customer', {})

            # Get first 6 and last 4 from card
            first6 = card_data.get('first_6digits', '')
            last4 = card_data.get('last_4digits', '')

            # If no card data, try to get from authorization
            if not first6 and not last4:
                auth_data = data.get('authorization', {})
                first6 = auth_data.get('first_6digits', '')
                last4 = auth_data.get('last_4digits', '')

            # Extract customer info
            customer_id = str(customer_data.get('id', ''))
            customer_email = customer_data.get('email', '')
            customer_phone = customer_data.get('phone_number', '')

            # Extract transaction details
            tx_ref = data.get('tx_ref', tx.get('tx_ref', ''))
            flw_ref = data.get('flw_ref', tx.get('flw_ref', ''))
            amount = float(data.get('amount', tx.get('amount', 0)))
            currency = data.get('currency', tx.get('currency', 'USD'))
            status = data.get('status', tx.get('status', ''))

            # Determine payment type
            payment_type = data.get('payment_type', tx.get('payment_type', 'card'))
            charge_type = data.get('charge_type', '')

            parsed = {
                'transaction_id': flw_ref or tx_ref or str(uuid.uuid4()),
                'source': 'FLUTTERWAVE',
                'source_type': 'WEBHOOK',
                'event': event,
                'tx_ref': tx_ref,
                'flw_ref': flw_ref,
                'timestamp': datetime.now().isoformat(),
                'event_time_ms': int(time.time() * 1000),

                # Card details (partial)
                'first6': first6,
                'last4': last4,

                # Customer details
                'customer_id': customer_id,
                'customer_email': customer_email,
                'customer_phone': customer_phone,

                # Transaction details
                'amount': amount,
                'currency': currency,
                'status': status,
                'payment_type': payment_type,
                'charge_type': charge_type,

                # Additional metadata
                'device_fingerprint': data.get('device_fingerprint'),
                'auth_model': data.get('auth_model'),
                'processor_response': data.get('processor_response'),
            }

            return parsed

        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error: {e}")
            return None
        except Exception as e:
            logger.error(f"Parse error: {e}")
            return None

    def _build_tokenised_message(self, parsed: Dict, card_token: str,
                                 customer_token: str, ref_token: str,
                                 from_cache: bool) -> Dict:
        """Build tokenised output message"""

        return {
            'transaction_id': parsed['transaction_id'],
            'source': parsed['source'],
            'source_type': parsed['source_type'],
            'event': parsed.get('event'),
            'tx_ref': parsed.get('tx_ref'),
            'flw_ref': parsed.get('flw_ref'),
            'timestamp': parsed['timestamp'],
            'event_time_ms': parsed['event_time_ms'],
            'tokenisation_timestamp': datetime.now().isoformat(),

            # Tokenised fields
            'card_token': card_token,
            'customer_token': customer_token,
            'ref_token': ref_token,
            'token_from_cache': from_cache,
            'tokenisation_method': 'HMAC_SHA256_PSEUDONYMISATION',

            # Non-sensitive transaction data
            'amount': parsed.get('amount'),
            'currency': parsed.get('currency'),
            'status': parsed.get('status'),
            'payment_type': parsed.get('payment_type'),
            'charge_type': parsed.get('charge_type'),

            # Analytics fields (preserved for features)
            'bin': parsed.get('first6'),
            'last4': parsed.get('last4'),

            # Device and auth info
            'device_fingerprint': parsed.get('device_fingerprint'),
            'auth_model': parsed.get('auth_model'),
            'processor_response': parsed.get('processor_response'),

            # Customer email/phone (non-sensitive - already pseudonymised by token)
            'customer_email_hash': hashlib.md5(parsed.get('customer_email', '').encode()).hexdigest()[
                :16] if parsed.get('customer_email') else None,
        }

    def process_message(self, message) -> bool:
        """Process a single Flutterwave message"""
        try:
            if message.value() is None:
                logger.warning("Empty message — skipping")
                return False

            # Parse the message - handle both string and dict
            raw_value = message.value()

            # If it's bytes, decode to string
            if isinstance(raw_value, bytes):
                raw_value = raw_value.decode('utf-8')

            # Try to parse as JSON if it's a string, otherwise use as dict
            if isinstance(raw_value, str):
                try:
                    tx_data = json.loads(raw_value)
                except json.JSONDecodeError as e:
                    self._send_to_dlq(message, f"JSON parse error: {e}")
                    return False
            else:
                tx_data = raw_value

            # Parse the message
            parsed = self.parse_flutterwave_message(tx_data)

            if not parsed:
                self._send_to_dlq(message, "Parse failure - unknown format")
                return False

            # Validate and tokenise card data
            first6 = parsed.get('first6')
            last4 = parsed.get('last4')
            flw_ref = parsed.get('flw_ref')

            card_token = None
            if first6 and last4:
                valid, error = self.validate_pan(first6, last4)
                if not valid:
                    self._send_to_dlq(message, f"Card validation failed: {error}")
                    return False
                card_token, from_cache = self.tokenise_card(first6, last4, flw_ref)
            else:
                from_cache = False
                # No card data - use reference as fallback
                if flw_ref:
                    card_token, from_cache = self.tokenise_reference(flw_ref)

            # Tokenise customer
            customer_id = parsed.get('customer_id')
            customer_email = parsed.get('customer_email')
            customer_phone = parsed.get('customer_phone')
            customer_token, _ = self.tokenise_customer(customer_id, customer_email, customer_phone)

            # Tokenise reference
            ref_token, _ = self.tokenise_reference(parsed.get('tx_ref'))

            output = self._build_tokenised_message(parsed, card_token, customer_token, ref_token, from_cache)

            # Use card token as partition key for consistent routing
            key = (card_token or customer_token or ref_token or parsed['transaction_id']).encode('utf-8')

            self.producer.produce(
                topic=self.config.output_topic,
                key=key,
                value=json.dumps(output).encode('utf-8'),
                callback=self._delivery_callback
            )
            self.producer.poll(0)

            self.stats['total_consumed'] += 1
            self.stats['total_tokenised'] += 1

            # Commit every 10 messages
            if self.stats['total_consumed'] % 10 == 0:
                self.consumer.commit(asynchronous=True)
                self.print_stats()

            logger.debug(f"Tokenised {parsed['transaction_id']}")
            return True

        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            self._send_to_dlq(message, f"Unexpected error: {e}")
            return False

    def _send_to_dlq(self, message, reason: str):
        """Send failed message to Dead Letter Queue"""
        try:
            # Get raw message value
            raw_value = message.value()
            if isinstance(raw_value, bytes):
                raw_value = raw_value.decode('utf-8')

            payload = {
                'original_message': raw_value if raw_value else None,
                'error_reason': reason,
                'failed_at': datetime.now().isoformat(),
                'topic': message.topic(),
                'partition': message.partition(),
                'offset': message.offset(),
                'service': 'flutterwave-tokeniser'
            }
            self.producer.produce(
                topic=self.config.dlq_topic,
                value=json.dumps(payload).encode('utf-8'),
                callback=self._delivery_callback
            )
            self.producer.poll(0)
            self.stats['total_dlq'] += 1
            logger.warning(f"DLQ: {reason}")
        except Exception as e:
            logger.error(f"Failed to write DLQ: {e}")
            self.stats['total_errors'] += 1

    def _delivery_callback(self, err, msg):
        """Kafka delivery callback"""
        if err:
            logger.error(f"Delivery failed: {err}")
            self.stats['total_errors'] += 1
        else:
            logger.debug(f"Delivered → {msg.topic()}[{msg.partition()}]@{msg.offset()}")

    # Statistics

    def print_stats(self):
        """Print current statistics"""
        total_cache = self.stats['cache_hits'] + self.stats['cache_misses']
        hit_rate = self.stats['cache_hits'] / total_cache if total_cache > 0 else 0
        avg_ms = (
            self.stats['tokenisation_total_ms'] / self.stats['cache_misses']
            if self.stats['cache_misses'] > 0 else 0
        )

        logger.info("── Flutterwave Tokeniser Stats ──────────────────")
        logger.info(f"  Consumed:        {self.stats['total_consumed']}")
        logger.info(f"  Tokenised:       {self.stats['total_tokenised']}")
        logger.info(f"  DLQ:             {self.stats['total_dlq']}")
        logger.info(f"  Errors:          {self.stats['total_errors']}")
        logger.info(f"  Card tokens:     {self.stats['card_tokens']}")
        logger.info(f"  Customer tokens: {self.stats['customer_tokens']}")
        logger.info(f"  Cache hit rate:  {self.stats['cache_hits']}/{total_cache} ({hit_rate:.1%})")
        logger.info(f"  Avg token time:  {avg_ms:.3f} ms (cache misses)")
        logger.info("────────────────────────────────────────────────")

    # Lifecycle

    def run(self):
        """Main consumer loop"""
        self.running = True
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        logger.info(f"Consuming: {self.config.input_topic} → {self.config.output_topic}")
        last_stats = time.time()

        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug("Partition EOF")
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        self.stats['total_errors'] += 1
                    continue

                success = self.process_message(msg)

                if success:
                    # Commit after successful processing
                    self.consumer.commit(message=msg, asynchronous=True)

                if time.time() - last_stats >= 60:
                    self.print_stats()
                    last_stats = time.time()

        except KeyboardInterrupt:
            pass
        except Exception as e:
            logger.error(f"Fatal: {e}", exc_info=True)
        finally:
            self.shutdown()

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Signal {signum} received — stopping")
        self.running = False

    def shutdown(self):
        """Clean shutdown"""
        logger.info("Shutting down Flutterwave Tokeniser...")
        self.print_stats()
        if self.producer:
            self.producer.flush(timeout=10)
        if self.consumer:
            self.consumer.close()
        logger.info("Flutterwave Tokeniser stopped")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Flutterwave Tokeniser')
    parser.add_argument('--bootstrap-servers', default='localhost:29092')
    parser.add_argument('--hmac-secret', default='CHANGE_ME_IN_PRODUCTION')
    parser.add_argument('--input-topic', default='flutter_raw')
    parser.add_argument('--output-topic', default='flutter_tokenised')
    parser.add_argument('--dlq-topic', default='flutter_dlq')
    args = parser.parse_args()

    config = FlutterwaveTokeniserConfig(
        bootstrap_servers=args.bootstrap_servers,
        hmac_secret=args.hmac_secret,
        input_topic=args.input_topic,
        output_topic=args.output_topic,
        dlq_topic=args.dlq_topic,
    )

    tokeniser = FlutterwaveTokeniser(config)
    tokeniser.setup()
    tokeniser.run()