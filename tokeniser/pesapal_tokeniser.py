import json
import hashlib
import hmac
import logging
import signal
import time
import uuid
import re
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
class PesapalTokeniserConfig:
    bootstrap_servers: str = "localhost:9092"
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str = "PLAIN"
    sasl_username: str = ""
    sasl_password: str = ""

    input_topic: str = "pesapal_raw"
    output_topic: str = "pesapal_tokenised"
    dlq_topic: str = "pesapal_dlq"

    consumer_group: str = "pesapal_tokeniser_group"

    hmac_secret: str = "CHANGE_ME_IN_PRODUCTION"
    max_cache_size: int = 100_000
    max_poll_interval_ms: int = 300000


class PesapalTokeniser:
    """
    Pesapal Tokeniser Service
    Consumes raw Pesapal IPN (Instant Payment Notification) callbacks from 'pesapal_raw' topic
    Tokenises customer email, phone number, and payment account
    Produces tokenised transactions to 'pesapal_tokenised' topic
    """

    def __init__(self, config: PesapalTokeniserConfig = None):
        self.config = config or PesapalTokeniserConfig()
        self.consumer = None
        self.producer = None
        self.running = False

        # Token caches
        self.email_cache: Dict[str, str] = {}  # Email → token
        self.phone_cache: Dict[str, str] = {}  # Phone → token
        self.account_cache: Dict[str, str] = {}  # Payment account → token
        self.merchant_cache: Dict[str, str] = {}  # Merchant reference → token

        self.stats = {
            'total_consumed': 0,
            'total_tokenised': 0,
            'total_dlq': 0,
            'total_errors': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'email_tokenised': 0,
            'phone_tokenised': 0,
            'account_tokenised': 0,
            'merchant_tokenised': 0,
            'tokenisation_total_ms': 0.0,
        }

    # Setup

    def setup(self):
        self._setup_consumer()
        self._setup_producer()
        logger.info("Pesapal Tokeniser ready")

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

    # Validation Methods

    def validate_email(self, email: str) -> Tuple[bool, Optional[str]]:
        """Validate email format"""
        if not email:
            return False, "Email is empty"

        email = email.strip().lower()

        # Basic email regex
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, email):
            return False, f"Invalid email format: {email}"

        return True, None

    def validate_phone(self, phone: str) -> Tuple[bool, Optional[str]]:
        """Validate phone number format (supports Kenyan and international)"""
        if not phone:
            return False, "Phone is empty"

        phone = phone.strip().replace(" ", "").replace("+", "")

        # Check length (8-15 digits typical for phone numbers)
        if not (8 <= len(phone) <= 15):
            return False, f"Phone length {len(phone)} invalid (must be 8-15)"

        # Check if contains only digits
        if not phone.isdigit():
            return False, "Phone must contain only digits"

        return True, None

    def validate_payment_account(self, account: str) -> Tuple[bool, Optional[str]]:
        """Validate payment account (could be card, mobile money, or bank account)"""
        if not account:
            return False, "Payment account is empty"

        account = account.strip()

        # Basic validation - should not be empty
        if len(account) < 4:
            return False, "Payment account too short"

        return True, None

    def normalise_phone(self, phone: str) -> str:
        """Normalise phone to E.164 format (254XXXXXXXXX)"""
        if not phone:
            return None

        phone = phone.strip().replace(" ", "").replace("+", "")

        # Convert 07XXXXXXXX to 2547XXXXXXXX
        if phone.startswith('0') and len(phone) == 10:
            phone = '254' + phone[1:]

        # Convert 7XXXXXXXX to 2547XXXXXXXX
        if phone.startswith('7') and len(phone) == 9:
            phone = '254' + phone

        return phone

    # Tokenisation Methods

    def _cache_key(self, value: str, prefix: str = "pesapal") -> str:
        """Derive non-reversible cache key"""
        return hmac.new(
            self.config.hmac_secret.encode(),
            f"{prefix}:{value}".encode(),
            hashlib.sha256
        ).hexdigest()

    def tokenise_email(self, email: str) -> Tuple[Optional[str], bool]:
        """Tokenise customer email address"""
        if not email:
            return None, False

        email = email.strip().lower()
        cache_key = self._cache_key(email, "email")

        if cache_key in self.email_cache:
            self.stats['cache_hits'] += 1
            return self.email_cache[cache_key], True

        self.stats['cache_misses'] += 1
        start = time.perf_counter()

        # Generate deterministic token
        token_hash = hmac.new(
            self.config.hmac_secret.encode(),
            email.encode(),
            hashlib.sha256
        ).hexdigest()[:16].upper()

        token = f"PESAPAL_EMAIL_{token_hash}"

        # Evict oldest 20% if cache is full
        if len(self.email_cache) >= self.config.max_cache_size:
            evict_count = self.config.max_cache_size // 5
            for key in list(self.email_cache.keys())[:evict_count]:
                del self.email_cache[key]

        self.email_cache[cache_key] = token
        self.stats['email_tokenised'] += 1
        self.stats['tokenisation_total_ms'] += (time.perf_counter() - start) * 1000

        return token, False

    def tokenise_phone(self, phone: str) -> Tuple[Optional[str], bool]:
        """Tokenise customer phone number"""
        if not phone:
            return None, False

        phone = self.normalise_phone(phone)
        if not phone:
            return None, False

        cache_key = self._cache_key(phone, "phone")

        if cache_key in self.phone_cache:
            self.stats['cache_hits'] += 1
            return self.phone_cache[cache_key], True

        self.stats['cache_misses'] += 1
        start = time.perf_counter()

        # Generate deterministic token
        token_hash = hmac.new(
            self.config.hmac_secret.encode(),
            phone.encode(),
            hashlib.sha256
        ).hexdigest()[:16].upper()

        token = f"PESAPAL_PHONE_{token_hash}"

        if len(self.phone_cache) >= self.config.max_cache_size:
            evict_count = self.config.max_cache_size // 5
            for key in list(self.phone_cache.keys())[:evict_count]:
                del self.phone_cache[key]

        self.phone_cache[cache_key] = token
        self.stats['phone_tokenised'] += 1
        self.stats['tokenisation_total_ms'] += (time.perf_counter() - start) * 1000

        return token, False

    def tokenise_payment_account(self, account: str) -> Tuple[Optional[str], bool]:
        """Tokenise payment account (card number, mobile money account, etc.)"""
        if not account:
            return None, False

        account = account.strip()
        cache_key = self._cache_key(account, "account")

        if cache_key in self.account_cache:
            self.stats['cache_hits'] += 1
            return self.account_cache[cache_key], True

        self.stats['cache_misses'] += 1
        start = time.perf_counter()

        # Format-preserving tokenisation for card numbers
        if account.isdigit() and len(account) >= 16:
            # Card number - preserve first 6 and last 4
            bin_part = account[:6]
            last4 = account[-4:]
            middle_len = len(account) - 10

            middle_hash = hmac.new(
                self.config.hmac_secret.encode(),
                account.encode(),
                hashlib.sha256
            ).hexdigest()[:middle_len].upper()

            # Ensure token contains only digits
            digit_token = ""
            for ch in middle_hash:
                if ch.isdigit():
                    digit_token += ch
                else:
                    digit_token += str(ord(ch) % 10)

            token = f"{bin_part}{digit_token}{last4}"
        else:
            # Non-card account (mobile money, bank account)
            token_hash = hmac.new(
                self.config.hmac_secret.encode(),
                account.encode(),
                hashlib.sha256
            ).hexdigest()[:12].upper()
            token = f"PESAPAL_ACC_{token_hash}"

        if len(self.account_cache) >= self.config.max_cache_size:
            evict_count = self.config.max_cache_size // 5
            for key in list(self.account_cache.keys())[:evict_count]:
                del self.account_cache[key]

        self.account_cache[cache_key] = token
        self.stats['account_tokenised'] += 1
        self.stats['tokenisation_total_ms'] += (time.perf_counter() - start) * 1000

        return token, False

    def tokenise_merchant_reference(self, merchant_ref: str) -> Tuple[Optional[str], bool]:
        """Tokenise merchant reference (merchant identifier)"""
        if not merchant_ref:
            return None, False

        merchant_ref = merchant_ref.strip()
        cache_key = self._cache_key(merchant_ref, "merchant")

        if cache_key in self.merchant_cache:
            return self.merchant_cache[cache_key], True

        start = time.perf_counter()

        token_hash = hmac.new(
            self.config.hmac_secret.encode(),
            merchant_ref.encode(),
            hashlib.sha256
        ).hexdigest()[:12].upper()

        token = f"PESAPAL_MERCHANT_{token_hash}"

        if len(self.merchant_cache) >= self.config.max_cache_size:
            evict_count = self.config.max_cache_size // 5
            for key in list(self.merchant_cache.keys())[:evict_count]:
                del self.merchant_cache[key]

        self.merchant_cache[cache_key] = token
        self.stats['merchant_tokenised'] += 1
        self.stats['tokenisation_total_ms'] += (time.perf_counter() - start) * 1000

        return token, False

    # Message Parsing

    def parse_pesapal_message(self, raw_message: Union[str, Dict]) -> Optional[Dict]:
        """
        Parse Pesapal IPN (Instant Payment Notification) callback message.
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

            # Check if message has ipn_payload wrapper (from test data)
            if 'ipn_payload' in tx:
                tx = tx['ipn_payload']

            # Extract source if present
            source = tx.get('source', tx.get('source_type', 'PESAPAL_IPN'))

            # Parse standard Pesapal IPN format
            return {
                'transaction_id': tx.get('pesapal_transaction_tracking_id') or tx.get('transaction_id',
                                                                                      str(uuid.uuid4())),
                'source': source,
                'source_type': 'IPN_CALLBACK',
                'merchant_reference': tx.get('pesapal_merchant_reference'),
                'tracking_id': tx.get('pesapal_transaction_tracking_id'),
                'payment_status': tx.get('payment_status', tx.get('status', 'PENDING')),
                'payment_method': tx.get('payment_method', 'UNKNOWN'),
                'amount': float(tx.get('amount', 0)),
                'currency': tx.get('currency', 'KES'),
                'created_date': tx.get('created_date'),
                'confirmation_code': tx.get('confirmation_code'),
                'payment_account': tx.get('payment_account'),
                'customer_email': tx.get('customer_email'),
                'customer_phone': tx.get('customer_phone'),
                'customer_first_name': tx.get('customer_first_name'),
                'customer_last_name': tx.get('customer_last_name'),
                'description': tx.get('description'),
                'timestamp': tx.get('timestamp', datetime.now().isoformat()),
                'event_time_ms': tx.get('event_time_ms', int(time.time() * 1000)),

                # Pesapal specific fields
                'pesapal_notification_type': tx.get('pesapal_notification_type'),
                'pesapal_merchant_reference': tx.get('pesapal_merchant_reference'),
                'pesapal_payment_id': tx.get('pesapal_payment_id'),
                'pesapal_signature': tx.get('pesapal_signature'),

                # Additional fields that might be present
                'error_code': tx.get('error_code'),
                'error_message': tx.get('error_message'),
            }

        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error: {e}")
            return None
        except Exception as e:
            logger.error(f"Parse error: {e}")
            return None

    def _build_tokenised_message(self, parsed: Dict, email_token: str,
                                 phone_token: str, account_token: str,
                                 merchant_token: str, from_cache: bool) -> Dict:
        """Build tokenised output message"""

        return {
            'transaction_id': parsed['transaction_id'],
            'source': parsed.get('source', 'PESAPAL'),
            'source_type': parsed.get('source_type', 'IPN_CALLBACK'),
            'merchant_reference_token': merchant_token,
            'tracking_id': parsed.get('tracking_id'),
            'payment_status': parsed.get('payment_status'),
            'payment_method': parsed.get('payment_method'),
            'timestamp': parsed['timestamp'],
            'event_time_ms': parsed['event_time_ms'],
            'tokenisation_timestamp': datetime.now().isoformat(),

            # Tokenised fields
            'customer_email_token': email_token,
            'customer_phone_token': phone_token,
            'payment_account_token': account_token,
            'token_from_cache': from_cache,
            'tokenisation_method': 'HMAC_SHA256_PSEUDONYMISATION',

            # Transaction data (non-sensitive)
            'amount': parsed.get('amount'),
            'currency': parsed.get('currency', 'KES'),
            'confirmation_code': parsed.get('confirmation_code'),
            'description': parsed.get('description'),

            # Customer info (non-sensitive)
            'customer_first_name': parsed.get('customer_first_name'),
            'customer_last_name': parsed.get('customer_last_name'),

            # Pesapal metadata
            'pesapal_notification_type': parsed.get('pesapal_notification_type'),
            'pesapal_payment_id': parsed.get('pesapal_payment_id'),

            # Error info if any
            'error_code': parsed.get('error_code'),
            'error_message': parsed.get('error_message'),
        }

    def process_message(self, message) -> bool:
        """Process a single Pesapal message"""
        try:
            if message.value() is None:
                logger.warning("Empty message — skipping")
                return False

            # Parse the message - handle both string and dict
            raw_value = message.value()

            # If it's bytes, decode to string
            if isinstance(raw_value, bytes):
                raw_value = raw_value.decode('utf-8')

            # Parse the message
            parsed = self.parse_pesapal_message(raw_value)

            if not parsed:
                self._send_to_dlq(message, "Parse failure - unknown format")
                return False

            # Tokenise email if present
            email = parsed.get('customer_email')
            email_token = None
            if email:
                valid, error = self.validate_email(email)
                if not valid:
                    self._send_to_dlq(message, f"Email validation failed: {error}")
                    return False
                email_token, _ = self.tokenise_email(email)

            # Tokenise phone if present
            phone = parsed.get('customer_phone')
            phone_token = None
            if phone:
                valid, error = self.validate_phone(phone)
                if not valid:
                    self._send_to_dlq(message, f"Phone validation failed: {error}")
                    return False
                phone_token, _ = self.tokenise_phone(phone)

            # Tokenise payment account if present
            account = parsed.get('payment_account')
            account_token = None
            if account:
                valid, error = self.validate_payment_account(account)
                if not valid:
                    self._send_to_dlq(message, f"Payment account validation failed: {error}")
                    return False
                account_token, _ = self.tokenise_payment_account(account)

            # Tokenise merchant reference
            merchant_ref = parsed.get('merchant_reference')
            merchant_token = None
            if merchant_ref:
                merchant_token, _ = self.tokenise_merchant_reference(merchant_ref)

            # Determine if token came from cache (use email cache status as indicator)
            from_cache = email_token in self.email_cache.values() if email_token else False

            output = self._build_tokenised_message(
                parsed, email_token, phone_token, account_token, merchant_token, from_cache
            )

            # Use tracking_id or transaction_id as partition key
            key = parsed['tracking_id'].encode('utf-8') if parsed.get('tracking_id') else parsed[
                'transaction_id'].encode('utf-8')

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
                'service': 'pesapal-tokeniser'
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

        logger.info("── Pesapal Tokeniser Stats ─────────────────────")
        logger.info(f"  Consumed:          {self.stats['total_consumed']}")
        logger.info(f"  Tokenised:         {self.stats['total_tokenised']}")
        logger.info(f"  DLQ:               {self.stats['total_dlq']}")
        logger.info(f"  Errors:            {self.stats['total_errors']}")
        logger.info(f"  Email tokens:      {self.stats['email_tokenised']}")
        logger.info(f"  Phone tokens:      {self.stats['phone_tokenised']}")
        logger.info(f"  Account tokens:    {self.stats['account_tokenised']}")
        logger.info(f"  Merchant tokens:   {self.stats['merchant_tokenised']}")
        logger.info(f"  Cache hit rate:    {self.stats['cache_hits']}/{total_cache} ({hit_rate:.1%})")
        logger.info(f"  Avg token time:    {avg_ms:.3f} ms (cache misses)")
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
        logger.info("Shutting down Pesapal Tokeniser...")
        self.print_stats()
        if self.producer:
            self.producer.flush(timeout=10)
        if self.consumer:
            self.consumer.close()
        logger.info("Pesapal Tokeniser stopped")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Pesapal Tokeniser')
    parser.add_argument('--bootstrap-servers', default='localhost:9092')
    parser.add_argument('--hmac-secret', default='CHANGE_ME_IN_PRODUCTION')
    parser.add_argument('--input-topic', default='pesapal_raw')
    parser.add_argument('--output-topic', default='pesapal_tokenised')
    parser.add_argument('--dlq-topic', default='pesapal_dlq')
    args = parser.parse_args()

    config = PesapalTokeniserConfig(
        bootstrap_servers=args.bootstrap_servers,
        hmac_secret=args.hmac_secret,
        input_topic=args.input_topic,
        output_topic=args.output_topic,
        dlq_topic=args.dlq_topic,
    )

    tokeniser = PesapalTokeniser(config)
    tokeniser.setup()
    tokeniser.run()