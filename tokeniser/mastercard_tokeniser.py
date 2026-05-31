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
class MastercardTokeniserConfig:
    bootstrap_servers: str = "localhost:9092"
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str = "PLAIN"
    sasl_username: str = ""
    sasl_password: str = ""

    input_topic: str = "mastercard_raw"
    output_topic: str = "mastercard_tokenised"
    dlq_topic: str = "mastercard_dlq"

    consumer_group: str = "mastercard_tokeniser_group"

    hmac_secret: str = "CHANGE_ME_IN_PRODUCTION"
    max_cache_size: int = 100_000
    max_poll_interval_ms: int = 300000


class MastercardTokeniser:
    """
    Mastercard Tokeniser Service
    Consumes raw Mastercard ISO 8583 messages from 'mastercard_raw' topic
    Tokenises PAN using format-preserving tokenisation
    Produces tokenised transactions to 'mastercard_tokenised' topic
    """

    def __init__(self, config: MastercardTokeniserConfig = None):
        self.config = config or MastercardTokeniserConfig()
        self.consumer = None
        self.producer = None
        self.running = False

        # Token cache
        self.token_cache: Dict[str, str] = {}  # PAN hash → token

        self.stats = {
            'total_consumed': 0,
            'total_tokenised': 0,
            'total_dlq': 0,
            'total_errors': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'tokenisation_total_ms': 0.0,
        }

    # Setup

    def setup(self):
        self._setup_consumer()
        self._setup_producer()
        logger.info("Mastercard Tokeniser ready")

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

    # PAN Validation

    def validate_pan(self, pan: str) -> Tuple[bool, Optional[str]]:
        """Validate Mastercard PAN format and Luhn algorithm"""
        if not pan:
            return False, "PAN is empty"

        pan = pan.replace(" ", "").replace("-", "")

        if not pan.isdigit():
            return False, "PAN must contain only digits"

        # Mastercard PAN length: 16 digits (typically)
        if len(pan) != 16:
            return False, f"Mastercard PAN length {len(pan)} invalid (must be 16)"

        # Mastercard BIN ranges: 51-55, 2221-2720
        if not (pan.startswith(('51', '52', '53', '54', '55')) or
                (2221 <= int(pan[:4]) <= 2720)):
            return False, f"PAN does not start with valid Mastercard BIN"

        # Luhn algorithm validation
        total = 0
        reverse = pan[::-1]
        for i, digit in enumerate(reverse):
            n = int(digit)
            if i % 2 == 1:  # every second digit from the right
                n *= 2
                if n > 9:
                    n -= 9
            total += n

        if total % 10 != 0:
            return False, "PAN failed Luhn validation"

        return True, None

    # Tokenisation

    def _cache_key(self, pan: str) -> str:
        """Derive non-reversible cache key from PAN"""
        return hmac.new(
            self.config.hmac_secret.encode(),
            f"mastercard:{pan}".encode(),
            hashlib.sha256
        ).hexdigest()

    def tokenise_pan(self, pan: str) -> Tuple[Optional[str], bool]:
        """
        Format-preserving tokenisation for Mastercard PAN.
        Preserves BIN (first 6) and last 4 digits.
        Tokenises the middle digits.
        Returns (token, from_cache)
        """
        if not pan:
            return None, False

        pan = pan.replace(" ", "").replace("-", "")
        cache_key = self._cache_key(pan)

        if cache_key in self.token_cache:
            self.stats['cache_hits'] += 1
            return self.token_cache[cache_key], True

        self.stats['cache_misses'] += 1
        start = time.perf_counter()

        # Extract BIN and last4
        bin6 = pan[:6]
        last4 = pan[-4:]
        middle = pan[6:-4]  # Should be 6 digits (16 - 6 - 4 = 6)

        # Generate deterministic token for middle digits
        middle_hash = hmac.new(
            self.config.hmac_secret.encode(),
            pan.encode(),  # Hash full PAN for determinism
            hashlib.sha256
        ).hexdigest()[:len(middle)].upper()

        # Ensure token contains only digits for format preservation
        digit_token = ""
        for ch in middle_hash:
            if ch.isdigit():
                digit_token += ch
            else:
                # Convert hex letters to digits (A=1, B=2, etc.)
                digit_token += str((ord(ch) - ord('A') + 1) % 10)

        # Format-preserving token
        token = f"{bin6}{digit_token}{last4}"

        # Evict oldest 20% if cache is full
        if len(self.token_cache) >= self.config.max_cache_size:
            evict_count = self.config.max_cache_size // 5
            for key in list(self.token_cache.keys())[:evict_count]:
                del self.token_cache[key]

        self.token_cache[cache_key] = token
        self.stats['tokenisation_total_ms'] += (time.perf_counter() - start) * 1000

        return token, False

    # Message Parsing

    def parse_mastercard_message(self, raw_message: Union[str, Dict]) -> Optional[Dict]:
        """
        Parse Mastercard ISO 8583 message.
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

            # Check for different message wrappers
            if 'bitmap' in tx:
                bitmap = tx['bitmap']
            else:
                bitmap = tx

            # Extract PAN from DE002
            pan = bitmap.get('DE002') or tx.get('pan')

            if not pan:
                logger.warning("No PAN found in message")
                return None

            # Currency code mapping
            currency_code = bitmap.get('DE049', tx.get('currency_code', '404'))

            return {
                'transaction_id': tx.get('transaction_id', str(uuid.uuid4())),
                'source': tx.get('source', 'MASTERCARD_ISO8583'),
                'message_type': tx.get('message_type', bitmap.get('message_type')),
                'timestamp': tx.get('timestamp', datetime.now().isoformat()),
                'event_time_ms': tx.get('event_time_ms', int(time.time() * 1000)),
                'pan': pan,
                'bin': pan[:6] if len(pan) >= 6 else None,
                'last4': pan[-4:] if len(pan) >= 4 else None,
                'amount': float(bitmap.get('DE004', tx.get('amount', 0))),
                'currency_code': currency_code,
                'merchant_id': bitmap.get('DE042', tx.get('merchant_id')),
                'terminal_id': bitmap.get('DE041', tx.get('terminal_id')),
                'merchant_name': bitmap.get('DE043', tx.get('merchant_name')),
                'mcc': bitmap.get('DE018', tx.get('mcc')),
                'transaction_type': bitmap.get('DE003', tx.get('transaction_type')),
                'pos_entry_mode': bitmap.get('DE022', tx.get('pos_entry_mode')),
                'stan': bitmap.get('DE011', tx.get('stan')),  # System Trace Audit Number
                'acquirer_id': bitmap.get('DE032', tx.get('acquirer_id')),
                'forwarder_id': bitmap.get('DE033', tx.get('forwarder_id')),
                'original_timestamp': tx.get('original_timestamp'),
                'original_event_time_ms': tx.get('original_event_time_ms')
            }

        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error: {e}")
            return None
        except Exception as e:
            logger.error(f"Parse error: {e}")
            return None

    def _currency_code_to_str(self, code: str) -> str:
        """Convert ISO currency code to currency string"""
        currency_map = {
            '404': 'KES',  # Kenyan Shilling
            '840': 'USD',  # US Dollar
            '826': 'GBP',  # British Pound
            '978': 'EUR',  # Euro
            '710': 'ZAR',  # South African Rand
            '800': 'UGX',  # Ugandan Shilling
            '834': 'TZS',  # Tanzanian Shilling
            '124': 'CAD',  # Canadian Dollar
            '392': 'JPY',  # Japanese Yen
            '756': 'CHF',  # Swiss Franc
        }
        return currency_map.get(code, 'USD')

    def _build_tokenised_message(self, parsed: Dict, token: str, from_cache: bool) -> Dict:
        """Build tokenised output message"""
        return {
            'transaction_id': parsed['transaction_id'],
            'source': parsed['source'],
            'source_type': 'ISO_8583',
            'message_type': parsed.get('message_type'),
            'timestamp': parsed['timestamp'],
            'event_time_ms': parsed['event_time_ms'],
            'tokenisation_timestamp': datetime.now().isoformat(),

            # Tokenised card data (PAN replaced with token)
            'card_token': token,
            'bin': parsed['bin'],
            'last4': parsed['last4'],
            'token_from_cache': from_cache,
            'tokenisation_method': 'FORMAT_PRESERVING_HMAC',

            # Transaction data
            'amount': parsed['amount'],
            'currency_code': parsed['currency_code'],
            'currency': self._currency_code_to_str(parsed['currency_code']),
            'merchant_id': parsed['merchant_id'],
            'merchant_name': parsed['merchant_name'],
            'terminal_id': parsed['terminal_id'],
            'mcc': parsed['mcc'],
            'transaction_type': parsed['transaction_type'],
            'pos_entry_mode': parsed['pos_entry_mode'],
            'stan': parsed['stan'],
            'acquirer_id': parsed['acquirer_id'],
            'forwarder_id': parsed.get('forwarder_id'),

            # Enrichment placeholder
            'enrichment': {},
            'is_international': False
        }

    def process_message(self, message) -> bool:
        """Process a single Mastercard message"""
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
            parsed = self.parse_mastercard_message(raw_value)

            if not parsed:
                self._send_to_dlq(message, "Parse failure - unknown format")
                return False

            # Validate and tokenise PAN
            pan = parsed.get('pan')
            if not pan:
                self._send_to_dlq(message, "PAN not found in message")
                return False

            valid, error = self.validate_pan(pan)
            if not valid:
                self._send_to_dlq(message, f"PAN validation failed: {error}")
                return False

            token, from_cache = self.tokenise_pan(pan)
            if not token:
                self._send_to_dlq(message, "Tokenisation returned None")
                return False

            output = self._build_tokenised_message(parsed, token, from_cache)

            # Use token as partition key for consistent routing
            key = token.encode('utf-8')

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
            elif isinstance(raw_value, dict):
                raw_value = json.dumps(raw_value)

            payload = {
                'original_message': raw_value if raw_value else None,
                'error_reason': reason,
                'failed_at': datetime.now().isoformat(),
                'topic': message.topic(),
                'partition': message.partition(),
                'offset': message.offset(),
                'service': 'mastercard-tokeniser'
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

        logger.info("── Mastercard Tokeniser Stats ────────────────────")
        logger.info(f"  Consumed:        {self.stats['total_consumed']}")
        logger.info(f"  Tokenised:       {self.stats['total_tokenised']}")
        logger.info(f"  DLQ:             {self.stats['total_dlq']}")
        logger.info(f"  Errors:          {self.stats['total_errors']}")
        logger.info(f"  Cache hit rate:  {self.stats['cache_hits']}/{total_cache} ({hit_rate:.1%})")
        logger.info(f"  Avg token time:  {avg_ms:.3f} ms (cache misses)")
        logger.info("──────────────────────────────────────────────────")

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
        logger.info("Shutting down Mastercard Tokeniser...")
        self.print_stats()
        if self.producer:
            self.producer.flush(timeout=10)
        if self.consumer:
            self.consumer.close()
        logger.info("Mastercard Tokeniser stopped")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Mastercard Tokeniser')
    parser.add_argument('--bootstrap-servers', default='localhost:9092')
    parser.add_argument('--hmac-secret', default='CHANGE_ME_IN_PRODUCTION')
    parser.add_argument('--input-topic', default='mastercard_raw')
    parser.add_argument('--output-topic', default='mastercard_tokenised')
    parser.add_argument('--dlq-topic', default='mastercard_dlq')
    args = parser.parse_args()

    config = MastercardTokeniserConfig(
        bootstrap_servers=args.bootstrap_servers,
        hmac_secret=args.hmac_secret,
        input_topic=args.input_topic,
        output_topic=args.output_topic,
        dlq_topic=args.dlq_topic,
    )

    tokeniser = MastercardTokeniser(config)
    tokeniser.setup()
    tokeniser.run()