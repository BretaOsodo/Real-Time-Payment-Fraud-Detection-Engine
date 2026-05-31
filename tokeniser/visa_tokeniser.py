import json
import hashlib
import hmac
import logging
import signal
import sys
import time
import uuid
from datetime import datetime
from typing import Dict, Optional, Tuple
from dataclasses import dataclass

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class TokeniserConfig:
    bootstrap_servers: str = "localhost:9092"   # external Docker port
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str = "PLAIN"
    sasl_username: str = ""
    sasl_password: str = ""

    input_topic: str = "visa_raw"
    output_topic: str = "visa_tokenised"
    dlq_topic: str = "visa_dlq"

    consumer_group: str = "visa_tokeniser_group"

    hmac_secret: str = "CHANGE_ME_IN_PRODUCTION"
    max_cache_size: int = 100_000
    max_poll_interval_ms: int = 300000


class VisaTokeniser:

    def __init__(self, config: TokeniserConfig = None):
        self.config = config or TokeniserConfig()
        self.consumer = None
        self.producer = None
        self.running = False
        self.token_cache: Dict[str, str] = {}   # cache key = HMAC(PAN), value = token

        self.stats = {
            'total_consumed': 0,
            'total_tokenised': 0,
            'total_dlq': 0,
            'total_errors': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'tokenisation_total_ms': 0.0,
        }

    #Setup

    def setup(self):
        self._setup_consumer()
        self._setup_producer()
        logger.info("Visa Tokeniser ready")

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

    # PAN validation

    def validate_pan(self, pan: str) -> Tuple[bool, Optional[str]]:
        if not pan:
            return False, "PAN is empty"

        pan = pan.replace(" ", "").replace("-", "")

        if not pan.isdigit():
            return False, "PAN must contain only digits"

        if not (13 <= len(pan) <= 19):
            return False, f"PAN length {len(pan)} invalid (must be 13-19)"

        # Luhn algorithm — correct implementation
        total = 0
        reverse = pan[::-1]
        for i, digit in enumerate(reverse):
            n = int(digit)
            if i % 2 == 1:          # every second digit from the right (1-indexed)
                n *= 2
                if n > 9:
                    n -= 9
            total += n

        if total % 10 != 0:
            return False, "PAN failed Luhn validation"

        return True, None

    # Tokenisation

    def _pan_cache_key(self, pan: str) -> str:
        """
        Derive a non-reversible cache key from the PAN.
        The raw PAN is NEVER stored as a dict key.
        """
        return hmac.new(
            self.config.hmac_secret.encode(),
            pan.encode(),
            hashlib.sha256
        ).hexdigest()

    def tokenise_pan(self, pan: str) -> Tuple[Optional[str], bool]:
        """
        Format-preserving tokenisation.
        Preserves BIN (first 6) and last 4. Tokenises the middle.
        Returns (token, from_cache).
        """
        pan = pan.replace(" ", "").replace("-", "")
        cache_key = self._pan_cache_key(pan)

        if cache_key in self.token_cache:
            self.stats['cache_hits'] += 1
            return self.token_cache[cache_key], True

        self.stats['cache_misses'] += 1
        start = time.perf_counter()

        bin_part = pan[:6]
        last4 = pan[-4:]
        middle = pan[6:-4]

        # Deterministic HMAC token for the middle digits
        middle_token = hmac.new(
            self.config.hmac_secret.encode(),
            pan.encode(),           # hash the full PAN, not just middle
            hashlib.sha256
        ).hexdigest()[:len(middle)].upper()

        # Ensure token middle contains only digits (replace hex letters)
        digit_token = ""
        for ch in middle_token:
            if ch.isdigit():
                digit_token += ch
            else:
                digit_token += str(ord(ch) % 10)

        token = f"{bin_part}{digit_token}{last4}"

        # Evict oldest 20% if cache is full
        if len(self.token_cache) >= self.config.max_cache_size:
            evict_count = self.config.max_cache_size // 5
            for key in list(self.token_cache.keys())[:evict_count]:
                del self.token_cache[key]

        self.token_cache[cache_key] = token
        self.stats['tokenisation_total_ms'] += (time.perf_counter() - start) * 1000

        return token, False

    #Message processing

    def parse_visa_message(self, raw: str) -> Optional[Dict]:
        try:
            tx = json.loads(raw) if isinstance(raw, str) else raw
            bitmap = tx.get('bitmap', {})

            pan = bitmap.get('DE002')
            return {
                'transaction_id': tx.get('transaction_id', str(uuid.uuid4())),
                'source': tx.get('source', 'VISA_ISO8583'),
                'message_type': tx.get('message_type'),
                'timestamp': tx.get('timestamp', datetime.now().isoformat()),
                'event_time_ms': tx.get('event_time_ms', int(time.time() * 1000)),
                'pan': pan,
                'bin': pan[:6] if pan and len(pan) >= 6 else None,
                'last4': pan[-4:] if pan and len(pan) >= 4 else None,
                'amount': float(bitmap.get('DE004', 0)),
                'currency_code': bitmap.get('DE049', '404'),
                'merchant_id': bitmap.get('DE042'),
                'terminal_id': bitmap.get('DE041'),
                'merchant_name': bitmap.get('DE043'),
                'mcc': bitmap.get('DE018'),
                'transaction_type': bitmap.get('DE003'),
                'pos_entry_mode': bitmap.get('DE022'),
                'stan': bitmap.get('DE011'),
                'acquirer_id': bitmap.get('DE032'),
            }
        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error: {e}")
            return None
        except Exception as e:
            logger.error(f"Parse error: {e}")
            return None

    def _build_tokenised_message(self, parsed: Dict, token: str, from_cache: bool) -> Dict:
        currency_map = {'404': 'KES', '840': 'USD', '826': 'GBP', '978': 'EUR', '710': 'ZAR'}
        return {
            'transaction_id': parsed['transaction_id'],
            'source': parsed['source'],
            'message_type': parsed['message_type'],
            'timestamp': parsed['timestamp'],
            'event_time_ms': parsed['event_time_ms'],
            'tokenisation_timestamp': datetime.now().isoformat(),
            'card_token': token,                # PAN replaced — never stored
            'bin': parsed['bin'],
            'last4': parsed['last4'],
            'token_from_cache': from_cache,
            'tokenisation_method': 'FORMAT_PRESERVING_HMAC',
            'amount': parsed['amount'],
            'currency_code': parsed['currency_code'],
            'currency': currency_map.get(parsed['currency_code'], 'KES'),
            'merchant_id': parsed['merchant_id'],
            'merchant_name': parsed['merchant_name'],
            'terminal_id': parsed['terminal_id'],
            'mcc': parsed['mcc'],
            'transaction_type': parsed['transaction_type'],
            'pos_entry_mode': parsed['pos_entry_mode'],
            'stan': parsed['stan'],
            'acquirer_id': parsed['acquirer_id'],
        }

    def process_message(self, message) -> bool:
        try:
            if message.value() is None:
                logger.warning("Empty message — skipping")
                return False

            raw = message.value().decode('utf-8')
            parsed = self.parse_visa_message(raw)

            if not parsed:
                self._send_to_dlq(message, "Parse failure")
                return False

            pan = parsed.get('pan')
            valid, error = self.validate_pan(pan)
            if not valid:
                self._send_to_dlq(message, f"PAN validation failed: {error}")
                return False

            token, from_cache = self.tokenise_pan(pan)
            if not token:
                self._send_to_dlq(message, "Tokenisation returned None")
                return False

            output = self._build_tokenised_message(parsed, token, from_cache)

            self.producer.produce(
                topic=self.config.output_topic,
                key=token.encode('utf-8'),    # full token as partition key
                value=json.dumps(output).encode('utf-8'),
                callback=self._delivery_callback
            )
            self.producer.poll(0)
            self.producer.flush(0.1)

            self.stats['total_consumed'] += 1
            self.stats['total_tokenised'] += 1
            logger.debug(f"Tokenised {parsed['transaction_id']}")
            return True

        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            self._send_to_dlq(message, f"Unexpected error: {e}")
            return False

    def _send_to_dlq(self, message, reason: str):
        try:
            payload = {
                'original_message': message.value().decode('utf-8') if message.value() else None,
                'error_reason': reason,
                'failed_at': datetime.now().isoformat(),
                'topic': message.topic(),
                'partition': message.partition(),
                'offset': message.offset(),
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

    def _delivery_callback(self, err, msg):
        if err:
            logger.error(f"Delivery failed: {err}")
            self.stats['total_errors'] += 1
        else:
            logger.debug(f"Delivered → {msg.topic()}[{msg.partition()}]@{msg.offset()}")

    # Stats

    def print_stats(self):
        processed = self.stats['total_tokenised']
        cache_total = self.stats['cache_hits'] + self.stats['cache_misses']
        avg_ms = (
            self.stats['tokenisation_total_ms'] / self.stats['cache_misses']
            if self.stats['cache_misses'] > 0 else 0
        )
        logger.info("── Tokeniser stats ──────────────────────────")
        logger.info(f"  Consumed:    {self.stats['total_consumed']}")
        logger.info(f"  Tokenised:   {processed}")
        logger.info(f"  DLQ:         {self.stats['total_dlq']}")
        logger.info(f"  Errors:      {self.stats['total_errors']}")
        logger.info(f"  Cache hits:  {self.stats['cache_hits']} / {cache_total} "
                    f"({self.stats['cache_hits']/cache_total:.1%} hit rate)" if cache_total else "  Cache: no data")
        logger.info(f"  Avg token:   {avg_ms:.3f} ms (cache misses only)")
        logger.info("─────────────────────────────────────────────")

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def run(self):
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
        logger.info(f"Signal {signum} received — stopping")
        self.running = False

    def shutdown(self):
        logger.info("Shutting down...")
        self.print_stats()
        if self.producer:
            self.producer.flush(timeout=10)
        if self.consumer:
            self.consumer.close()
        logger.info("Stopped")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Visa Tokeniser')
    parser.add_argument('--bootstrap-servers', default='localhost:9092')
    parser.add_argument('--hmac-secret', default='CHANGE_ME_IN_PRODUCTION')
    args = parser.parse_args()

    config = TokeniserConfig(
        bootstrap_servers=args.bootstrap_servers,
        hmac_secret=args.hmac_secret,
        input_topic="visa_raw",
        output_topic="visa_tokenised",
        dlq_topic="visa_dlq",
    )

    tokeniser = VisaTokeniser(config)
    tokeniser.setup()
    tokeniser.run()