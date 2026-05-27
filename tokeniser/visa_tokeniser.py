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
import threading

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================
# VISA TOKENISER CONFIGURATION
# ============================================

@dataclass
class TokeniserConfig:
    """Configuration for Visa Tokeniser"""
    # Kafka configuration
    bootstrap_servers: str = "localhost:9092"
    security_protocol: str = "SASL_SSL"  # or "PLAINTEXT" for dev
    sasl_mechanism: str = "PLAIN"
    sasl_username: str = ""
    sasl_password: str = ""

    # Topics
    input_topic: str = "visa_raw"
    output_topic: str = "visa_tokenised"
    dlq_topic: str = "visa_dlq"

    # Consumer group
    consumer_group: str = "visa_tokeniser_group"

    # Tokenisation settings
    hmac_secret: str = "CHANGE_ME_IN_PRODUCTION"
    cache_ttl_seconds: int = 86400  # 24 hours
    vault_mount_point: str = "transform"
    vault_role_name: str = "payments"

    # Performance
    batch_size: int = 100
    batch_timeout_ms: int = 1000
    max_poll_interval_ms: int = 300000


class VisaTokeniser:
    """
    Visa ISO 8583 Tokeniser Service

    Consumes raw Visa transactions from Kafka, tokenises PAN,
    and produces tokenised transactions to output topic.

    Flow:
        visa_raw (Kafka) → consume → tokenise PAN → produce → visa_tokenised (Kafka)
                                    ↓
                                visa_dlq (on error)
    """

    def __init__(self, config: TokeniserConfig = None):
        self.config = config or TokeniserConfig()
        self.consumer = None
        self.producer = None
        self.running = False

        # Token cache for performance (reduce repeated tokenisation)
        self.token_cache = {}

        # Statistics
        self.stats = {
            'total_consumed': 0,
            'total_tokenised': 0,
            'total_dlq': 0,
            'total_errors': 0,
            'avg_tokenisation_ms': 0,
            'cache_hit_rate': 0,
            'cache_hits': 0,
            'cache_misses': 0
        }

        # Luhn table for PAN validation
        self.luhn_table = [0, 2, 4, 6, 8, 1, 3, 5, 7, 9]

    def setup(self):
        """Setup Kafka consumer and producer"""
        self.setup_consumer()
        self.setup_producer()
        logger.info("Visa Tokeniser setup complete")

    def setup_consumer(self):
        """Setup Kafka consumer for visa_raw topic"""

        consumer_config = {
            'bootstrap.servers': self.config.bootstrap_servers,
            'group.id': self.config.consumer_group,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,  # Manual commit after processing
            'max.poll.interval.ms': self.config.max_poll_interval_ms,
            'session.timeout.ms': 30000,
            'heartbeat.interval.ms': 10000,
            'fetch.max.bytes': 1048576,
            'max.partition.fetch.bytes': 524288,
        }

        # Add security if configured
        if self.config.security_protocol != "PLAINTEXT":
            consumer_config['security.protocol'] = self.config.security_protocol
            if self.config.sasl_mechanism:
                consumer_config['sasl.mechanism'] = self.config.sasl_mechanism
                consumer_config['sasl.username'] = self.config.sasl_username
                consumer_config['sasl.password'] = self.config.sasl_password

        try:
            self.consumer = Consumer(consumer_config)
            self.consumer.subscribe([self.config.input_topic])
            logger.info(f"Subscribed to topic: {self.config.input_topic}")
        except Exception as e:
            logger.error(f"Failed to create consumer: {e}")
            raise

    def setup_producer(self):
        """Setup Kafka producer for tokenised output"""

        producer_config = {
            'bootstrap.servers': self.config.bootstrap_servers,
            'acks': 'all',  # Wait for all replicas
            'retries': 3,
            'retry.backoff.ms': 500,
            'delivery.timeout.ms': 5000,
            'compression.type': 'snappy',
            'batch.size': 32768,
            'linger.ms': 20,
        }

        # Add security if configured
        if self.config.security_protocol != "PLAINTEXT":
            producer_config['security.protocol'] = self.config.security_protocol
            if self.config.sasl_mechanism:
                producer_config['sasl.mechanism'] = self.config.sasl_mechanism
                producer_config['sasl.username'] = self.config.sasl_username
                producer_config['sasl.password'] = self.config.sasl_password

        try:
            self.producer = Producer(producer_config)
            logger.info("Producer created successfully")
        except Exception as e:
            logger.error(f"Failed to create producer: {e}")
            raise

    def validate_pan(self, pan: str) -> Tuple[bool, Optional[str]]:
        """
        Validate PAN using Luhn algorithm and basic checks

        Returns:
            (is_valid, error_message)
        """
        if not pan:
            return False, "PAN is empty"

        # Remove spaces and dashes
        pan = pan.replace(" ", "").replace("-", "")

        if not pan.isdigit():
            return False, "PAN must contain only digits"

        if len(pan) < 13 or len(pan) > 19:
            return False, f"PAN length {len(pan)} invalid (must be 13-19 digits)"

        # Luhn check
        try:
            digits = [int(d) for d in pan]
            checksum = 0
            for i in range(len(digits) - 2, -1, -2):
                checksum += self.luhn_table[digits[i]]
            for i in range(len(digits) - 1, -1, -2):
                checksum += digits[i]

            if checksum % 10 != 0:
                return False, "PAN failed Luhn validation"
        except Exception as e:
            return False, f"Luhn validation error: {e}"

        return True, None

    def tokenise_pan(self, pan: str) -> Tuple[str, bool]:
        """
        Tokenise PAN using format-preserving tokenisation

        Returns:
            (token, from_cache)
        """
        if not pan:
            return None, False

        # Clean PAN
        pan = pan.replace(" ", "").replace("-", "")

        # Check cache first
        if pan in self.token_cache:
            self.stats['cache_hits'] += 1
            return self.token_cache[pan], True

        self.stats['cache_misses'] += 1
        start_time = time.perf_counter()

        # Format-preserving tokenisation
        # Keep BIN (first 6) and last 4, tokenise the middle
        bin_part = pan[:6]
        last4 = pan[-4:]
        middle_part = pan[6:-4]

        # Create deterministic token for middle part
        # In production, this would call HashiCorp Vault
        hmac_key = self.config.hmac_secret.encode('utf-8')
        middle_hash = hmac.new(
            hmac_key,
            f"{middle_part}{pan}".encode('utf-8'),
            hashlib.sha256
        ).hexdigest()[:len(middle_part)]

        token = f"{bin_part}{middle_hash.upper()}{last4}"

        # Cache the token
        if len(self.token_cache) > 100000:  # Limit cache size
            # Clear 20% of oldest entries
            items_to_remove = list(self.token_cache.keys())[:20000]
            for key in items_to_remove:
                del self.token_cache[key]

        self.token_cache[pan] = token

        # Update statistics
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        total_processed = self.stats['total_tokenised'] + 1
        self.stats['avg_tokenisation_ms'] = (
                (self.stats['avg_tokenisation_ms'] * (total_processed - 1) + elapsed_ms)
                / total_processed
        )

        return token, False

    def parse_visa_message(self, raw_message: str) -> Optional[Dict]:
        """
        Parse raw Visa ISO 8583 message to extract fields

        Args:
            raw_message: JSON string containing Visa transaction

        Returns:
            Parsed transaction dict or None if parsing fails
        """
        try:
            # Parse JSON message
            if isinstance(raw_message, str):
                tx = json.loads(raw_message)
            else:
                tx = raw_message

            # Extract required fields
            bitmap = tx.get('bitmap', {})

            parsed = {
                'transaction_id': tx.get('transaction_id', str(uuid.uuid4())),
                'source': tx.get('source', 'VISA_ISO8583'),
                'message_type': tx.get('message_type'),
                'timestamp': tx.get('timestamp', datetime.now().isoformat()),
                'event_time_ms': tx.get('event_time_ms', int(time.time() * 1000)),

                # Card data
                'pan': bitmap.get('DE002'),
                'bin': None,
                'last4': None,

                # Transaction data
                'amount': float(bitmap.get('DE004', 0)),
                'currency_code': bitmap.get('DE049', '404'),  # 404 = KES

                # Merchant data
                'merchant_id': bitmap.get('DE042'),
                'terminal_id': bitmap.get('DE041'),
                'merchant_name': bitmap.get('DE043'),
                'mcc': bitmap.get('DE018'),

                # Transaction type
                'transaction_type': bitmap.get('DE003'),
                'pos_entry_mode': bitmap.get('DE022'),

                # Additional fields
                'stan': bitmap.get('DE011'),  # System Trace Audit Number
                'acquirer_id': bitmap.get('DE032'),
                'forwarding_id': bitmap.get('DE033'),

                # Original raw message for debugging
                '_raw': tx
            }

            # Extract BIN and last4
            if parsed['pan']:
                parsed['bin'] = parsed['pan'][:6] if len(parsed['pan']) >= 6 else None
                parsed['last4'] = parsed['pan'][-4:] if len(parsed['pan']) >= 4 else None

            return parsed

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON: {e}")
            return None
        except Exception as e:
            logger.error(f"Failed to parse Visa message: {e}")
            return None

    def create_tokenised_message(self, parsed_tx: Dict, token: str, from_cache: bool) -> Dict:
        """
        Create tokenised message for output topic

        Args:
            parsed_tx: Parsed transaction data
            token: Tokenised PAN
            from_cache: Whether token came from cache

        Returns:
            Tokenised message dict
        """
        tokenised = {
            'transaction_id': parsed_tx['transaction_id'],
            'source': parsed_tx['source'],
            'message_type': parsed_tx['message_type'],
            'timestamp': parsed_tx['timestamp'],
            'event_time_ms': parsed_tx['event_time_ms'],
            'tokenisation_timestamp': datetime.now().isoformat(),

            # Tokenised card data (PCI-DSS compliant)
            'card_token': token,
            'bin': parsed_tx['bin'],
            'last4': parsed_tx['last4'],
            'token_from_cache': from_cache,

            # Transaction data (preserved as-is)
            'amount': parsed_tx['amount'],
            'currency_code': parsed_tx['currency_code'],
            'currency': self._currency_code_to_str(parsed_tx['currency_code']),

            # Merchant data
            'merchant_id': parsed_tx['merchant_id'],
            'merchant_name': parsed_tx['merchant_name'],
            'terminal_id': parsed_tx['terminal_id'],
            'mcc': parsed_tx['mcc'],

            # Transaction metadata
            'transaction_type': parsed_tx['transaction_type'],
            'pos_entry_mode': parsed_tx['pos_entry_mode'],
            'stan': parsed_tx['stan'],
            'acquirer_id': parsed_tx['acquirer_id'],

            # Tokenisation metadata
            'tokenisation_method': 'FORMAT_PRESERVING_HMAC',

            # Remove sensitive data
            '_redacted': True,
            '_raw_pan_removed': True
        }

        return tokenised

    def _currency_code_to_str(self, code: str) -> str:
        """Convert ISO currency code to string"""
        mapping = {
            '404': 'KES',
            '840': 'USD',
            '826': 'GBP',
            '978': 'EUR',
            '710': 'ZAR'
        }
        return mapping.get(code, 'KES')

    def delivery_callback(self, err, msg):
        """Callback for producer delivery reports"""
        if err:
            logger.error(f"Delivery failed: {err}")
            self.stats['total_errors'] += 1
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

    def process_message(self, message) -> bool:
        """
        Process a single Kafka message

        Returns:
            True if processed successfully, False otherwise
        """
        try:
            # Parse message value
            if message.value() is None:
                logger.warning("Empty message received")
                return False

            raw_value = message.value().decode('utf-8')

            # Parse Visa message
            parsed = self.parse_visa_message(raw_value)
            if not parsed:
                self.stats['total_errors'] += 1
                self._send_to_dlq(message, "Failed to parse Visa message")
                return False

            # Validate PAN
            pan = parsed.get('pan')
            is_valid, error_msg = self.validate_pan(pan)
            if not is_valid:
                logger.warning(f"Invalid PAN for transaction {parsed['transaction_id']}: {error_msg}")
                self.stats['total_errors'] += 1
                self._send_to_dlq(message, f"PAN validation failed: {error_msg}")
                return False

            # Tokenise PAN
            token, from_cache = self.tokenise_pan(pan)
            if not token:
                self.stats['total_errors'] += 1
                self._send_to_dlq(message, "Tokenisation failed")
                return False

            # Create tokenised message
            tokenised = self.create_tokenised_message(parsed, token, from_cache)

            # Produce to output topic
            key = token[:16]  # Use token prefix as partition key
            self.producer.produce(
                topic=self.config.output_topic,
                key=key.encode('utf-8') if key else None,
                value=json.dumps(tokenised).encode('utf-8'),
                callback=self.delivery_callback
            )

            # Flush to ensure delivery (in production, batch this)
            self.producer.poll(0)

            # Update statistics
            self.stats['total_consumed'] += 1
            self.stats['total_tokenised'] += 1

            logger.debug(f"Processed transaction {parsed['transaction_id']} - Token: {token[:20]}...")

            return True

        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
            self.stats['total_errors'] += 1
            self._send_to_dlq(message, f"Processing error: {str(e)}")
            return False

    def _send_to_dlq(self, message, error_reason: str):
        """Send failed message to Dead Letter Queue"""
        try:
            dlq_message = {
                'original_message': message.value().decode('utf-8') if message.value() else None,
                'error_reason': error_reason,
                'timestamp': datetime.now().isoformat(),
                'topic': message.topic(),
                'partition': message.partition(),
                'offset': message.offset()
            }

            self.producer.produce(
                topic=self.config.dlq_topic,
                value=json.dumps(dlq_message).encode('utf-8'),
                callback=self.delivery_callback
            )
            self.producer.poll(0)
            self.stats['total_dlq'] += 1
            logger.warning(f"Sent to DLQ: {error_reason}")
        except Exception as e:
            logger.error(f"Failed to send to DLQ: {e}")

    def commit_offsets(self):
        """Commit consumer offsets"""
        try:
            self.consumer.commit(asynchronous=False)
            logger.debug("Offsets committed")
        except KafkaException as e:
            logger.error(f"Failed to commit offsets: {e}")

    def print_stats(self):
        """Print processing statistics"""
        logger.info("=" * 60)
        logger.info("VISA TOKENISER STATISTICS")
        logger.info("=" * 60)
        logger.info(f"Total Consumed: {self.stats['total_consumed']}")
        logger.info(f"Total Tokenised: {self.stats['total_tokenised']}")
        logger.info(f"Total DLQ: {self.stats['total_dlq']}")
        logger.info(f"Total Errors: {self.stats['total_errors']}")
        logger.info(f"Cache Hits: {self.stats['cache_hits']}")
        logger.info(f"Cache Misses: {self.stats['cache_misses']}")

        total_requests = self.stats['cache_hits'] + self.stats['cache_misses']
        if total_requests > 0:
            logger.info(f"Cache Hit Rate: {self.stats['cache_hits'] / total_requests:.2%}")

        logger.info(f"Avg Tokenisation: {self.stats['avg_tokenisation_ms']:.2f} ms")
        logger.info("=" * 60)

    def run(self):
        """Main processing loop"""
        self.running = True
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        logger.info("Starting Visa Tokeniser...")
        logger.info(f"Consuming from: {self.config.input_topic}")
        logger.info(f"Producing to: {self.config.output_topic}")

        last_stats_time = time.time()
        stats_interval = 60  # Print stats every 60 seconds

        try:
            while self.running:
                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"End of partition reached: {msg.topic()} [{msg.partition()}]")
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        self.stats['total_errors'] += 1
                    continue

                # Process the message
                success = self.process_message(msg)

                # Commit offset if processing succeeded
                if success:
                    self.consumer.commit(message=msg, asynchronous=True)

                # Print stats periodically
                current_time = time.time()
                if current_time - last_stats_time >= stats_interval:
                    self.print_stats()
                    last_stats_time = current_time

        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        except Exception as e:
            logger.error(f"Fatal error in main loop: {e}", exc_info=True)
        finally:
            self.shutdown()

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False

    def shutdown(self):
        """Clean shutdown"""
        logger.info("Shutting down Visa Tokeniser...")

        self.print_stats()

        # Flush remaining messages
        if self.producer:
            self.producer.flush()

        # Close consumer
        if self.consumer:
            self.consumer.close()

        logger.info("Visa Tokeniser stopped")



# ============================================
# MAIN EXECUTION
# ============================================

import random


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='Visa Tokeniser Service')
    parser.add_argument('--mode', choices=['consume', 'produce_test'], default='consume',
                        help='Run mode: consume (tokeniser) or produce_test (generate test data)')
    parser.add_argument('--bootstrap-servers', default='localhost:9092',
                        help='Kafka bootstrap servers')
    parser.add_argument('--num-transactions', type=int, default=100,
                        help='Number of test transactions to produce')

    args = parser.parse_args()

    if args.mode == 'produce_test':
        # Generate test data
        generator = VisaRawDataGenerator(bootstrap_servers=args.bootstrap_servers)
        generator.send_test_transactions(num_transactions=args.num_transactions)

    else:
        # Run tokeniser
        config = TokeniserConfig(
            bootstrap_servers=args.bootstrap_servers,
            input_topic="visa_raw",
            output_topic="visa_tokenised",
            dlq_topic="visa_dlq"
        )

        tokeniser = VisaTokeniser(config)
        tokeniser.setup()
        tokeniser.run()


if __name__ == "__main__":
    main()