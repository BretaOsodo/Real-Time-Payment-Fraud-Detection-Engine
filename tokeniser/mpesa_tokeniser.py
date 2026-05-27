import json
import hashlib
import hmac
import logging
import time
import uuid
from datetime import datetime
from typing import Dict, Optional
from dataclasses import dataclass
import redis

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.error import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================
# CONFIGURATION
# ============================================

@dataclass
class KafkaConfig:
    bootstrap_servers: str = "localhost:9092"
    consumer_group: str = "mpesa_tokeniser_group"
    raw_topic: str = "mpesa_raw"
    tokenised_topic: str = "mpesa_tokenised"
    dlq_topic: str = "mpesa_dlq"

    # Performance settings
    batch_size: int = 100
    linger_ms: int = 100
    max_poll_interval_ms: int = 300000
    session_timeout_ms: int = 30000

    # Security
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str = None
    sasl_username: str = None
    sasl_password: str = None


@dataclass
class RedisConfig:
    host: str = "localhost"
    port: int = 6379
    password: str = None
    db: int = 0
    token_cache_ttl: int = 86400  # 24 hours


# ============================================
# M-PESA TOKENISER SERVICE
# ============================================

class MpesaTokeniser:
    """
    Tokeniser service for M-Pesa Daraja webhook data.
    Consumes from mpesa_raw, tokenises sensitive fields, produces to mpesa_tokenised.

    Sensitive fields tokenised:
    - MSISDN (phone number) → phone_token
    - FirstName, MiddleName, LastName → name_hash
    - BillRefNumber → ref_token
    """

    def __init__(self, kafka_config: KafkaConfig = None, redis_config: RedisConfig = None):
        self.kafka_config = kafka_config or KafkaConfig()
        self.redis_config = redis_config or RedisConfig()

        # HMAC secret for deterministic tokenisation
        self._hmac_secret = b'mpesa_tokenisation_secret_key_2024'

        # Initialize Redis connection (for caching)
        self._redis_client = self._init_redis()

        # Initialize Kafka consumer and producer
        self._consumer = self._init_consumer()
        self._producer = self._init_producer()

        # Statistics
        self.stats = {
            'processed': 0,
            'tokenised': 0,
            'errors': 0,
            'cache_hits': 0,
            'cache_misses': 0
        }

        logger.info("M-Pesa Tokeniser initialised")
        logger.info(f"Consuming from: {self.kafka_config.raw_topic}")
        logger.info(f"Producing to: {self.kafka_config.tokenised_topic}")

    def _init_redis(self) -> Optional[redis.Redis]:
        """Initialize Redis connection for token caching"""
        try:
            client = redis.Redis(
                host=self.redis_config.host,
                port=self.redis_config.port,
                password=self.redis_config.password,
                db=self.redis_config.db,
                decode_responses=True,
                socket_timeout=0.5,
                socket_connect_timeout=0.5
            )
            client.ping()
            logger.info(f"Redis connected at {self.redis_config.host}:{self.redis_config.port}")
            return client
        except Exception as e:
            logger.warning(f"Redis not available: {e}. Running without cache.")
            return None

    def _init_consumer(self) -> Consumer:
        """Initialize Kafka consumer"""
        conf = {
            'bootstrap.servers': self.kafka_config.bootstrap_servers,
            'group.id': self.kafka_config.consumer_group,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,  # Manual commit after processing
            'max.poll.interval.ms': self.kafka_config.max_poll_interval_ms,
            'session.timeout.ms': self.kafka_config.session_timeout_ms,
            'security.protocol': self.kafka_config.security_protocol,
        }

        if self.kafka_config.sasl_mechanism:
            conf['sasl.mechanism'] = self.kafka_config.sasl_mechanism
            conf['sasl.username'] = self.kafka_config.sasl_username
            conf['sasl.password'] = self.kafka_config.sasl_password

        consumer = Consumer(conf)
        consumer.subscribe([self.kafka_config.raw_topic])

        logger.info(f"Subscribed to topic: {self.kafka_config.raw_topic}")
        return consumer

    def _init_producer(self) -> Producer:
        """Initialize Kafka producer"""
        conf = {
            'bootstrap.servers': self.kafka_config.bootstrap_servers,
            'batch.num.messages': self.kafka_config.batch_size,
            'linger.ms': self.kafka_config.linger_ms,
            'compression.type': 'snappy',
            'security.protocol': self.kafka_config.security_protocol,
            'enable.idempotence':True,
            'acks':'all',
            'retries':10,
            'max.in.flight.requests.per.connection':5
        }

        if self.kafka_config.sasl_mechanism:
            conf['sasl.mechanism'] = self.kafka_config.sasl_mechanism
            conf['sasl.username'] = self.kafka_config.sasl_username
            conf['sasl.password'] = self.kafka_config.sasl_password

        return Producer(conf)

    def _tokenise_phone(self, msisdn: str) -> Optional[str]:
        """
        Tokenise M-Pesa phone number.
        Uses HMAC-SHA256 with deterministic output.
        Returns token format: PHONE_{hash_first_16_chars}
        """
        if not msisdn:
            return None

        # Clean phone number: remove +, spaces, ensure 254 prefix
        clean = msisdn.strip().replace("+", "").replace(" ", "")
        if clean.startswith("0"):
            clean = "254" + clean[1:]
        elif clean.startswith("7"):
            clean = "254" + clean

        # Check cache first
        cache_key = f"phone_token:{clean}"
        if self._redis_client:
            try:
                cached = self._redis_client.get(cache_key)
                if cached:
                    self.stats['cache_hits'] += 1
                    return cached
            except Exception as e:
                logger.debug(f"Redis read failed: {e}")

        self.stats['cache_misses'] += 1

        # Generate deterministic token
        token = hashlib.pbkdf2_hmac(
            'sha256',
            clean.encode(),
            self._hmac_secret,
            100000
        ).hex()[:16].upper()

        result = f"PHONE_{token}"

        # Cache the result
        if self._redis_client:
            try:
                self._redis_client.setex(cache_key, self.redis_config.token_cache_ttl, result)
            except Exception as e:
                logger.debug(f"Redis write failed: {e}")

        return result

    def _tokenise_name(self, name: str) -> Optional[str]:
        """
        Tokenise customer name.
        Returns: NAME_{hash_first_12_chars}
        """
        if not name:
            return None

        clean = name.strip().lower()
        cache_key = f"name_token:{clean}"

        if self._redis_client:
            try:
                cached = self._redis_client.get(cache_key)
                if cached:
                    return cached
            except Exception:
                pass

        token = hashlib.md5(f"{clean}{self._hmac_secret}".encode()).hexdigest()[:12].upper()
        result = f"NAME_{token}"

        if self._redis_client:
            try:
                self._redis_client.setex(cache_key, self.redis_config.token_cache_ttl, result)
            except Exception:
                pass

        return result

    def _tokenise_reference(self, reference: str) -> Optional[str]:
        """
        Tokenise bill reference number.
        Returns: REF_{hash_first_12_chars}
        """
        if not reference:
            return None

        token = hashlib.sha256(f"{reference}{self._hmac_secret}".encode()).hexdigest()[:12].upper()
        return f"REF_{token}"

    def _validate_raw_message(self, raw_msg: Dict) -> tuple:
        """
        Validate raw M-Pesa message.
        Returns (is_valid, error_message)
        """
        required_fields = ['TransID', 'TransAmount', 'MSISDN', 'TransactionType']

        for field in required_fields:
            if field not in raw_msg:
                return False, f"Missing required field: {field}"

        if float(raw_msg.get('TransAmount', 0)) <= 0:
            return False, "Invalid amount (must be > 0)"

        if len(raw_msg.get('MSISDN', '')) < 10:
            return False, "Invalid MSISDN format"

        return True, None

    def process_message(self, raw_msg: Dict, kafka_key: str = None,
                        kafka_partition: int = None, kafka_offset: int = None) -> Dict:
        """
        Process a single raw M-Pesa message:
        1. Validate
        2. Tokenise sensitive fields
        3. Build tokenised output
        """

        # Validate
        is_valid, error = self._validate_raw_message(raw_msg)
        if not is_valid:
            logger.warning(f"Invalid message: {error}")
            return {
                'valid': False,
                'error': error,
                'raw': raw_msg
            }

        start_time = time.perf_counter()

        # Extract fields
        msisdn = raw_msg.get('MSISDN')
        first_name = raw_msg.get('FirstName')
        last_name = raw_msg.get('LastName', '')
        middle_name = raw_msg.get('MiddleName', '')
        bill_ref = raw_msg.get('BillRefNumber', '')

        # Tokenise sensitive fields
        phone_token = self._tokenise_phone(msisdn)
        name_token = self._tokenise_name(f"{first_name} {middle_name} {last_name}".strip())
        ref_token = self._tokenise_reference(bill_ref) if bill_ref else None

        # Generate transaction ID if not present
        trans_id = raw_msg.get('TransID', f"MP_{uuid.uuid4().hex[:12].upper()}")

        # Build tokenised output
        tokenised_msg = {
            # Tokenised identifiers
            'transaction_id': trans_id,
            'phone_token': phone_token,
            'name_token': name_token,
            'ref_token': ref_token,

            # Non-sensitive fields (preserved)
            'TransactionType': raw_msg.get('TransactionType'),
            'TransAmount': float(raw_msg.get('TransAmount', 0)),
            'TransTime': raw_msg.get('TransTime'),
            'BusinessShortCode': raw_msg.get('BusinessShortCode'),
            'OrgAccountBalance': raw_msg.get('OrgAccountBalance'),
            'ThirdPartyTransID': raw_msg.get('ThirdPartyTransID'),
            'TransactionReceipt': raw_msg.get('TransactionReceipt'),

            # Metadata
            'timestamp': datetime.now().isoformat(),
            'event_time_ms': int(time.time() * 1000),
            'tokenisation_latency_ms': round((time.perf_counter() - start_time) * 1000, 2),
            'tokenisation_version': '1.0',

            # Kafka metadata
            'kafka_metadata': {
                'source_topic': self.kafka_config.raw_topic,
                'source_key': kafka_key,
                'source_partition': kafka_partition,
                'source_offset': kafka_offset,
                'processed_at': datetime.now().isoformat()
            }
        }

        self.stats['processed'] += 1
        self.stats['tokenised'] += 1

        return {
            'valid': True,
            'tokenised': tokenised_msg,
            'raw': raw_msg
        }

    def _delivery_callback(self, err, msg):
        """Kafka producer delivery callback"""
        if err:
            logger.error(f"Message delivery failed: {err}")
            self.stats['errors'] += 1
        else:
            logger.debug(f"Message delivered to {msg.topic()}[{msg.partition()}] @ {msg.offset()}")

    def run(self, poll_timeout: float = 1.0, max_messages: int = None):
        """
        Main processing loop.

        Args:
            poll_timeout: Kafka poll timeout in seconds
            max_messages: Maximum messages to process (for testing)
        """
        logger.info("Starting M-Pesa Tokeniser...")

        messages_processed = 0

        try:
            while True:
                # Poll for messages
                msg = self._consumer.poll(timeout=poll_timeout)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"End of partition reached: {msg.topic()}[{msg.partition()}]")
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                    continue

                # Parse raw message
                try:
                    raw_msg = json.loads(msg.value().decode('utf-8'))
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON: {e}")
                    self._send_to_dlq(msg.value(), f"JSON parse error: {e}")
                    self._consumer.commit(asynchronous=False)
                    continue

                # Process message (tokenise)
                result = self.process_message(
                    raw_msg,
                    kafka_key=msg.key().decode('utf-8') if msg.key() else None,
                    kafka_partition=msg.partition(),
                    kafka_offset=msg.offset()
                )

                if result['valid']:
                    # Send to tokenised topic
                    tokenised_msg = result['tokenised']
                    key = tokenised_msg.get('phone_token', tokenised_msg.get('transaction_id'))

                    self._producer.produce(
                        topic=self.kafka_config.tokenised_topic,
                        key=key.encode('utf-8'),
                        value=json.dumps(tokenised_msg).encode('utf-8'),
                        callback=self._delivery_callback
                    )

                    # Log progress
                    messages_processed += 1
                    if messages_processed % 100 == 0:
                        logger.info(f"Processed {messages_processed} messages. Stats: {self.stats}")
                else:
                    # Send invalid messages to DLQ
                    self._send_to_dlq(msg.value(), result.get('error', 'Validation failed'))

                # Commit offset after processing
                self._consumer.commit(asynchronous=False)
                self._producer.flush()

                # Check if we've reached max messages
                if max_messages and messages_processed >= max_messages:
                    logger.info(f"Reached max messages: {max_messages}")
                    break

        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            self._consumer.close()
            self._producer.flush()
            logger.info(f"Final stats: {self.stats}")

    def _send_to_dlq(self, raw_value: bytes, error: str):
        """Send invalid message to Dead Letter Queue"""
        dlq_msg = {
            'original_message': raw_value.decode('utf-8'),
            'error': error,
            'timestamp': datetime.now().isoformat(),
            'consumer_group': self.kafka_config.consumer_group
        }

        self._producer.produce(
            topic=self.kafka_config.dlq_topic,
            key=f"error_{int(time.time())}".encode('utf-8'),
            value=json.dumps(dlq_msg).encode('utf-8'),
            callback=self._delivery_callback
        )
        self.stats['errors'] += 1


# ============================================
# KAFKA TOPIC MANAGEMENT
# ============================================

def create_topics(kafka_config: KafkaConfig):
    """Create required Kafka topics if they don't exist"""

    admin = AdminClient({
        'bootstrap.servers': kafka_config.bootstrap_servers
    })

    topics = [
        NewTopic(kafka_config.raw_topic, num_partitions=12, replication_factor=1),
        NewTopic(kafka_config.tokenised_topic, num_partitions=12, replication_factor=1),
        NewTopic(kafka_config.dlq_topic, num_partitions=3, replication_factor=1)
    ]

    try:
        # Check existing topics
        existing = admin.list_topics().topics

        for topic in topics:
            if topic.topic not in existing:
                admin.create_topics([topic])
                logger.info(f"Created topic: {topic.topic}")
            else:
                logger.info(f"Topic already exists: {topic.topic}")
    except Exception as e:
        logger.error(f"Topic creation failed: {e}")



# ============================================
# MAIN EXECUTION
# ============================================

import random


def main():
    """Main entry point"""
    print("M-PESA TOKENISER SERVICE")
    print("Consumes from: mpesa_raw → Tokenises → Produces to: mpesa_tokenised")


    kafka_config = KafkaConfig()
    redis_config = RedisConfig()

    # Create topics if they don't exist
    print("\nStep 1: Ensuring Kafka topics exist...")
    create_topics(kafka_config)

    # Start tokeniser
    print("\nStep 3: Starting M-Pesa Tokeniser...")
    tokeniser = MpesaTokeniser(kafka_config, redis_config)

    # Run tokeniser (process 100 messages for demo)
    tokeniser.run(max_messages=100, poll_timeout=1.0)

    # Print final statistics
    print("\n" + "=" * 80)
    print("FINAL STATISTICS")
    print("=" * 80)
    print(f"\nProcessing Stats:")
    print(f"   Processed: {tokeniser.stats['processed']}")
    print(f"   Tokenised: {tokeniser.stats['tokenised']}")
    print(f"   Errors: {tokeniser.stats['errors']}")
    print(f"   Cache Hits: {tokeniser.stats['cache_hits']}")
    print(f"   Cache Misses: {tokeniser.stats['cache_misses']}")
    print(
        f"   Cache Hit Rate: {tokeniser.stats['cache_hits'] / (tokeniser.stats['cache_hits'] + tokeniser.stats['cache_misses']) * 100:.1f}%")


if __name__ == "__main__":
    main()