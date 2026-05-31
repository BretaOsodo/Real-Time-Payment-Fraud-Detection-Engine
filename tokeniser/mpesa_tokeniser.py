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
class MpesaTokeniserConfig:
    bootstrap_servers: str = "localhost:9092"
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str = "PLAIN"
    sasl_username: str = ""
    sasl_password: str = ""

    input_topic: str = "mpesa_raw"
    output_topic: str = "mpesa_tokenised"
    dlq_topic: str = "mpesa_dlq"

    consumer_group: str = "mpesa_tokeniser_group"

    hmac_secret: str = "CHANGE_ME_IN_PRODUCTION"
    max_cache_size: int = 100_000
    max_poll_interval_ms: int = 300000


class MpesaTokeniser:
    """
    M-Pesa Tokeniser Service
    Consumes raw M-Pesa Daraja webhook messages from 'mpesa_raw' topic
    Tokenises MSISDN (phone numbers) and sensitive fields
    Produces tokenised transactions to 'mpesa_tokenised' topic
    """

    def __init__(self, config: MpesaTokeniserConfig = None):
        self.config = config or MpesaTokeniserConfig()
        self.consumer = None
        self.producer = None
        self.running = False

        # Token caches
        self.msisdn_cache: Dict[str, str] = {}  # MSISDN → token
        self.account_cache: Dict[str, str] = {}  # Business account → token

        self.stats = {
            'total_consumed': 0,
            'total_tokenised': 0,
            'total_dlq': 0,
            'total_errors': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'msisdn_tokenised': 0,
            'account_tokenised': 0,
            'tokenisation_total_ms': 0.0,
        }

    # Setup

    def setup(self):
        self._setup_consumer()
        self._setup_producer()
        logger.info("M-Pesa Tokeniser ready")

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

    # MSISDN (Phone Number) Validation

    def validate_msisdn(self, msisdn: str) -> Tuple[bool, Optional[str]]:
        """Validate M-Pesa MSISDN format"""
        if not msisdn:
            return False, "MSISDN is empty"

        # Clean MSISDN
        msisdn = msisdn.strip().replace(" ", "").replace("+", "")

        # Check length (Kenyan numbers: 9-13 digits including country code)
        if not (9 <= len(msisdn) <= 13):
            return False, f"MSISDN length {len(msisdn)} invalid (must be 9-13)"

        # Check if contains only digits
        if not msisdn.isdigit():
            return False, "MSISDN must contain only digits"

        # Check Kenyan format (254XXXXXXXXX or 07XXXXXXXX)
        if msisdn.startswith('254'):
            if len(msisdn) != 12:
                return False, "Kenyan MSISDN with country code must be 12 digits"
        elif msisdn.startswith('0'):
            if len(msisdn) != 10:
                return False, "Kenyan MSISDN without country code must be 10 digits"

        return True, None

    def normalise_msisdn(self, msisdn: str) -> str:
        """Normalise MSISDN to E.164 format (254XXXXXXXXX)"""
        if not msisdn:
            return None

        msisdn = msisdn.strip().replace(" ", "").replace("+", "")

        # Convert 07XXXXXXXX to 2547XXXXXXXX
        if msisdn.startswith('0') and len(msisdn) == 10:
            msisdn = '254' + msisdn[1:]

        # Convert 7XXXXXXXX to 2547XXXXXXXX
        if msisdn.startswith('7') and len(msisdn) == 9:
            msisdn = '254' + msisdn

        return msisdn

    # Tokenisation

    def _cache_key(self, value: str, prefix: str = "mpesa") -> str:
        """Derive non-reversible cache key"""
        return hmac.new(
            self.config.hmac_secret.encode(),
            f"{prefix}:{value}".encode(),
            hashlib.sha256
        ).hexdigest()

    def tokenise_msisdn(self, msisdn: str) -> Tuple[Optional[str], bool]:
        """
        Tokenise M-Pesa MSISDN (phone number)
        Format: MPESA_MSISDN_{hash}
        Returns (token, from_cache)
        """
        if not msisdn:
            return None, False

        msisdn = self.normalise_msisdn(msisdn)
        if not msisdn:
            return None, False

        cache_key = self._cache_key(msisdn, "msisdn")

        if cache_key in self.msisdn_cache:
            self.stats['cache_hits'] += 1
            return self.msisdn_cache[cache_key], True

        self.stats['cache_misses'] += 1
        start = time.perf_counter()

        # Generate deterministic token
        token_hash = hmac.new(
            self.config.hmac_secret.encode(),
            msisdn.encode(),
            hashlib.sha256
        ).hexdigest()[:16].upper()

        token = f"MPESA_MSISDN_{token_hash}"

        # Evict oldest 20% if cache is full
        if len(self.msisdn_cache) >= self.config.max_cache_size:
            evict_count = self.config.max_cache_size // 5
            for key in list(self.msisdn_cache.keys())[:evict_count]:
                del self.msisdn_cache[key]

        self.msisdn_cache[cache_key] = token
        self.stats['msisdn_tokenised'] += 1
        self.stats['tokenisation_total_ms'] += (time.perf_counter() - start) * 1000

        return token, False

    def tokenise_account(self, account: Union[str, int]) -> Tuple[Optional[str], bool]:
        """
        Tokenise business account number/shortcode
        Format: MPESA_ACC_{hash}
        """
        if not account:
            return None, False

        account_str = str(account)
        cache_key = self._cache_key(account_str, "account")

        if cache_key in self.account_cache:
            return self.account_cache[cache_key], True

        start = time.perf_counter()

        token_hash = hmac.new(
            self.config.hmac_secret.encode(),
            account_str.encode(),
            hashlib.sha256
        ).hexdigest()[:12].upper()

        token = f"MPESA_ACC_{token_hash}"

        if len(self.account_cache) >= self.config.max_cache_size:
            evict_count = self.config.max_cache_size // 5
            for key in list(self.account_cache.keys())[:evict_count]:
                del self.account_cache[key]

        self.account_cache[cache_key] = token
        self.stats['account_tokenised'] += 1
        self.stats['tokenisation_total_ms'] += (time.perf_counter() - start) * 1000

        return token, False

    # Message Parsing - FIXED for dict input

    def parse_mpesa_message(self, raw_message: Union[str, Dict]) -> Optional[Dict]:
        """
        Parse M-Pesa Daraja webhook message.
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

            # Check if message has webhook_payload wrapper (from test data)
            if 'webhook_payload' in tx:
                tx = tx['webhook_payload']

            # Extract source if present
            source = tx.get('source', tx.get('source_type', 'MPESA_DARAJA'))

            # Format 1: Standard M-Pesa API callback with TransactionType
            if 'TransactionType' in tx:
                return {
                    'transaction_id': tx.get('TransID', str(uuid.uuid4())),
                    'source': source,
                    'source_type': 'MPESA_DARAJA',
                    'transaction_type': tx.get('TransactionType'),
                    'timestamp': self._parse_mpesa_timestamp(tx.get('TransTime')),
                    'event_time_ms': int(time.time() * 1000),
                    'msisdn': tx.get('MSISDN'),
                    'amount': float(tx.get('TransAmount', 0)),
                    'business_shortcode': tx.get('BusinessShortCode'),
                    'bill_ref_number': tx.get('BillRefNumber'),
                    'invoice_number': tx.get('InvoiceNumber'),
                    'org_account_balance': tx.get('OrgAccountBalance'),
                    'third_party_trans_id': tx.get('ThirdPartyTransID'),
                    'first_name': tx.get('FirstName'),
                    'middle_name': tx.get('MiddleName'),
                    'last_name': tx.get('LastName'),
                    'transaction_receipt': tx.get('TransactionReceipt'),
                }

            # Format 2: Daraja STKPush callback
            elif 'Body' in tx and 'stkCallback' in tx.get('Body', {}):
                stk = tx['Body']['stkCallback']
                return {
                    'transaction_id': stk.get('CheckoutRequestID', str(uuid.uuid4())),
                    'source': source,
                    'source_type': 'MPESA_DARAJA',
                    'transaction_type': 'STKPush',
                    'timestamp': datetime.now().isoformat(),
                    'event_time_ms': int(time.time() * 1000),
                    'msisdn': stk.get('PhoneNumber'),
                    'amount': float(stk.get('Amount', 0)),
                    'merchant_request_id': stk.get('MerchantRequestID'),
                    'checkout_request_id': stk.get('CheckoutRequestID'),
                    'result_code': stk.get('ResultCode'),
                    'result_desc': stk.get('ResultDesc'),
                    'mpesa_receipt_number': stk.get('MpesaReceiptNumber'),
                    'transaction_date': stk.get('TransactionDate'),
                }

            # Format 3: C2B webhook - TransactionType at top level
            elif 'TransactionType' in tx:
                return {
                    'transaction_id': tx.get('TransID', str(uuid.uuid4())),
                    'source': source,
                    'source_type': 'MPESA_DARAJA',
                    'transaction_type': tx.get('TransactionType'),
                    'timestamp': datetime.now().isoformat(),
                    'event_time_ms': int(time.time() * 1000),
                    'msisdn': tx.get('MSISDN'),
                    'amount': float(tx.get('TransAmount', 0)),
                    'business_shortcode': tx.get('BusinessShortCode'),
                    'bill_ref_number': tx.get('BillRefNumber'),
                }

            else:
                logger.warning(f"Unknown M-Pesa message format: {str(tx)[:200]}")
                return None

        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error: {e}")
            return None
        except Exception as e:
            logger.error(f"Parse error: {e}")
            return None

    def _parse_mpesa_timestamp(self, trans_time: str) -> str:
        """Parse M-Pesa timestamp (YYYYMMDDHHMMSS) to ISO format"""
        if not trans_time or len(trans_time) != 14:
            return datetime.now().isoformat()

        try:
            dt = datetime.strptime(trans_time, '%Y%m%d%H%M%S')
            return dt.isoformat()
        except ValueError:
            return datetime.now().isoformat()

    def _build_tokenised_message(self, parsed: Dict, msisdn_token: str,
                                 account_token: str, from_cache: bool) -> Dict:
        """Build tokenised output message"""

        # Determine if transaction is incoming or outgoing
        transaction_role = "CUSTOMER"
        if parsed.get('business_shortcode'):
            transaction_role = "MERCHANT"

        return {
            'transaction_id': parsed['transaction_id'],
            'source': parsed.get('source', 'MPESA'),
            'source_type': parsed.get('source_type', 'DARAJA_WEBHOOK'),
            'transaction_type': parsed.get('transaction_type'),
            'timestamp': parsed['timestamp'],
            'event_time_ms': parsed['event_time_ms'],
            'tokenisation_timestamp': datetime.now().isoformat(),

            # Tokenised fields (MSISDN replaced with token)
            'msisdn_token': msisdn_token,
            'account_token': account_token,
            'token_from_cache': from_cache,
            'tokenisation_method': 'HMAC_SHA256_PSEUDONYMISATION',

            # Transaction data (non-sensitive)
            'amount': parsed.get('amount'),
            'currency': 'KES',  # M-Pesa always KES
            'bill_ref_number': parsed.get('bill_ref_number'),
            'invoice_number': parsed.get('invoice_number'),
            'transaction_receipt': parsed.get('transaction_receipt'),
            'mpesa_receipt_number': parsed.get('mpesa_receipt_number'),

            # Customer info (non-sensitive)
            'first_name': parsed.get('first_name'),
            'last_name': parsed.get('last_name'),

            # Transaction metadata
            'result_code': parsed.get('result_code'),
            'result_desc': parsed.get('result_desc'),
            'transaction_role': transaction_role,

            # Raw references
            'third_party_trans_id': parsed.get('third_party_trans_id'),
            'merchant_request_id': parsed.get('merchant_request_id'),
            'checkout_request_id': parsed.get('checkout_request_id'),
        }

    def process_message(self, message) -> bool:
        """Process a single M-Pesa message"""
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
            parsed = self.parse_mpesa_message(tx_data)

            if not parsed:
                self._send_to_dlq(message, "Parse failure - unknown format")
                return False

            # Validate and tokenise MSISDN
            msisdn = parsed.get('msisdn')
            msisdn_token = None
            from_cache = False

            if msisdn:
                valid, error = self.validate_msisdn(msisdn)
                if not valid:
                    self._send_to_dlq(message, f"MSISDN validation failed: {error}")
                    return False
                msisdn_token, from_cache = self.tokenise_msisdn(msisdn)

            # Tokenise business account if present
            account = parsed.get('business_shortcode')
            account_token, _ = self.tokenise_account(account) if account else (None, False)

            output = self._build_tokenised_message(parsed, msisdn_token, account_token, from_cache)

            # Use token as partition key for consistent routing
            key = msisdn_token.encode('utf-8') if msisdn_token else parsed['transaction_id'].encode('utf-8')

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
                'service': 'mpesa-tokeniser'
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

        logger.info("── M-Pesa Tokeniser Stats ──────────────────────")
        logger.info(f"  Consumed:        {self.stats['total_consumed']}")
        logger.info(f"  Tokenised:       {self.stats['total_tokenised']}")
        logger.info(f"  DLQ:             {self.stats['total_dlq']}")
        logger.info(f"  Errors:          {self.stats['total_errors']}")
        logger.info(f"  MSISDN tokens:   {self.stats['msisdn_tokenised']}")
        logger.info(f"  Account tokens:  {self.stats['account_tokenised']}")
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
        logger.info("Shutting down M-Pesa Tokeniser...")
        self.print_stats()
        if self.producer:
            self.producer.flush(timeout=10)
        if self.consumer:
            self.consumer.close()
        logger.info("M-Pesa Tokeniser stopped")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='M-Pesa Tokeniser')
    parser.add_argument('--bootstrap-servers', default='localhost:9092')
    parser.add_argument('--hmac-secret', default='CHANGE_ME_IN_PRODUCTION')
    parser.add_argument('--input-topic', default='mpesa_raw')
    parser.add_argument('--output-topic', default='mpesa_tokenised')
    parser.add_argument('--dlq-topic', default='mpesa_dlq')
    args = parser.parse_args()

    config = MpesaTokeniserConfig(
        bootstrap_servers=args.bootstrap_servers,
        hmac_secret=args.hmac_secret,
        input_topic=args.input_topic,
        output_topic=args.output_topic,
        dlq_topic=args.dlq_topic,
    )

    tokeniser = MpesaTokeniser(config)
    tokeniser.setup()
    tokeniser.run()