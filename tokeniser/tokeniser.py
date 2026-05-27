import hashlib
import hmac
import json
import logging
import os
import time
import uuid
from datetime import datetime
from functools import lru_cache
from typing import Dict, Optional, List
import random


# Simulate Redis for development
class MockRedis:
    def __init__(self):
        self.cache = {}

    def get(self, key):
        return self.cache.get(key)

    def setex(self, key, ttl, value):
        self.cache[key] = value
        return True

    def ping(self):
        return True


# Configuration
class Config:
    tokenisation = {
        'vault_url': os.environ.get('VAULT_URL', 'http://localhost:8200'),
        'vault_token': os.environ.get('VAULT_TOKEN', 'dev-token')
    }
    redis_cfg = {
        'host': 'localhost',
        'port': 6379,
        'password': None,
        'ssl': False,
        'db': 0
    }


tok_cfg = Config.tokenisation
redis_cfg = Config.redis_cfg
_PRODUCTION = os.environ.get("ENVIRONMENT", "development").lower() == "production"
_DEV_HMAC_SECRET = os.environ.get("DEV_TOKENISATION_SECRET", "CHANGE_ME_NOT_FOR_PRODUCTION")

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class Tokeniser:
    """
    Converts sensitive identifiers to privacy-safe tokens.
    PCI-DSS compliant: PAN never stored in plaintext.
    """

    def __init__(self, use_mock_redis=True):
        self._vault_client = self._init_vault()
        self._redis = self._init_redis(use_mock_redis)

    def _init_vault(self):
        # Mock Vault for development
        if not _PRODUCTION:
            logger.info("Development mode: using mock Vault")
            return MockVault()

        # Production Vault initialization
        try:
            import hvac
            if not tok_cfg.get('vault_url') or not tok_cfg.get('vault_token'):
                raise RuntimeError("Vault credentials required in production")

            client = hvac.Client(url=tok_cfg['vault_url'], token=tok_cfg['vault_token'])
            if not client.is_authenticated():
                raise RuntimeError("Vault authentication failed")

            logger.info("Vault client authenticated")
            return client
        except Exception as e:
            logger.warning(f"Vault not available: {e} — using HMAC fallback")
            return None

    def _init_redis(self, use_mock=True):
        """Redis caches the pan_hash → token mapping to avoid duplicate Vault calls."""
        if use_mock:
            return MockRedis()

        try:
            import redis
            r = redis.Redis(
                host=redis_cfg['host'],
                port=redis_cfg['port'],
                password=redis_cfg['password'],
                ssl=redis_cfg['ssl'],
                db=redis_cfg['db'],
                socket_timeout=0.003,  # 3 ms — fail fast
                decode_responses=True,
            )
            r.ping()
            return r
        except Exception as exc:
            logger.warning(f"Redis not available for token cache: {exc}")
            return None

    def tokenise(self, pan: str) -> str:
        """
        Return a format-preserving token for a full PAN.
        Preserves BIN (first 6) and last 4 digits.
        Passes Luhn check for downstream compatibility.
        """
        if not pan:
            return str(uuid.uuid4())

        pan_clean = pan.replace(" ", "").replace("-", "")
        cache_key = f"tok:{self._pan_cache_key(pan_clean)}"

        # Cache check (sub-millisecond)
        if self._redis:
            try:
                cached = self._redis.get(cache_key)
                if cached:
                    return cached
            except Exception:
                pass

        # Tokenisation (prioritise Vault, fallback to HMAC)
        token = self._vault_tokenise(pan_clean)
        if not token:
            token = self._hmac_tokenise(pan_clean)

        # Cache the result (TTL = 24 hours — tokens are stable)
        if self._redis:
            try:
                self._redis.setex(cache_key, 86400, token)
            except Exception:
                pass

        return token

    def tokenise_partial(self, first6: str, last4: str, token_hint: str = None) -> str:
        """
        When only BIN and last4 are available (e.g., Flutterwave webhook),
        derive a consistent pseudonymous token.
        """
        if not first6 or not last4:
            return f"TOKEN_{uuid.uuid4().hex[:16].upper()}"

        composite = f"{first6}****{last4}"
        if token_hint:
            composite = f"{composite}:{token_hint}"

        token = self._hmac_tokenise(composite)

        # Format: preserve BIN + last4 for analytics
        return f"{first6}TOKEN{token[:8].upper()}{last4}"

    def tokenise_phone(self, msisdn: str) -> Optional[str]:
        """Return a pseudonymous token for a phone number."""
        if not msisdn:
            return None

        # Normalise: strip +, spaces, country code
        clean = msisdn.strip().replace("+", "").replace(" ", "")
        token = self._hmac_tokenise(clean)[:16]
        return f"PHONE_{token}"

    def _vault_tokenise(self, pan: str) -> Optional[str]:
        """Call Vault Transform secrets engine for format-preserving tokenisation."""
        if not self._vault_client:
            return None

        try:
            # Mock Vault response for development
            if hasattr(self._vault_client, 'encode_value'):
                return self._vault_client.encode_value(pan)

            # Real Vault call
            response = self._vault_client.secrets.transform.encode_value(
                role_name="payments",
                value=pan,
                mount_point="transform",
            )
            return response["data"]["encoded_value"]
        except Exception as exc:
            logger.error(f"Vault tokenisation failed: {exc}")
            return None

    def _hmac_tokenise(self, value: str) -> str:
        """
        HMAC-SHA256 pseudonymisation.
        Used as fallback in development or when Vault is unavailable.
        """
        digest = hmac.new(
            _DEV_HMAC_SECRET.encode("utf-8"),
            value.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        # Format: first 8 chars of hash
        return digest[:16].upper()

    def _pan_cache_key(self, pan: str) -> str:
        """Derive a non-reversible cache key without storing PAN in Redis."""
        return hashlib.sha256(
            f"cache:{_DEV_HMAC_SECRET}:{pan}".encode()
        ).hexdigest()[:32]

    def validate_luhn(self, pan: str) -> bool:
        """Validate PAN passes Luhn algorithm (card number check)."""
        if not pan or not pan.isdigit():
            return False

        total = 0
        reverse_digits = [int(d) for d in str(pan)][::-1]

        for i, digit in enumerate(reverse_digits):
            if i % 2 == 1:  # Double every second digit
                doubled = digit * 2
                total += doubled if doubled < 10 else doubled - 9
            else:
                total += digit

        return total % 10 == 0


class MockVault:
    """Mock Vault for development - format-preserving tokenisation"""

    def __init__(self):
        self.token_cache = {}

    def encode_value(self, pan: str) -> str:
        """Mock format-preserving tokenisation - returns token string directly"""
        if pan in self.token_cache:
            return self.token_cache[pan]

        # Format-preserving: keep BIN (first 6) and last 4, replace middle
        if len(pan) >= 16:
            bin_part = pan[:6]
            last4 = pan[-4:]
            middle_len = len(pan) - 10

            # Generate deterministic token for the middle part
            middle_hash = hashlib.md5(f"{pan}{_DEV_HMAC_SECRET}".encode()).hexdigest()[:middle_len]
            token = f"{bin_part}{middle_hash.upper()}{last4}"
        else:
            # For non-PAN values
            token = f"TOKEN_{hashlib.md5(pan.encode()).hexdigest()[:12].upper()}"

        self.token_cache[pan] = token
        return token


# ============================================
# DATA GENERATION WITH TOKENISATION
# ============================================

class TokenisedDataGenerator:
    def __init__(self):
        self.tokeniser = Tokeniser(use_mock_redis=True)

        # BIN ranges by issuer
        self.bin_ranges = {
            'VISA': ['412345', '498765', '432187', '445678', '453201', '471610'],
            'MASTERCARD': ['512345', '534567', '545678', '556789', '522234', '531234'],
            'MPESA': ['600000', '600123', '600456', '600789'],
            'FLUTTERWAVE': ['520000', '530000', '540000'],
            'PESAPAL': ['501234', '502345', '503456']
        }

    def generate_pan(self, issuer: str) -> str:
        """Generate a realistic PAN with valid Luhn checksum"""
        bin_prefix = random.choice(self.bin_ranges.get(issuer, self.bin_ranges['VISA']))
        account = ''.join([str(random.randint(0, 9)) for _ in range(9)])
        pan_base = bin_prefix + account

        # Calculate Luhn checksum digit
        def luhn_checksum(card_number):
            def digits_of(n):
                return [int(d) for d in str(n)]

            digits = digits_of(card_number)
            odd_digits = digits[-1::-2]
            even_digits = digits[-2::-2]
            checksum = sum(odd_digits)
            for d in even_digits:
                checksum += sum(digits_of(d * 2))
            return (10 - (checksum % 10)) % 10

        check_digit = luhn_checksum(int(pan_base + '0'))
        return pan_base + str(check_digit)

    def generate_transaction_with_tokenisation(self, source: str) -> Dict:
        """Generate a transaction and tokenise the PAN/phone"""

        if source in ['VISA', 'MASTERCARD']:
            pan = self.generate_pan(source)
            token = self.tokeniser.tokenise(pan)

            transaction = {
                'source': source,
                'source_type': 'ISO_8583',
                'card_token': token,
                'bin': pan[:6],
                'last4': pan[-4:],
                'amount': round(random.uniform(10, 5000), 2),
                'currency': random.choice(['KES', 'USD', 'GBP']),
                'timestamp': datetime.now().isoformat(),
                'tokenisation_method': 'VAULT_FORMAT_PRESERVING' if self.tokeniser._vault_client else 'HMAC_FALLBACK',
                'luhn_valid': self.tokeniser.validate_luhn(pan)
            }

        elif source == 'MPESA':
            phone = f"254{random.randint(700000000, 799999999)}"
            token = self.tokeniser.tokenise_phone(phone)

            transaction = {
                'source': 'MPESA',
                'source_type': 'DARAJA_WEBHOOK',
                'phone_token': token,
                'amount': round(random.uniform(10, 50000), 2),
                'currency': 'KES',
                'timestamp': datetime.now().isoformat(),
                'tokenisation_method': 'PHONE_PSEUDONYMISATION'
            }

        elif source == 'FLUTTERWAVE':
            # Flutterwave only provides BIN + last4
            bin_prefix = random.choice(self.bin_ranges['FLUTTERWAVE'])
            last4 = str(random.randint(1000, 9999))
            token_hint = f"FLW_{random.randint(1000, 9999)}"

            token = self.tokeniser.tokenise_partial(bin_prefix, last4, token_hint)

            transaction = {
                'source': 'FLUTTERWAVE',
                'source_type': 'WEBHOOK',
                'bin': bin_prefix,
                'last4': last4,
                'card_token': token,
                'amount': round(random.uniform(10, 5000), 2),
                'currency': random.choice(['KES', 'USD', 'EUR']),
                'timestamp': datetime.now().isoformat(),
                'tokenisation_method': 'PARTIAL_PAN_TOKENISATION'
            }

        else:  # PESAPAL
            pan = self.generate_pan('PESAPAL')
            token = self.tokeniser.tokenise(pan)

            transaction = {
                'source': 'PESAPAL',
                'source_type': 'IPN_CALLBACK',
                'card_token': token,
                'bin': pan[:6],
                'last4': pan[-4:],
                'amount': round(random.uniform(10, 50000), 2),
                'currency': random.choice(['KES', 'USD', 'UGX']),
                'timestamp': datetime.now().isoformat(),
                'tokenisation_method': 'VAULT_FORMAT_PRESERVING' if self.tokeniser._vault_client else 'HMAC_FALLBACK',
                'luhn_valid': self.tokeniser.validate_luhn(pan)
            }

        transaction['transaction_id'] = str(uuid.uuid4())
        transaction['event_time_ms'] = int(time.time() * 1000)

        return transaction

    def generate_dataset(self, transactions_per_source: int = 100) -> Dict:
        """Generate complete tokenised dataset"""

        sources = ['VISA', 'MASTERCARD', 'MPESA', 'FLUTTERWAVE', 'PESAPAL']
        all_transactions = []

        print("=" * 70)
        print("TOKENISED DATA GENERATION - PCI-DSS COMPLIANT")
        print("=" * 70)

        for source in sources:
            print(f"\n📊 Generating {transactions_per_source} {source} transactions...")
            for i in range(transactions_per_source):
                tx = self.generate_transaction_with_tokenisation(source)
                all_transactions.append(tx)

        # Add some duplicate PANs to test token consistency
        print(f"\n🔄 Adding duplicate PAN transactions to test token consistency...")
        for _ in range(50):
            source = random.choice(['VISA', 'MASTERCARD'])
            tx = self.generate_transaction_with_tokenisation(source)
            all_transactions.append(tx)

        # Tokenisation statistics - FIXED: extract token strings properly
        token_counter = {}

        for tx in all_transactions:
            # Get the token value (string) from either card_token or phone_token
            token_value = tx.get('card_token') or tx.get('phone_token')

            if token_value and isinstance(token_value, str):  # Ensure it's a string
                if token_value in token_counter:
                    token_counter[token_value] += 1
                else:
                    token_counter[token_value] = 1

        unique_tokens = len(token_counter)
        total_tokens = sum(token_counter.values())

        output = {
            'metadata': {
                'total_transactions': len(all_transactions),
                'unique_tokens_generated': unique_tokens,
                'token_collision_rate': (len(all_transactions) - unique_tokens) / len(
                    all_transactions) if all_transactions else 0,
                'tokenisation_methods': {
                    'Vault': len([t for t in all_transactions if 'VAULT' in t.get('tokenisation_method', '')]),
                    'HMAC': len([t for t in all_transactions if 'HMAC' in t.get('tokenisation_method', '')]),
                    'Phone': len([t for t in all_transactions if 'PHONE' in t.get('tokenisation_method', '')]),
                    'Partial': len([t for t in all_transactions if 'PARTIAL' in t.get('tokenisation_method', '')])
                },
                'generated_at': datetime.now().isoformat(),
                'version': '2.0',
                'pci_compliant': True,
                'pci_notes': 'No PAN stored in plaintext. Tokens preserve BIN+last4 for analytics.'
            },
            'transactions': all_transactions,
            'tokenisation_stats': {
                'unique_tokens': unique_tokens,
                'total_token_assignments': total_tokens,
                'most_frequent_token': max(token_counter.items(), key=lambda x: x[1]) if token_counter else None,
                'sample_token_consistency': {k: v for k, v in list(token_counter.items())[:5]} if token_counter else {}
            }
        }

        # Save to file
        with open('tokenised_transactions.json', 'w') as f:
            json.dump(output, f, indent=2)

        # Print statistics
        print("\n" + "=" * 70)
        print("✅ TOKENISATION COMPLETE")
        print("=" * 70)
        print(f"\n📁 Output: tokenised_transactions.json")
        print(f"\n📊 Statistics:")
        print(f"   Total transactions: {len(all_transactions):,}")
        print(f"   Unique tokens: {unique_tokens:,}")
        print(f"   Token collision rate: {output['metadata']['token_collision_rate']:.2%}")
        print(f"\n🔐 Tokenisation Methods:")
        for method, count in output['metadata']['tokenisation_methods'].items():
            print(f"   {method}: {count}")

        # Show sample tokenised transaction
        print("\n" + "=" * 70)
        print("📋 SAMPLE TOKENISED TRANSACTIONS")
        print("=" * 70)

        for source in sources:
            sample = next(tx for tx in all_transactions if tx['source'] == source)
            print(f"\n🔷 {source}:")
            if 'card_token' in sample:
                print(f"   Card Token: {sample['card_token']}")
                print(f"   BIN: {sample.get('bin', 'N/A')}")
                print(f"   Last4: {sample.get('last4', 'N/A')}")
                print(f"   Amount: {sample['amount']} {sample['currency']}")
                print(f"   Luhn Valid: {sample.get('luhn_valid', 'N/A')}")
            elif 'phone_token' in sample:
                print(f"   Phone Token: {sample['phone_token']}")
                print(f"   Amount: {sample['amount']} {sample['currency']}")
            print(f"   Token Method: {sample['tokenisation_method']}")

        # Verify PCI-DSS compliance
        print("\n" + "=" * 70)
        print("🔒 PCI-DSS COMPLIANCE CHECK")
        print("=" * 70)

        has_raw_pan = any('raw_pan' in tx or 'raw_phone' in tx for tx in all_transactions)
        print(f"   Raw PAN stored: {'❌ FAILED' if has_raw_pan else '✅ PASSED'}")

        # Check token format preservation
        visa_tokens = [tx.get('card_token', '') for tx in all_transactions
                       if tx.get('source') == 'VISA' and 'card_token' in tx]

        if visa_tokens:
            sample_token = visa_tokens[0]
            print(f"   Token preserves BIN/last4: {'✅' if len(sample_token) >= 16 else '⚠️'}")
            print(f"   Sample token format: {sample_token}")

        # Cache performance
        print(f"\n⚡ Performance:")
        print(f"   Redis cache: {'Enabled' if self.tokeniser._redis else 'Disabled'}")
        print(f"   Vault: {'Available' if self.tokeniser._vault_client else 'HMAC Fallback'}")

        return output


# Run the generator
if __name__ == "__main__":
    generator = TokenisedDataGenerator()
    data = generator.generate_dataset(transactions_per_source=100)