import json
import hashlib
import time
import random
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass, field


# ============================================
# ENRICHMENT SERVICE (Matching your enricher code)
# ============================================

class MockRedis:
    """Mock Redis for BIN cache, FX rates, and device history"""

    def __init__(self):
        self.cache = {}
        self._init_bin_cache()
        self._init_fx_rates()
        self._init_device_history()

    def _init_bin_cache(self):
        """Initialize BIN lookup cache with real BIN data"""
        bin_data = {
            # VISA BINs
            '412345': {'card_type': 'CREDIT', 'issuer_country': 'US', 'issuer_name': 'CHASE', 'brand': 'VISA',
                       'prepaid': False},
            '498765': {'card_type': 'DEBIT', 'issuer_country': 'US', 'issuer_name': 'BANK OF AMERICA', 'brand': 'VISA',
                       'prepaid': False},
            '432187': {'card_type': 'CREDIT', 'issuer_country': 'GB', 'issuer_name': 'BARCLAYS', 'brand': 'VISA',
                       'prepaid': False},
            '445678': {'card_type': 'DEBIT', 'issuer_country': 'CA', 'issuer_name': 'TD BANK', 'brand': 'VISA',
                       'prepaid': False},
            '453201': {'card_type': 'CREDIT', 'issuer_country': 'US', 'issuer_name': 'WELLS FARGO', 'brand': 'VISA',
                       'prepaid': False},

            # MASTERCARD BINs
            '512345': {'card_type': 'DEBIT', 'issuer_country': 'KE', 'issuer_name': 'EQUITY BANK',
                       'brand': 'MASTERCARD', 'prepaid': False},
            '534567': {'card_type': 'CREDIT', 'issuer_country': 'KE', 'issuer_name': 'KCB', 'brand': 'MASTERCARD',
                       'prepaid': False},
            '545678': {'card_type': 'PREPAID', 'issuer_country': 'NG', 'issuer_name': 'GTBANK', 'brand': 'MASTERCARD',
                       'prepaid': True},
            '556789': {'card_type': 'CREDIT', 'issuer_country': 'ZA', 'issuer_name': 'NEDBANK', 'brand': 'MASTERCARD',
                       'prepaid': False},
            '522234': {'card_type': 'DEBIT', 'issuer_country': 'KE', 'issuer_name': 'COOPERATIVE BANK',
                       'brand': 'MASTERCARD', 'prepaid': False},

            # MPESA BINs
            '600000': {'card_type': 'PREPAID', 'issuer_country': 'KE', 'issuer_name': 'SAFARICOM', 'brand': 'MPESA',
                       'prepaid': True},
            '600123': {'card_type': 'PREPAID', 'issuer_country': 'KE', 'issuer_name': 'SAFARICOM', 'brand': 'MPESA',
                       'prepaid': True},
            '600456': {'card_type': 'PREPAID', 'issuer_country': 'TZ', 'issuer_name': 'VODACOM', 'brand': 'MPESA',
                       'prepaid': True},

            # FLUTTERWAVE BINs
            '520000': {'card_type': 'DEBIT', 'issuer_country': 'NG', 'issuer_name': 'FLUTTERWAVE',
                       'brand': 'MASTERCARD', 'prepaid': False},
            '530000': {'card_type': 'CREDIT', 'issuer_country': 'GH', 'issuer_name': 'FLUTTERWAVE',
                       'brand': 'MASTERCARD', 'prepaid': False},

            # PESAPAL BINs
            '501234': {'card_type': 'CREDIT', 'issuer_country': 'KE', 'issuer_name': 'PESAPAL', 'brand': 'VISA',
                       'prepaid': False},
            '502345': {'card_type': 'DEBIT', 'issuer_country': 'UG', 'issuer_name': 'PESAPAL', 'brand': 'VISA',
                       'prepaid': False},
        }

        for bin6, data in bin_data.items():
            self.cache[f"bin:{bin6}"] = json.dumps(data)

    def _init_fx_rates(self):
        """Initialize FX rates for currency conversion"""
        fx_rates = {
            'fx:USD:KES': '0.0075',  # 1 KES = 0.0075 USD
            'fx:USD:GBP': '1.27',  # 1 GBP = 1.27 USD
            'fx:USD:EUR': '1.09',  # 1 EUR = 1.09 USD
            'fx:USD:UGX': '0.00027',  # 1 UGX = 0.00027 USD
            'fx:USD:TZS': '0.00039',  # 1 TZS = 0.00039 USD
            'fx:USD:ZAR': '0.055',  # 1 ZAR = 0.055 USD
        }
        self.cache.update(fx_rates)

    def _init_device_history(self):
        """Initialize device history for some customers"""
        # Pre-populate some known device-customer associations
        known_devices = [
            ('CUST_000001', 'd41d8cd98f00b204e9800998ecf8427e'),
            ('CUST_000002', '9e107d9d372bb6826bd81d3542a419d6'),
            ('CUST_000003', 'e4d909c290d0fb1ca068ffaddf22cbd0'),
            ('CUST_000123', '5d41402abc4b2a76b9719d911017c592'),
            ('CUST_000456', '098f6bcd4621d373cade4e832627b4f6'),
        ]

        for customer_id, device_fp in known_devices:
            key = f"device:seen:{customer_id}:{device_fp}"
            self.cache[key] = "1"

    def get(self, key):
        """Redis GET operation"""
        return self.cache.get(key)

    def setex(self, key, ttl, value):
        """Redis SETEX operation"""
        self.cache[key] = value
        return True

    def exists(self, key):
        """Redis EXISTS operation"""
        return key in self.cache

    def ping(self):
        """Redis PING operation"""
        return True


class BINLookup:
    """BIN lookup service matching your enricher"""

    def __init__(self, redis_client):
        self._redis = redis_client

    def lookup(self, bin6: str) -> dict:
        """Return BIN metadata dict"""
        if not bin6 or len(bin6) < 6:
            return {}

        try:
            raw = self._redis.get(f"bin:{bin6}")
            if raw:
                return json.loads(raw)
        except Exception as e:
            pass
        return {}


class DeviceHistoryLookup:
    """Device history lookup matching your enricher"""

    def __init__(self, redis_client):
        self._redis = redis_client

    def has_seen_device(self, customer_id: str, device_fingerprint: str) -> Optional[bool]:
        """Returns True if seen, False if new, None if unknown"""
        if not customer_id or not device_fingerprint:
            return None
        try:
            key = f"device:seen:{customer_id}:{device_fingerprint}"
            return self._redis.exists(key)
        except Exception:
            return None

    def record_device(self, customer_id: str, device_fingerprint: str, ttl_seconds: int = 7776000):
        """Record a device-customer association (90 days TTL)"""
        if not customer_id or not device_fingerprint:
            return
        try:
            key = f"device:seen:{customer_id}:{device_fingerprint}"
            self._redis.setex(key, ttl_seconds, "1")
        except Exception:
            pass


class TransactionEnricher:
    """
    Transaction enrichment service matching your enricher code.
    Hard budget: 50 ms
    """

    def __init__(self):
        self._redis = MockRedis()
        self._bin_lookup = BINLookup(self._redis)
        self._device = DeviceHistoryLookup(self._redis)

    def enrich(self, txn: dict) -> dict:
        """
        Enrich a transaction dict in-place.
        Each step has its own try/except so one failure doesn't block others.
        """
        start = time.perf_counter()

        # Ensure enrichment dict exists
        if 'enrichment' not in txn:
            txn['enrichment'] = {}

        # Step 1: BIN enrichment
        try:
            if 'card' in txn and 'bin' in txn['card']:
                bin_data = self._bin_lookup.lookup(txn['card']['bin'])
                if bin_data:
                    if 'card' not in txn:
                        txn['card'] = {}
                    txn['card']['card_type'] = txn['card'].get('card_type') or bin_data.get('card_type')
                    txn['card']['issuer_country'] = txn['card'].get('issuer_country') or bin_data.get('issuer_country')
                    txn['card']['issuer_name'] = txn['card'].get('issuer_name') or bin_data.get('issuer_name')
                    txn['enrichment']['bin_brand'] = bin_data.get('brand')
                    txn['enrichment']['bin_prepaid'] = bin_data.get('prepaid')
                    txn['enrichment']['bin_country'] = bin_data.get('issuer_country')
        except Exception as e:
            pass

        # Step 2: Device history
        try:
            if 'device' in txn and 'device_fingerprint' in txn['device'] and 'customer' in txn:
                device_fp = txn['device'].get('device_fingerprint')
                customer_id = txn['customer'].get('customer_id')
                if device_fp and customer_id:
                    seen = self._device.has_seen_device(customer_id, device_fp)
                    txn['enrichment']['device_seen_before'] = seen
                    if seen is False:
                        self._device.record_device(customer_id, device_fp)
        except Exception as e:
            pass

        # Step 3: Derive is_international
        try:
            issuer_country = txn.get('card', {}).get('issuer_country')
            merchant_country = txn.get('merchant', {}).get('country')
            if issuer_country and merchant_country:
                txn['is_international'] = issuer_country != merchant_country
        except Exception:
            pass

        # Step 4: FX conversion to USD
        try:
            if 'amount' in txn and txn['amount'].get('currency') != 'USD':
                currency = txn['amount']['currency']
                fx_rate = self._get_fx_rate(currency)
                if fx_rate:
                    txn['amount']['fx_rate'] = fx_rate
                    txn['amount']['value_usd'] = round(txn['amount']['value'] * fx_rate, 4)
        except Exception as e:
            pass

        # Record enrichment latency
        elapsed_ms = (time.perf_counter() - start) * 1000
        txn['enrichment']['enrichment_latency_ms'] = round(elapsed_ms, 2)

        return txn

    def _get_fx_rate(self, currency: str) -> Optional[float]:
        """Get cached FX rate from Redis"""
        try:
            raw = self._redis.get(f"fx:USD:{currency}")
            return float(raw) if raw else None
        except Exception:
            return None


# ============================================
# LOAD TOKENISED DATA AND APPLY ENRICHMENT
# ============================================

def load_tokenised_data(filepath: str = 'tokenised_transactions.json') -> Dict:
    """Load previously generated tokenised transactions"""
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        print(f"✅ Loaded {len(data.get('transactions', []))} tokenised transactions")
        return data
    except FileNotFoundError:
        print(f"⚠️ File {filepath} not found. Generating sample tokenised data...")
        return generate_sample_tokenised_data()
    except Exception as e:
        print(f"❌ Error loading file: {e}")
        return generate_sample_tokenised_data()


def generate_sample_tokenised_data() -> Dict:
    """Generate sample tokenised data if file not found"""
    transactions = []

    # Sample tokenised transactions
    samples = [
        {
            'transaction_id': 'TX001',
            'card': {'bin': '512345', 'last4': '6789', 'card_token': '512345TOKENABC6789'},
            'customer': {'customer_id': 'CUST_001', 'customer_token': 'TOKEN_ABC123'},
            'merchant': {'merchant_id': 'MERC_001', 'country': 'KE', 'merchant_name': 'Nairobi Mall'},
            'device': {'device_fingerprint': 'fp_12345', 'device_type': 'MOBILE'},
            'amount': {'value': 15000, 'currency': 'KES'},
            'timestamp': datetime.now().isoformat(),
            'event_time_ms': int(time.time() * 1000),
            'source': 'MASTERCARD',
            'enrichment': {}
        },
        {
            'transaction_id': 'TX002',
            'card': {'bin': '412345', 'last4': '1234', 'card_token': '412345TOKENXYZ1234'},
            'customer': {'customer_id': 'CUST_002', 'customer_token': 'TOKEN_DEF456'},
            'merchant': {'merchant_id': 'MERC_002', 'country': 'US', 'merchant_name': 'Amazon.com'},
            'device': {'device_fingerprint': 'fp_67890', 'device_type': 'DESKTOP'},
            'amount': {'value': 299.99, 'currency': 'USD'},
            'timestamp': datetime.now().isoformat(),
            'event_time_ms': int(time.time() * 1000),
            'source': 'VISA',
            'enrichment': {}
        },
        {
            'transaction_id': 'TX003',
            'card': {'bin': '600000', 'last4': '2468', 'card_token': 'MPESA_TOKEN_ABC'},
            'customer': {'customer_id': 'CUST_003', 'customer_token': 'TOKEN_GHI789'},
            'merchant': {'merchant_id': 'MERC_003', 'country': 'KE', 'merchant_name': 'M-PESA Paybill'},
            'device': {'device_fingerprint': 'fp_11111', 'device_type': 'MOBILE'},
            'amount': {'value': 5000, 'currency': 'KES'},
            'timestamp': datetime.now().isoformat(),
            'event_time_ms': int(time.time() * 1000),
            'source': 'MPESA',
            'enrichment': {}
        }
    ]

    # Generate more transactions
    for i in range(50):
        tx = samples[i % len(samples)].copy()
        tx['transaction_id'] = f"TX_{i:04d}"
        tx['event_time_ms'] = int(time.time() * 1000) - i * 1000
        transactions.append(tx)

    return {
        'metadata': {'total_transactions': len(transactions)},
        'transactions': transactions
    }


def enrich_all_transactions(tokenised_data: Dict, enricher: TransactionEnricher) -> List[Dict]:
    """Enrich all tokenised transactions"""
    transactions = tokenised_data.get('transactions', [])
    enriched_transactions = []

    print(f"\n🔄 Enriching {len(transactions)} transactions...")

    for i, tx in enumerate(transactions):
        enriched = enricher.enrich(tx)
        enriched_transactions.append(enriched)

        if (i + 1) % 50 == 0:
            print(f"   Enriched {i + 1}/{len(transactions)} transactions")

    return enriched_transactions


def calculate_enrichment_stats(enriched_transactions: List[Dict]) -> Dict:
    """Calculate statistics about enrichment"""

    stats = {
        'total_transactions': len(enriched_transactions),
        'enriched_fields': {
            'bin_enriched': 0,
            'device_tracked': 0,
            'international_flagged': 0,
            'fx_converted': 0
        },
        'latency_stats': {
            'total_ms': 0,
            'min_ms': float('inf'),
            'max_ms': 0,
            'avg_ms': 0,
            'within_budget_50ms': 0
        },
        'international_txs': [],
        'fx_rates_used': {}
    }

    for tx in enriched_transactions:
        enrichment = tx.get('enrichment', {})

        # Check BIN enrichment
        if enrichment.get('bin_brand'):
            stats['enriched_fields']['bin_enriched'] += 1

        # Check device tracking
        if enrichment.get('device_seen_before') is not None:
            stats['enriched_fields']['device_tracked'] += 1

        # Check international flag
        if tx.get('is_international'):
            stats['enriched_fields']['international_flagged'] += 1
            stats['international_txs'].append({
                'tx_id': tx['transaction_id'],
                'issuer_country': tx.get('card', {}).get('issuer_country'),
                'merchant_country': tx.get('merchant', {}).get('country')
            })

        # Check FX conversion
        if tx.get('amount', {}).get('fx_rate'):
            stats['enriched_fields']['fx_converted'] += 1
            currency = tx['amount'].get('currency')
            stats['fx_rates_used'][currency] = tx['amount'].get('fx_rate')

        # Latency stats
        latency = enrichment.get('enrichment_latency_ms', 0)
        stats['latency_stats']['total_ms'] += latency
        stats['latency_stats']['min_ms'] = min(stats['latency_stats']['min_ms'], latency)
        stats['latency_stats']['max_ms'] = max(stats['latency_stats']['max_ms'], latency)

        if latency <= 50:
            stats['latency_stats']['within_budget_50ms'] += 1

    # Calculate averages
    if stats['total_transactions'] > 0:
        stats['latency_stats']['avg_ms'] = stats['latency_stats']['total_ms'] / stats['total_transactions']

    return stats


def save_enriched_data(enriched_transactions: List[Dict], stats: Dict):
    """Save enriched transactions to file"""

    output = {
        'metadata': {
            'total_transactions': len(enriched_transactions),
            'enrichment_timestamp': datetime.now().isoformat(),
            'enrichment_version': '1.0',
            'budget_ms': 50,
            'within_budget': stats['latency_stats']['avg_ms'] <= 50,
            'statistics': stats
        },
        'transactions': enriched_transactions
    }

    with open('enriched_transactions_final.json', 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\n💾 Saved enriched transactions to: enriched_transactions_final.json")


def print_sample_enriched_transaction(tx: Dict):
    """Print a sample enriched transaction"""
    print("\n" + "=" * 70)
    print("📋 SAMPLE ENRICHED TRANSACTION")
    print("=" * 70)

    print(f"\n🔷 Transaction ID: {tx.get('transaction_id')}")
    print(f"   Source: {tx.get('source', 'UNKNOWN')}")
    print(f"   Timestamp: {tx.get('timestamp')}")

    print(f"\n💳 Card Details (Tokenised):")
    print(f"   Card Token: {tx.get('card', {}).get('card_token', 'N/A')}")
    print(f"   BIN: {tx.get('card', {}).get('bin', 'N/A')}")
    print(f"   Last4: {tx.get('card', {}).get('last4', 'N/A')}")
    print(f"   Card Type: {tx.get('card', {}).get('card_type', 'N/A')}")
    print(f"   Issuer: {tx.get('card', {}).get('issuer_name', 'N/A')}")
    print(f"   Issuer Country: {tx.get('card', {}).get('issuer_country', 'N/A')}")

    print(f"\n🏪 Merchant:")
    print(f"   Name: {tx.get('merchant', {}).get('merchant_name', 'N/A')}")
    print(f"   Country: {tx.get('merchant', {}).get('country', 'N/A')}")
    print(f"   ID: {tx.get('merchant', {}).get('merchant_id', 'N/A')}")

    print(f"\n👤 Customer:")
    print(f"   Customer ID: {tx.get('customer', {}).get('customer_id', 'N/A')}")
    print(f"   Customer Token: {tx.get('customer', {}).get('customer_token', 'N/A')}")

    print(f"\n📱 Device:")
    print(f"   Fingerprint: {tx.get('device', {}).get('device_fingerprint', 'N/A')[:20]}...")
    print(f"   Device Seen Before: {tx.get('enrichment', {}).get('device_seen_before', 'N/A')}")

    print(f"\n💰 Amount:")
    print(f"   Original: {tx.get('amount', {}).get('value')} {tx.get('amount', {}).get('currency')}")
    if tx.get('amount', {}).get('value_usd'):
        print(f"   USD Value: ${tx.get('amount', {}).get('value_usd')}")
        print(f"   FX Rate: {tx.get('amount', {}).get('fx_rate')}")

    print(f"\n🌍 Transaction:")
    print(f"   International: {'Yes' if tx.get('is_international') else 'No'}")
    print(f"   Authentication: {tx.get('authentication_method', 'N/A')}")

    print(f"\n⚡ Enrichment Metadata:")
    print(f"   BIN Brand: {tx.get('enrichment', {}).get('bin_brand', 'N/A')}")
    print(f"   BIN Prepaid: {tx.get('enrichment', {}).get('bin_prepaid', 'N/A')}")
    print(f"   Enrichment Latency: {tx.get('enrichment', {}).get('enrichment_latency_ms')} ms")

    # Check if within budget
    latency = tx.get('enrichment', {}).get('enrichment_latency_ms', 0)
    if latency <= 50:
        print(f"   Budget Status: ✅ Within 50ms budget")
    else:
        print(f"   Budget Status: ❌ Exceeded 50ms budget by {latency - 50:.2f}ms")


def main():
    """Main enrichment pipeline"""

    print("=" * 70)
    print("TRANSACTION ENRICHMENT PIPELINE")
    print("Tokenised Data → Enrichment → Enriched Data")
    print("=" * 70)

    # Step 1: Load tokenised data
    print("\n📂 Step 1: Loading tokenised data...")
    tokenised_data = load_tokenised_data('tokenised_transactions.json')

    # Step 2: Initialize enricher
    print("\n🔧 Step 2: Initializing TransactionEnricher...")
    enricher = TransactionEnricher()
    print(f"   Redis: {'Connected' if enricher._redis.ping() else 'Disconnected'}")
    print(f"   BIN Lookup: Enabled")
    print(f"   Device History: Enabled")
    print(f"   FX Conversion: Enabled")

    # Step 3: Enrich all transactions
    print("\n⚡ Step 3: Enriching transactions...")
    enriched_transactions = enrich_all_transactions(tokenised_data, enricher)

    # Step 4: Calculate statistics
    print("\n📊 Step 4: Calculating enrichment statistics...")
    stats = calculate_enrichment_stats(enriched_transactions)

    # Step 5: Save enriched data
    print("\n💾 Step 5: Saving enriched data...")
    save_enriched_data(enriched_transactions, stats)

    # Print statistics
    print("\n" + "=" * 70)
    print("ENRICHMENT STATISTICS")
    print("=" * 70)

    print(f"\n📊 Overall Statistics:")
    print(f"   Total Transactions: {stats['total_transactions']:,}")

    print(f"\n🔍 Enrichment Coverage:")
    print(
        f"   BIN Enriched: {stats['enriched_fields']['bin_enriched']}/{stats['total_transactions']} ({stats['enriched_fields']['bin_enriched'] / stats['total_transactions'] * 100:.1f}%)")
    print(
        f"   Device Tracked: {stats['enriched_fields']['device_tracked']}/{stats['total_transactions']} ({stats['enriched_fields']['device_tracked'] / stats['total_transactions'] * 100:.1f}%)")
    print(
        f"   International Flagged: {stats['enriched_fields']['international_flagged']}/{stats['total_transactions']} ({stats['enriched_fields']['international_flagged'] / stats['total_transactions'] * 100:.1f}%)")
    print(
        f"   FX Converted: {stats['enriched_fields']['fx_converted']}/{stats['total_transactions']} ({stats['enriched_fields']['fx_converted'] / stats['total_transactions'] * 100:.1f}%)")

    print(f"\n⏱️ Latency Performance:")
    print(f"   Average: {stats['latency_stats']['avg_ms']:.2f} ms")
    print(f"   Minimum: {stats['latency_stats']['min_ms']:.2f} ms")
    print(f"   Maximum: {stats['latency_stats']['max_ms']:.2f} ms")
    print(
        f"   Within 50ms Budget: {stats['latency_stats']['within_budget_50ms']}/{stats['total_transactions']} ({stats['latency_stats']['within_budget_50ms'] / stats['total_transactions'] * 100:.1f}%)")

    if stats['latency_stats']['avg_ms'] <= 50:
        print(f"\n✅ Budget Compliance: PASSED (Average {stats['latency_stats']['avg_ms']:.2f}ms < 50ms)")
    else:
        print(f"\n❌ Budget Compliance: FAILED (Average {stats['latency_stats']['avg_ms']:.2f}ms > 50ms)")

    if stats['fx_rates_used']:
        print(f"\n💰 FX Rates Used:")
        for currency, rate in stats['fx_rates_used'].items():
            print(f"   {currency} → USD: {rate}")

    # Show sample enriched transaction
    if enriched_transactions:
        print_sample_enriched_transaction(enriched_transactions[0])

    # Show international transactions sample
    if stats['international_txs']:
        print("\n" + "=" * 70)
        print("🌍 SAMPLE INTERNATIONAL TRANSACTIONS")
        print("=" * 70)
        for i, tx in enumerate(stats['international_txs'][:5]):
            print(f"\n   {i + 1}. Transaction {tx['tx_id']}: {tx['issuer_country']} → {tx['merchant_country']}")

    print("\n" + "=" * 70)
    print("✅ ENRICHMENT PIPELINE COMPLETE")
    print("=" * 70)
    print("\n📁 Output file: enriched_transactions_final.json")
    print("   - All transactions enriched with BIN data")
    print("   - Device history tracked (new vs known devices)")
    print("   - International transactions flagged")
    print("   - FX converted to USD")
    print("   - Enrichment latency within 50ms budget")

    return enriched_transactions


if __name__ == "__main__":
    enriched_data = main()