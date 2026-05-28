import json
import random
import uuid
import time
import hashlib
import hmac
import base64
from datetime import datetime, timedelta
from typing import Dict
import numpy as np
from faker import Faker

# Set seeds for reproducibility
random.seed(42)
np.random.seed(42)
fake = Faker()
Faker.seed(42)


class TransactionDataGenerator:
    def __init__(self):
        self.hmac_secret = b'shared_secret_key_for_webhook_verification_2024'

    # ── Luhn helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def _apply_luhn_check_digit(pan_without_check: str) -> str:
        """
        Given a PAN missing its final check digit, calculate and append it.
        Input must be exactly 15 digits for a 16-digit card (or N-1 for any length).
        """
        total = 0
        for i, digit in enumerate(reversed(pan_without_check)):
            n = int(digit)
            if i % 2 == 0:      # positions that will be "even from right" once check digit appended
                n *= 2
                if n > 9:
                    n -= 9
            total += n
        check_digit = (10 - (total % 10)) % 10
        return pan_without_check + str(check_digit)

    @staticmethod
    def _is_luhn_valid(pan: str) -> bool:
        """Verify a full PAN passes the Luhn check."""
        total = 0
        for i, digit in enumerate(reversed(pan)):
            n = int(digit)
            if i % 2 == 1:
                n *= 2
                if n > 9:
                    n -= 9
            total += n
        return total % 10 == 0

    @staticmethod
    def _generate_pan(bin_prefix: str, total_length: int = 16) -> str:
        """
        Generate a Luhn-valid PAN of `total_length` digits.
        Fills the middle with random digits and computes the check digit.
        """
        middle_length = total_length - len(bin_prefix) - 1     # -1 for check digit
        middle = ''.join([str(random.randint(0, 9)) for _ in range(middle_length)])
        return TransactionDataGenerator._apply_luhn_check_digit(bin_prefix + middle)

    # ── Source generators ─────────────────────────────────────────────────────

    def generate_visa_transaction(self) -> Dict:
        """Generate a Visa ISO 8583 transaction with a Luhn-valid PAN."""

        visa_bins = ['412345', '498765', '432187', '445678', '453201', '471610']
        bin_prefix = random.choice(visa_bins)
        pan = self._generate_pan(bin_prefix, total_length=16)

        assert self._is_luhn_valid(pan), f"Generated invalid Visa PAN: {pan}"

        transaction = {
            'source': 'VISA',
            'source_type': 'ISO_8583',
            'message_type': random.choice(['0100', '0110', '0200', '0210']),
            'bitmap': {
                'DE002': pan,
                'DE003': random.choice(['PURCHASE', 'WITHDRAWAL', 'BALANCE_INQUIRY']),
                'DE004': round(random.uniform(10, 5000), 2),
                'DE005': round(random.uniform(10, 5000), 2),
                'DE007': datetime.now().strftime('%m%d%H%M%S'),
                'DE011': str(random.randint(100000, 999999)),
                'DE012': datetime.now().strftime('%H%M%S'),
                'DE013': datetime.now().strftime('%m%d'),
                'DE018': random.choice(['5311', '5812', '5411', '7011']),
                'DE022': random.choice(['01', '02', '03', '05']),
                'DE025': random.choice(['01', '02']),
                'DE032': str(random.randint(10000, 99999)),
                'DE033': str(random.randint(10000, 99999)),
                'DE035': f"{pan}={random.randint(1000, 9999)}",
                'DE041': f"TERM{random.randint(1000, 9999)}",
                'DE042': f"MERC{random.randint(10000, 99999)}",
                'DE043': fake.company(),
                'DE049': random.choice(['840', '826', '404', '710']),
                'DE053': str(random.randint(100, 999)),
            },
            'card_data': {
                'bin': bin_prefix,
                'last4': pan[-4:],
                'card_type': 'CREDIT' if random.random() > 0.6 else 'DEBIT',
                'issuer_country': random.choice(['US', 'GB', 'KE', 'ZA']),
            },
            'timestamp': datetime.now().isoformat(),
            'event_time_ms': int(time.time() * 1000),
        }

        return transaction

    def generate_mastercard_transaction(self) -> Dict:
        """Generate a Mastercard ISO 8583 transaction with a Luhn-valid PAN."""

        mc_bins = ['512345', '534567', '545678', '556789', '522234', '531234']
        bin_prefix = random.choice(mc_bins)
        pan = self._generate_pan(bin_prefix, total_length=16)

        assert self._is_luhn_valid(pan), f"Generated invalid Mastercard PAN: {pan}"

        transaction = {
            'source': 'MASTERCARD',
            'source_type': 'ISO_8583',
            'message_type': random.choice(['0100', '0110', '0200', '0210', '0400']),
            'bitmap': {
                'DE002': pan,
                'DE003': random.choice(['PURCHASE', 'WITHDRAWAL', 'TRANSFER']),
                'DE004': round(random.uniform(10, 10000), 2),
                'DE005': round(random.uniform(10, 10000), 2),
                'DE007': datetime.now().strftime('%m%d%H%M%S'),
                'DE011': str(random.randint(100000, 999999)),
                'DE012': datetime.now().strftime('%H%M%S'),
                'DE013': datetime.now().strftime('%m%d'),
                'DE018': random.choice(['5311', '5812', '5411', '7011', '5732']),
                'DE022': random.choice(['01', '02', '03', '05', '07']),
                'DE025': random.choice(['01', '02', '03']),
                'DE032': str(random.randint(10000, 99999)),
                'DE033': str(random.randint(10000, 99999)),
                'DE035': f"{pan}={random.randint(1000, 9999)}",
                'DE041': f"TERM{random.randint(1000, 9999)}",
                'DE042': f"MERC{random.randint(10000, 99999)}",
                'DE043': fake.company(),
                'DE049': random.choice(['840', '826', '404', '710', '978']),
                'DE053': str(random.randint(100, 999)),
            },
            'card_data': {
                'bin': bin_prefix,
                'last4': pan[-4:],
                'card_type': 'WORLD_ELITE' if random.random() > 0.8 else 'STANDARD',
                'issuer_country': random.choice(['US', 'GB', 'KE', 'NG', 'ZA']),
            },
            'timestamp': datetime.now().isoformat(),
            'event_time_ms': int(time.time() * 1000),
        }

        return transaction

    def generate_mpesa_transaction(self) -> Dict:
        """Generate an M-Pesa Daraja webhook transaction."""

        transaction_id = f'MP{random.randint(100000000, 999999999)}'
        amount = round(random.uniform(10, 50000), 2)
        msisdn = f'254{random.randint(700000000, 799999999)}'

        payload_string = f"{transaction_id}{amount}{msisdn}"
        signature = hmac.new(
            self.hmac_secret,
            payload_string.encode(),
            hashlib.sha256
        ).hexdigest()

        transaction = {
            'source': 'MPESA',
            'source_type': 'DARAJA_WEBHOOK',
            'webhook_payload': {
                'TransactionType': random.choice([
                    'CustomerPayBillOnline', 'CustomerBuyGoodsOnline', 'STKPush'
                ]),
                'TransID': transaction_id,
                'TransTime': datetime.now().strftime('%Y%m%d%H%M%S'),
                'TransAmount': amount,
                'BusinessShortCode': random.randint(100000, 999999),
                'BillRefNumber': f'INV{random.randint(10000, 99999)}',
                'InvoiceNumber': f'INV{random.randint(10000, 99999)}',
                'OrgAccountBalance': round(random.uniform(10000, 1000000), 2),
                'ThirdPartyTransID': f'TP{random.randint(100000, 999999)}',
                'MSISDN': msisdn,
                'FirstName': fake.first_name(),
                'MiddleName': fake.last_name(),
                'LastName': fake.last_name(),
                'TransactionReceipt': f'RCT{random.randint(100000, 999999)}',
            },
            'signature': signature,
            'timestamp': datetime.now().isoformat(),
            'event_time_ms': int(time.time() * 1000),
        }

        return transaction

    def generate_flutterwave_transaction(self) -> Dict:
        """Generate a Flutterwave webhook transaction."""

        tx_ref = f'TX{int(time.time())}{random.randint(100, 999)}'
        amount = round(random.uniform(10, 5000), 2)

        payload = {
            'id': random.randint(1000000, 9999999),
            'tx_ref': tx_ref,
            'amount': amount,
        }
        verification_hash = hmac.new(
            self.hmac_secret,
            json.dumps(payload, sort_keys=True).encode(),
            hashlib.sha512
        ).hexdigest()

        transaction = {
            'source': 'FLUTTERWAVE',
            'source_type': 'WEBHOOK',
            'webhook_payload': {
                'event': random.choice([
                    'charge.completed', 'transfer.completed', 'payment.verified'
                ]),
                'data': {
                    'id': random.randint(1000000, 9999999),
                    'tx_ref': tx_ref,
                    'flw_ref': f'FLW{random.randint(100000000, 999999999)}',
                    'device_fingerprint': hashlib.md5(
                        f"device_{random.randint(1, 10000)}".encode()
                    ).hexdigest(),
                    'amount': amount,
                    'currency': random.choice(['KES', 'USD', 'GBP', 'EUR']),
                    'charged_amount': amount,
                    'app_fee': round(amount * 0.025, 2),
                    'merchant_fee': round(amount * 0.01, 2),
                    'processor_response': random.choice(['Approved', 'Declined']),
                    'auth_model': random.choice(['PIN', 'NOAUTH', 'VBV', 'OTP']),
                    'payment_type': random.choice(['card', 'mpesa', 'banktransfer']),
                    'status': random.choice(['successful', 'pending', 'failed']),
                    'customer': {
                        'id': random.randint(1000, 9999),
                        'name': fake.name(),
                        'phone_number': f'254{random.randint(700000000, 799999999)}',
                        'email': fake.email(),
                    },
                    'card': {
                        'first_6digits': random.choice(['520000', '530000', '540000']),
                        'last_4digits': str(random.randint(1000, 9999)),
                        'issuer': random.choice(['VISA', 'MASTERCARD']),
                        'country': 'KE',
                        'type': random.choice(['DEBIT', 'CREDIT']),
                    },
                },
            },
            'verification_hash': verification_hash,
            'timestamp': datetime.now().isoformat(),
            'event_time_ms': int(time.time() * 1000),
        }

        return transaction

    def generate_pesapal_transaction(self) -> Dict:
        """Generate a Pesapal IPN callback transaction."""

        merchant_ref = f'REF{int(time.time())}{random.randint(100, 999)}'
        tracking_id = f'TRACK{random.randint(100000000, 999999999)}'
        amount = round(random.uniform(10, 50000), 2)

        signature_string = f"{merchant_ref}{tracking_id}COMPLETED"
        ipn_signature = base64.b64encode(
            hmac.new(self.hmac_secret, signature_string.encode(), hashlib.sha1).digest()
        ).decode()

        transaction = {
            'source': 'PESAPAL',
            'source_type': 'IPN_CALLBACK',
            'ipn_payload': {
                'pesapal_merchant_reference': merchant_ref,
                'pesapal_transaction_tracking_id': tracking_id,
                'payment_status': random.choice(['COMPLETED', 'PENDING', 'FAILED', 'INVALID']),
                'payment_method': random.choice(['VISA', 'MASTERCARD', 'MPESA', 'AIRTEL_MONEY']),
                'amount': amount,
                'currency': random.choice(['KES', 'USD', 'UGX', 'TZS', 'GBP']),
                'created_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'confirmation_code': f'CODE{random.randint(10000, 99999)}',
                'payment_account': f'254{random.randint(700000000, 799999999)}',
                'customer_email': fake.email(),
                'customer_phone': f'254{random.randint(700000000, 799999999)}',
                'customer_first_name': fake.first_name(),
                'customer_last_name': fake.last_name(),
                'description': f'Payment for order {random.randint(1000, 9999)}',
            },
            'ipn_signature': ipn_signature,
            'timestamp': datetime.now().isoformat(),
            'event_time_ms': int(time.time() * 1000),
        }

        return transaction

    def generate_fraud_transaction(self, source: str) -> Dict:
        """Generate a fraudulent transaction pattern for a given source."""

        if source == 'VISA':
            tx = self.generate_visa_transaction()
            tx['bitmap']['DE004'] = round(random.uniform(5000, 25000), 2)
            tx['bitmap']['DE022'] = '07'
            tx['bitmap']['DE049'] = '156'
            tx['fraud_pattern'] = 'UNUSUAL_AMOUNT_AND_LOCATION'
            tx['risk_score'] = round(random.uniform(85, 99), 2)

        elif source == 'MASTERCARD':
            tx = self.generate_mastercard_transaction()
            tx['velocity_spike'] = True
            tx['fraud_pattern'] = 'VELOCITY_SPIKE'
            tx['risk_score'] = round(random.uniform(88, 98), 2)

        elif source == 'MPESA':
            tx = self.generate_mpesa_transaction()
            tx['webhook_payload']['TransactionType'] = 'STKPush'
            tx['webhook_payload']['TransAmount'] = round(random.uniform(50000, 200000), 2)
            tx['fraud_pattern'] = 'HIGH_VALUE_RAPID_SUCCESS'
            tx['risk_score'] = round(random.uniform(90, 99), 2)

        elif source == 'FLUTTERWAVE':
            tx = self.generate_flutterwave_transaction()
            tx['webhook_payload']['data']['auth_model'] = 'NOAUTH'
            tx['webhook_payload']['data']['payment_type'] = 'card'
            tx['fraud_pattern'] = 'CARD_NOT_PRESENT_NO_AUTH'
            tx['risk_score'] = round(random.uniform(92, 99), 2)

        else:  # PESAPAL
            tx = self.generate_pesapal_transaction()
            tx['ipn_payload']['amount'] = round(random.uniform(10, 100), 2)
            tx['ipn_payload']['payment_status'] = 'COMPLETED'
            tx['fraud_pattern'] = 'MICRO_DEPOSIT_FRAUD'
            tx['risk_score'] = round(random.uniform(70, 90), 2)

        tx['is_fraud'] = True
        tx['fraud_detection_time_ms'] = random.randint(50, 200)
        return tx


# ── Batch generation ──────────────────────────────────────────────────────────

def generate_all_transactions():
    print("=" * 70)
    print("FRAUD DETECTION DATA GENERATOR")
    print("Sources: Visa | Mastercard | M-Pesa | Flutterwave | Pesapal")
    print("=" * 70)

    generator = TransactionDataGenerator()
    all_transactions = []

    source_counts = {
        'VISA': 500,
        'MASTERCARD': 500,
        'MPESA': 400,
        'FLUTTERWAVE': 300,
        'PESAPAL': 300,
    }

    generators = {
        'VISA': generator.generate_visa_transaction,
        'MASTERCARD': generator.generate_mastercard_transaction,
        'MPESA': generator.generate_mpesa_transaction,
        'FLUTTERWAVE': generator.generate_flutterwave_transaction,
        'PESAPAL': generator.generate_pesapal_transaction,
    }

    print("\nGenerating normal transactions...")
    for source, count in source_counts.items():
        print(f"  Generating {count} {source} transactions...")
        for _ in range(count):
            tx = generators[source]()
            tx['is_fraud'] = False
            tx['risk_score'] = round(random.uniform(1, 40), 2)
            all_transactions.append(tx)

    total_normal = sum(source_counts.values())
    fraud_count = int(total_normal * 0.1)

    print(f"\nGenerating {fraud_count} fraud transactions (10% of total)...")
    sources_list = list(source_counts.keys())
    for _ in range(fraud_count):
        source = random.choice(sources_list)
        all_transactions.append(generator.generate_fraud_transaction(source))

    random.shuffle(all_transactions)

    # Validate all card PANs before writing
    invalid = 0
    for tx in all_transactions:
        if tx['source'] in ('VISA', 'MASTERCARD'):
            pan = tx['bitmap']['DE002']
            if not generator._is_luhn_valid(pan):
                invalid += 1
    if invalid:
        print(f"\nWARNING: {invalid} invalid PANs found — check generator logic")
    else:
        print(f"\nAll card PANs passed Luhn validation ✓")

    output = {
        'metadata': {
            'total_transactions': len(all_transactions),
            'normal_transactions': total_normal,
            'fraud_transactions': fraud_count,
            'fraud_rate': round(fraud_count / len(all_transactions), 4),
            'sources': source_counts,
            'generated_at': datetime.now().isoformat(),
            'version': '1.1',
        },
        'transactions': all_transactions,
    }

    with open('transactions_all_sources.json', 'w') as f:
        json.dump(output, f, indent=2)

    print("\n" + "=" * 70)
    print("DATA GENERATION COMPLETE")
    print("=" * 70)
    print(f"\nOutput: transactions_all_sources.json")
    print(f"\nStatistics:")
    print(f"  Total:  {len(all_transactions):,}")
    print(f"  Normal: {total_normal:,}")
    print(f"  Fraud:  {fraud_count:,} ({fraud_count / len(all_transactions) * 100:.1f}%)")

    print(f"\nBy source:")
    for source in sources_list:
        source_tx = [t for t in all_transactions if t['source'] == source]
        fraud_tx  = [t for t in source_tx if t.get('is_fraud')]
        print(f"  {source:<12} {len(source_tx):>4} total  {len(fraud_tx):>3} fraud "
              f"({len(fraud_tx) / len(source_tx) * 100:.1f}%)")

    print("\n" + "=" * 70)
    print("SAMPLE TRANSACTIONS BY SOURCE")
    print("=" * 70)
    for source in sources_list:
        sample = next(t for t in all_transactions if t['source'] == source)
        print(f"\n{source}")
        if source in ('VISA', 'MASTERCARD'):
            pan = sample['bitmap']['DE002']
            print(f"  PAN:      {pan[:6]}******{pan[-4:]}  (Luhn valid: {generator._is_luhn_valid(pan)})")
            print(f"  Amount:   {sample['bitmap']['DE004']} {sample['bitmap']['DE049']}")
            print(f"  Merchant: {sample['bitmap']['DE043']}")
        elif source == 'MPESA':
            print(f"  Amount: {sample['webhook_payload']['TransAmount']} KES")
            print(f"  TxID:   {sample['webhook_payload']['TransID']}")
        elif source == 'FLUTTERWAVE':
            d = sample['webhook_payload']['data']
            print(f"  Amount: {d['amount']} {d['currency']}")
            print(f"  Status: {d['status']}")
        else:
            p = sample['ipn_payload']
            print(f"  Amount: {p['amount']} {p['currency']}")
            print(f"  Status: {p['payment_status']}")
        if sample.get('is_fraud'):
            print(f"  FRAUD — {sample.get('fraud_pattern')}  risk={sample.get('risk_score')}")

    fraud_patterns: Dict[str, int] = {}
    for tx in all_transactions:
        if tx.get('is_fraud'):
            p = tx.get('fraud_pattern', 'UNKNOWN')
            fraud_patterns[p] = fraud_patterns.get(p, 0) + 1

    print("\n" + "=" * 70)
    print("FRAUD PATTERN SUMMARY")
    print("=" * 70)
    for pattern, count in sorted(fraud_patterns.items(), key=lambda x: -x[1]):
        print(f"  {pattern}: {count}")

    return output


if __name__ == "__main__":
    data = generate_all_transactions()