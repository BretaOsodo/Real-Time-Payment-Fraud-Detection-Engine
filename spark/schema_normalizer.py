"""
Schema Normaliser

Maps each provider's tokenised message format to a single canonical
schema so all downstream enrichment and feature engineering steps
work on one consistent DataFrame structure regardless of source.

Canonical fields produced:
    transaction_id, provider, source_type, event_timestamp,
    card_token, bin, last4, amount, currency, currency_code,
    merchant_id, merchant_name, mcc, terminal_id,
    transaction_type, pos_entry_mode, channel,
    customer_id, device_fingerprint, is_fraud, risk_score
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, coalesce, to_timestamp, from_unixtime,
    upper, trim, lpad,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, BooleanType, TimestampType,
)

#canonical schema
CANONICAL_SCHEMA = StructType([
    StructField("transaction_id",     StringType(),    False),
    StructField("provider",           StringType(),    False),
    StructField("source_type",        StringType(),    True),
    StructField("event_timestamp",    TimestampType(), False),
    StructField("event_time_ms",      LongType(),      True),
    StructField("card_token",         StringType(),    True),
    StructField("bin",                StringType(),    True),
    StructField("last4",              StringType(),    True),
    StructField("amount",             DoubleType(),    True),
    StructField("currency",           StringType(),    True),
    StructField("currency_code",      StringType(),    True),
    StructField("merchant_id",        StringType(),    True),
    StructField("merchant_name",      StringType(),    True),
    StructField("mcc",                StringType(),    True),
    StructField("terminal_id",        StringType(),    True),
    StructField("transaction_type",   StringType(),    True),
    StructField("pos_entry_mode",     StringType(),    True),
    StructField("channel",            StringType(),    True),
    StructField("customer_id",        StringType(),    True),
    StructField("device_fingerprint", StringType(),    True),
    StructField("payment_method",     StringType(),    True),
    StructField("auth_model",         StringType(),    True),
    StructField("status",             StringType(),    True),
    StructField("acquirer_id",        StringType(),    True),
    StructField("stan",               StringType(),    True),
    StructField("is_fraud",           BooleanType(),   True),
    StructField("risk_score",         DoubleType(),    True),
    StructField("fraud_pattern",      StringType(),    True),
])

class SchemaNormalizer:
    """
    Normalises provider-specific tokenised messages to the canonical schema.
    Called once per micro-batch after Avro parsing.
    """

    def normalize(self, df: DataFrame) -> DataFrame:
        """
        Route each row to its provider-specific normalisation function,
        then union back into one canonical DataFrame.
        """
        providers = ["VISA", "MASTERCARD", "MPESA", "PESAPAL", "FLUTTERWAVE"]
        normalised = []

        for provider in providers:
            provider_df = df.filter(col("provider") == provider)
            method = getattr(self, f"_normalize_{provider.lower()}", None)
            if method:
                normalised.append(method(provider_df))

        from functools import reduce
        return reduce(lambda a, b: a.union(b), normalised)

    #Visa

    def _normalize_visa(self, df: DataFrame) -> DataFrame:
        return df.select(
            col("value.transaction_id").alias("transaction_id"),
            lit("VISA").alias("provider"),
            coalesce(col("value.source_type"), lit("ISO_8583")).alias("source_type"),
            self._parse_timestamp(df, "value.timestamp", "value.event_time_ms"),
            col("value.event_time_ms").cast(LongType()).alias("event_time_ms"),
            col("value.card_token").alias("card_token"),
            col("value.bin").alias("bin"),
            col("value.last4").alias("last4"),
            col("value.amount").cast(DoubleType()).alias("amount"),
            col("value.currency").alias("currency"),
            col("value.currency_code").alias("currency_code"),
            col("value.merchant_id").alias("merchant_id"),
            col("value.merchant_name").alias("merchant_name"),
            col("value.mcc").alias("mcc"),
            col("value.terminal_id").alias("terminal_id"),
            col("value.transaction_type").alias("transaction_type"),
            col("value.pos_entry_mode").alias("pos_entry_mode"),
            lit("POS").alias("channel"),
            col("value.card_token").alias("customer_id"),
            lit(None).cast(StringType()).alias("device_fingerprint"),
            lit("CARD").alias("payment_method"),
            lit(None).cast(StringType()).alias("auth_model"),
            lit("APPROVED").alias("status"),
            col("value.acquirer_id").alias("acquirer_id"),
            col("value.stan").alias("stan"),
            col("value.is_fraud").alias("is_fraud"),
            col("value.risk_score").cast(DoubleType()).alias("risk_score"),
            col("value.fraud_pattern").alias("fraud_pattern"),
        )

    # Mastercard

    def _normalize_mastercard(self, df: DataFrame) -> DataFrame:
        # Same ISO 8583 structure as Visa
        return self._normalize_visa(
            df.withColumn("provider", lit("MASTERCARD"))
        ).withColumn("provider", lit("MASTERCARD"))

    # M-Pesa

    def _normalize_mpesa(self, df: DataFrame) -> DataFrame:
        return df.select(
            col("value.transaction_id").alias("transaction_id"),
            lit("MPESA").alias("provider"),
            lit("DARAJA_WEBHOOK").alias("source_type"),
            self._parse_timestamp(df, "value.timestamp", "value.event_time_ms"),
            col("value.event_time_ms").cast(LongType()).alias("event_time_ms"),
            col("value.msisdn_token").alias("card_token"),
            lit("639301").alias("bin"),          # Synthetic M-Pesa BIN
            lit(None).cast(StringType()).alias("last4"),
            col("value.amount").cast(DoubleType()).alias("amount"),
            lit("KES").alias("currency"),
            lit("404").alias("currency_code"),
            coalesce(
                col("value.merchant_id"),
                col("value.business_short_code").cast(StringType())
            ).alias("merchant_id"),
            lit(None).cast(StringType()).alias("merchant_name"),
            lit("4814").alias("mcc"),            # Telecom default
            lit(None).cast(StringType()).alias("terminal_id"),
            col("value.transaction_type").alias("transaction_type"),
            lit(None).cast(StringType()).alias("pos_entry_mode"),
            lit("MOBILE_APP").alias("channel"),
            col("value.msisdn_token").alias("customer_id"),
            lit(None).cast(StringType()).alias("device_fingerprint"),
            lit("MOBILE_MONEY").alias("payment_method"),
            lit(None).cast(StringType()).alias("auth_model"),
            lit("APPROVED").alias("status"),
            lit(None).cast(StringType()).alias("acquirer_id"),
            col("value.mpesa_receipt_number").alias("stan"),
            col("value.is_fraud").alias("is_fraud"),
            col("value.risk_score").cast(DoubleType()).alias("risk_score"),
            col("value.fraud_pattern").alias("fraud_pattern"),
        )

    #flutterwave
    def _normalize_flutterwave(self, df: DataFrame) -> DataFrame:
        return df.select(
            col("value.transaction_id").alias("transaction_id"),
            lit("FLUTTERWAVE").alias("provider"),
            lit("WEBHOOK").alias("source_type"),
            self._parse_timestamp(df, "value.timestamp", "value.event_time_ms"),
            col("value.event_time_ms").cast(LongType()).alias("event_time_ms"),
            col("value.card_token").alias("card_token"),
            col("value.bin").alias("bin"),
            col("value.last4").alias("last4"),
            col("value.amount").cast(DoubleType()).alias("amount"),
            col("value.currency").alias("currency"),
            col("value.currency_code").alias("currency_code"),
            coalesce(col("value.merchant_id"), lit("UNKNOWN")).alias("merchant_id"),
            lit(None).cast(StringType()).alias("merchant_name"),
            coalesce(col("value.mcc"), lit("5999")).alias("mcc"),
            lit(None).cast(StringType()).alias("terminal_id"),
            lit("PURCHASE").alias("transaction_type"),
            lit("01").alias("pos_entry_mode"),   # CNP
            lit("WEB").alias("channel"),
            col("value.card_token").alias("customer_id"),
            col("value.device_fingerprint").alias("device_fingerprint"),
            lit("CARD").alias("payment_method"),
            col("value.auth_model").alias("auth_model"),
            col("value.status").alias("status"),
            lit(None).cast(StringType()).alias("acquirer_id"),
            col("value.transaction_id").alias("stan"),
            col("value.is_fraud").alias("is_fraud"),
            col("value.risk_score").cast(DoubleType()).alias("risk_score"),
            col("value.fraud_pattern").alias("fraud_pattern"),
        )

    # pesapal


    def _normalize_pesapal(self, df: DataFrame) -> DataFrame:
        return df.select(
            col("value.transaction_id").alias("transaction_id"),
            lit("PESAPAL").alias("provider"),
            lit("IPN_CALLBACK").alias("source_type"),
            self._parse_timestamp(df, "value.timestamp", "value.event_time_ms"),
            col("value.event_time_ms").cast(LongType()).alias("event_time_ms"),
            col("value.card_token").alias("card_token"),
            coalesce(col("value.bin"), lit("000000")).alias("bin"),
            lit(None).cast(StringType()).alias("last4"),
            col("value.amount").cast(DoubleType()).alias("amount"),
            col("value.currency").alias("currency"),
            col("value.currency_code").alias("currency_code"),
            coalesce(
                col("value.merchant_id"),
                col("value.pesapal_merchant_reference")
            ).alias("merchant_id"),
            lit(None).cast(StringType()).alias("merchant_name"),
            coalesce(col("value.mcc"), lit("5999")).alias("mcc"),
            lit(None).cast(StringType()).alias("terminal_id"),
            lit("PURCHASE").alias("transaction_type"),
            lit("01").alias("pos_entry_mode"),   # CNP
            lit("WEB").alias("channel"),
            col("value.card_token").alias("customer_id"),
            lit(None).cast(StringType()).alias("device_fingerprint"),
            lit("CARD").alias("payment_method"),
            lit(None).cast(StringType()).alias("auth_model"),
            col("value.payment_status").alias("status"),
            lit(None).cast(StringType()).alias("acquirer_id"),
            col("value.transaction_id").alias("stan"),
            col("value.is_fraud").alias("is_fraud"),
            col("value.risk_score").cast(DoubleType()).alias("risk_score"),
            col("value.fraud_pattern").alias("fraud_pattern"),
        )

    # Time Helper
    @staticmethod
    def _parse_timestamp(df, ts_col: str, ms_col: str):
        """
        Parse timestamp from ISO string or epoch ms — whichever is available.
        Returns a TimestampType column named 'event_timestamp'.
        """
        return coalesce(
            to_timestamp(col(ts_col)),
            (col(ms_col).cast(DoubleType()) / 1000).cast(TimestampType()),
        ).alias("event_timestamp")
