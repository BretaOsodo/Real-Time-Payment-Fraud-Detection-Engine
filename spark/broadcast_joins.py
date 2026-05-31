import json
import logging
from typing import Optional

import redis
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import col, broadcast, when, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType,
)

logger = logging.getLogger(__name__)

_MCC_DATA = [
    ("5311", "Department Stores",            "RETAIL",        False),
    ("5411", "Grocery Stores",               "GROCERY",       False),
    ("5812", "Restaurants",                  "FOOD_BEVERAGE", False),
    ("5814", "Fast Food Restaurants",        "FOOD_BEVERAGE", False),
    ("5541", "Service Stations",             "FUEL",          False),
    ("5732", "Electronics Stores",           "ELECTRONICS",   False),
    ("5912", "Drug Stores / Pharmacies",     "HEALTH",        False),
    ("7011", "Hotels / Lodging",             "TRAVEL",        False),
    ("4111", "Transport / Commuter",         "TRANSPORT",     False),
    ("4814", "Telecom Services",             "TELECOM",       False),
    ("6011", "ATM / Cash Dispensing",        "ATM",           True),
    ("6012", "Financial Institutions",       "FINANCIAL",     False),
    ("7995", "Gambling",                     "GAMBLING",      True),
    ("4829", "Wire Transfer",                "FINANCIAL",     True),
    ("5933", "Pawn Shops",                   "RETAIL",        True),
    ("5999", "Miscellaneous Retail",         "OTHER",         False),
    ("0000", "Unknown",                      "UNKNOWN",       False),
]
_MCC_SCHEMA = StructType([
    StructField("mcc",             StringType(),  False),
    StructField("mcc_description", StringType(),  True),
    StructField("mcc_category",    StringType(),  True),
    StructField("is_high_risk_mcc",BooleanType(), True),
])

_CURRENCY_DATA = [
    ("840", "USD", "US Dollar",             "NORTH_AMERICA"),
    ("826", "GBP", "British Pound",         "EUROPE"),
    ("978", "EUR", "Euro",                  "EUROPE"),
    ("404", "KES", "Kenyan Shilling",       "AFRICA"),
    ("710", "ZAR", "South African Rand",    "AFRICA"),
    ("566", "NGN", "Nigerian Naira",        "AFRICA"),
    ("800", "UGX", "Ugandan Shilling",      "AFRICA"),
    ("834", "TZS", "Tanzanian Shilling",    "AFRICA"),
    ("646", "RWF", "Rwandan Franc",         "AFRICA"),
    ("936", "GHS", "Ghanaian Cedi",         "AFRICA"),
    ("784", "AED", "UAE Dirham",            "MIDDLE_EAST"),
    ("356", "INR", "Indian Rupee",          "ASIA"),
    ("156", "CNY", "Chinese Yuan",          "ASIA"),
    ("124", "CAD", "Canadian Dollar",       "NORTH_AMERICA"),
]
_CURRENCY_SCHEMA = StructType([
    StructField("currency_code",   StringType(), False),
    StructField("currency_alpha",  StringType(), True),
    StructField("currency_name",   StringType(), True),
    StructField("currency_region", StringType(), True),
])

_FX_DATA = [
    ("USD", 1.0),
    ("GBP", 1.27),
    ("EUR", 1.09),
    ("KES", 0.0077),
    ("ZAR", 0.055),
    ("NGN", 0.00065),
    ("UGX", 0.00027),
    ("TZS", 0.00038),
    ("RWF", 0.00073),
    ("GHS", 0.083),
    ("AED", 0.272),
    ("INR", 0.012),
    ("CNY", 0.138),
    ("CAD", 0.74),
]
_FX_SCHEMA = StructType([
    StructField("currency_alpha", StringType(),  False),
    StructField("fx_rate_to_usd", DoubleType(),  True),
])

_BIN_DATA = [
    ("639301", "VISA",       "PREPAID",  "KE", "Safaricom M-Pesa", True),
    ("411111", "VISA",       "CREDIT",   "US", "Test Bank",        False),
    ("512345", "MASTERCARD", "DEBIT",    "KE", "Equity Bank",      False),
    ("498765", "VISA",       "DEBIT",    "KE", "KCB Bank",         False),
]

_BIN_SCHEMA = StructType([
    StructField("bin",            StringType(),  False),
    StructField("bin_brand",      StringType(),  True),
    StructField("bin_card_type",  StringType(),  True),
    StructField("bin_country",    StringType(),  True),
    StructField("bin_issuer",     StringType(),  True),
    StructField("bin_prepaid",    BooleanType(), True),
])

class BroadcastJoinManager:
    def __init__(self):
        self._spark = None
        self._mcc_df = None
        self._currency_df = None
        self._fx_df = None
        self._bin_df = None

    def initialize(self,
                   spark: SparkSession,
                   redis_client:Optional[redis.Redis] = None,
    ):
        self._spark = spark
        self._load_mcc_table()
        self._load_currency_table()
        self._load_fx_table(redis_client)
        self._load_bin_table()
        logger.info("All broadcast reference tables loaded")

    #Table Loaders
    def _load_mcc_table(self):
        self._mcc_df = self._spark.createDataFrame(_MCC_DATA, schema=_MCC_SCHEMA)
        logger.info(f"MCC table: {self._mcc_df.count()} rows")

    def _load_currency_table(self):
        self._currency_df = self._spark.createDataFrame(_CURRENCY_DATA, schema=_CURRENCY_SCHEMA)
        logger.info(f"Currency table: {self._currency_df.count()} rows")

    def _load_fx_table(self, redis_client: Optional[redis.Redis] = None):
        """
        Load FX rates. Prefers live rates from Redis (key: fx:USD:<CURRENCY>).
        Falls back to hardcoded rates on Redis failure.
        """
        fx_data = list(_FX_DATA)  # start with fallbacks

        if redis_client:
            try:
                currencies = [row[0] for row in _FX_DATA]
                for currency in currencies:
                    rate_raw = redis_client.get(f"fx:USD:{currency}")
                    if rate_raw:
                        live_rate = float(rate_raw)
                        # Replace the fallback rate with the live rate
                        fx_data = [
                            (c, live_rate if c == currency else r)
                            for c, r in fx_data
                        ]
                logger.info("FX rates loaded from Redis")
            except Exception as exc:
                logger.warning(f"Redis FX load failed: {exc} — using fallback rates")

        self._fx_df = self._spark.createDataFrame(fx_data, schema=_FX_SCHEMA)
        logger.info(f"FX table: {self._fx_df.count()} rows")

    def _load_bin_table(self):
        """
        Load BIN reference table.
        In production, replace _BIN_DATA with a Delta Lake read of the full BIN table.
        """
        self._bin_df = self._spark.createDataFrame(_BIN_DATA, schema=_BIN_SCHEMA)
        logger.info(f"BIN table: {self._bin_df.count()} rows")

        # ── Join application ──────────────────────────────────────────────────────

    def apply_all_joins(self, df: DataFrame) -> DataFrame:
        """Apply all broadcast joins to a streaming DataFrame."""
        df = self._join_mcc(df)
        df = self._join_currency(df)
        df = self._join_fx(df)
        df = self._join_bin(df)
        return df

    def _join_mcc(self, df: DataFrame) -> DataFrame:
        """Attach MCC description and risk flag."""
        return df.join(
            broadcast(self._mcc_df),
            on=df["mcc"] == self._mcc_df["mcc"],
            how="left",
        ).drop(self._mcc_df["mcc"])

    def _join_currency(self, df: DataFrame) -> DataFrame:
        """Attach currency name and region via currency code."""
        return df.join(
            broadcast(self._currency_df),
            on=df["currency_code"] == self._currency_df["currency_code"],
            how="left",
        ).drop(self._currency_df["currency_code"])

    def _join_fx(self, df: DataFrame) -> DataFrame:
        """
        Attach FX rate and compute amount_usd.
        Join on currency alpha code (e.g. KES, USD).
        """
        df = df.join(
            broadcast(self._fx_df),
            on=df["currency"] == self._fx_df["currency_alpha"],
            how="left",
        ).drop(self._fx_df["currency_alpha"])

        df = df.withColumn(
            "amount_usd",
            when(
                col("fx_rate_to_usd").isNotNull() & col("amount").isNotNull(),
                (col("amount") * col("fx_rate_to_usd")).cast(DoubleType()),
            ).otherwise(lit(None).cast(DoubleType())),
        )
        return df

    def _join_bin(self, df: DataFrame) -> DataFrame:
        """Attach BIN-level card metadata."""
        return df.join(
            broadcast(self._bin_df),
            on=df["bin"] == self._bin_df["bin"],
            how="left",
        ).drop(self._bin_df["bin"])

    def refresh_fx(self, redis_client: Optional[redis.Redis] = None):
        """
        Reload FX rates — call from Airflow every 5 minutes.
        Safe to call while the pipeline is running.
        """
        self._load_fx_table(redis_client)
        logger.info("FX rates refreshed")
