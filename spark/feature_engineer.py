import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
col, count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    stddev, when, lit, hour, dayofweek, dayofmonth, month, year,
    unix_timestamp, current_timestamp, lag, window,
    coalesce, abs as spark_abs, expr,
)
from pyspark.sql.types import DoubleType, LongType, IntegerType
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)

class FeatureEngineer:
    def compute_velocity_features(self,df: DataFrame) -> DataFrame:
        """
        Velocity features: counts and amounts over time windows.
        """

        #window specs for different lookback periods
        win_1min = window(col("event_timestamp"), "1 minute")
        win_5min = window(col("event_timestamp"), "5 minutes")
        win_1hr = window(col("event_timestamp"), "1 hour")

        #Card level velocity
        #Count and sum of transactions per card token per window
        card_velocity_1min=(
            df.groupBy(col("card_token"),win_1min)
            .agg(
                count("*").alias("card_txn_count_1min"),
                spark_sum("amount").alias("card_txn_sum_1min"),
            )
        )
        card_velocity_5min = (
            df.groupBy(col("card_token"), win_5min)
            .agg(
                count("*").alias("card_txn_count_5min"),
                spark_sum("amount").alias("card_amount_sum_5min"),
            )
        )
        card_velocity_1hr = (
            df.groupBy(col("card_token"), win_1hr)
            .agg(
                count("*").alias("card_txn_count_1hr"),
                spark_sum("amount").alias("card_amount_sum_1hr"),
                avg("amount").alias("card_amount_avg_1hr"),
                spark_max("amount").alias("card_amount_max_1hr"),
            )
        )

        #Merchant-level velocity
        merchant_velocity_1hr = (
            df.groupBy(col("merchant_id"), win_1hr)
            .agg(
                count("*").alias("merchant_txn_count_1hr"),
                spark_sum("amount").alias("merchant_amount_sum_1hr"),
            )
        )

        #Join velocity back to main Dataframe
        df = df.join(
            card_velocity_1min,
            on=["card_token"],
            how="left",
        ).join(
            card_velocity_5min,
            on=["card_token"],
            how="left",
        ).join(
            card_velocity_1hr,
            on=["card_token"],
            how="left",
        ).join(
            merchant_velocity_1hr,
            on=["merchant_id"],
            how="left",
        )

        #Drop the window struct columns
        df=df.drop("window")

        #fill null velcoity with 1
        df = df.fillna({
            "card_txn_count_1min": 1,
            "card_txn_count_5min": 1,
            "card_txn_count_1hr": 1,
            "merchant_txn_count_1hr": 1,
        })

        return df
    def compute_temporal_features(self,df: DataFrame) -> DataFrame:
        df = df.withColumn("txn_hour", hour(col("event_timestamp")))
        df = df.withColumn("txn_day_of_week", dayofweek(col("event_timestamp")))
        df = df.withColumn("txn_day_of_month", dayofmonth(col("event_timestamp")))
        df = df.withColumn("txn_month", month(col("event_timestamp")))

        # Business hours: 8am–8pm local (using UTC as proxy)
        df = df.withColumn(
            "is_business_hours",
            col("txn_hour").between(8, 20),
        )

        # Late night: highest fraud rate window (1am–4am)
        df = df.withColumn(
            "is_late_night",
            col("txn_hour").between(1, 4),
        )

        # Weekend
        df = df.withColumn(
            "is_weekend",
            col("txn_day_of_week").isin(1, 7),  # 1=Sunday, 7=Saturday in Spark
        )

        # Month-end (last 3 days) — elevated fraud pattern
        df = df.withColumn(
            "is_month_end",
            col("txn_day_of_month") >= 28,
        )

        # Unix timestamp for time-delta calculations downstream
        df = df.withColumn(
            "event_unix_ts",
            unix_timestamp(col("event_timestamp")).cast(LongType()),
        )

        return df

    def compute_location_features(self, df: DataFrame) -> DataFrame:
        """
        Location and cross-border features.
        Uses fields populated by broadcast joins (bin_country, currency_region).
        """
        # is_international: card issuer country ≠ transaction currency region
        df = df.withColumn(
            "is_international",
            when(
                col("bin_country").isNotNull() & col("currency_region").isNotNull(),
                when(col("bin_country").isin(
                    "KE", "UG", "TZ", "RW", "ZA", "NG", "GH"
                ), col("currency_region") != "AFRICA")
                .otherwise(col("currency_region") == "AFRICA"),
            ).otherwise(lit(False)),
        )

        # is_cross_border_currency
        df = df.withColumn(
            "is_cross_border_currency",
            when(
                col("bin_country").isin("KE", "UG", "TZ") &
                col("currency").isin("USD", "GBP", "EUR"),
                lit(True),
            ).when(
                col("bin_country").isin("US", "GB") &
                col("currency").isin("KES", "NGN", "ZAR"),
                lit(True),
            ).otherwise(lit(False)),
        )

        return df

    def compute_behavioral_features(self, df: DataFrame) -> DataFrame:
        """
        Behavioural features: amount deviation, transaction patterns.
        """
        # Amount relative to card average (requires card_amount_avg_1hr from velocity)
        df = df.withColumn(
            "amount_vs_avg_ratio",
            when(
                col("card_amount_avg_1hr").isNotNull() &
                (col("card_amount_avg_1hr") > 0),
                col("amount") / col("card_amount_avg_1hr"),
            ).otherwise(lit(1.0).cast(DoubleType())),
        )

        # Large amount spike: current txn > 3x average
        df = df.withColumn(
            "is_amount_spike",
            col("amount_vs_avg_ratio") > 3.0,
        )

        # Rapid transactions: more than 3 in 1 minute
        df = df.withColumn(
            "is_rapid_succession",
            coalesce(col("card_txn_count_1min"), lit(1)) > 3,
        )

        # High velocity: more than 10 transactions per hour
        df = df.withColumn(
            "is_high_velocity",
            coalesce(col("card_txn_count_1hr"), lit(1)) > 10,
        )

        # Round amount (e.g. exactly 1000, 5000) — fraud pattern
        df = df.withColumn(
            "is_round_amount",
            (col("amount") % 1000 == 0) & (col("amount") >= 1000),
        )

        # Micro transaction (< $1 USD) — often used to test stolen cards
        df = df.withColumn(
            "is_micro_transaction",
            coalesce(col("amount_usd"), col("amount")).cast(DoubleType()) < 1.0,
        )

        return df

    def compute_network_features(self, df: DataFrame) -> DataFrame:
        """
        Network features: device and terminal sharing patterns.
        Full cross-batch device history is handled by RedisStateStore.
        Intra-batch patterns are computed here.
        """
        # Card-not-present detection
        df = df.withColumn(
            "is_card_not_present",
            when(
                col("pos_entry_mode").isin("01", "10", "00") |
                col("auth_model").isin("NOAUTH", "VBV", "3DS") |
                col("channel").isin("WEB", "MOBILE_APP"),
                lit(True),
            ).otherwise(lit(False)),
        )

        # Chip transaction — lower risk
        df = df.withColumn(
            "is_chip_transaction",
            col("pos_entry_mode").isin("051", "071", "091"),
        )

        # Has device fingerprint
        df = df.withColumn(
            "has_device_fingerprint",
            col("device_fingerprint").isNotNull(),
        )

        return df

    def compute_merchant_features(self, df: DataFrame) -> DataFrame:
        """
        Merchant-level features.
        Uses is_high_risk_mcc from BIN broadcast join and velocity from above.
        """
        # High-volume merchant in this batch (proxy for popular legitimate merchant)
        df = df.withColumn(
            "is_high_volume_merchant",
            coalesce(col("merchant_txn_count_1hr"), lit(1)) > 100,
        )

        # Prepaid card at high-risk MCC — elevated fraud signal
        df = df.withColumn(
            "prepaid_at_high_risk_mcc",
            coalesce(col("bin_prepaid"), lit(False)) &
            coalesce(col("is_high_risk_mcc"), lit(False)),
        )

        # High value at ATM MCC
        df = df.withColumn(
            "is_high_value_atm",
            (col("mcc") == "6011") &
            coalesce(col("amount_usd"), col("amount")).cast(DoubleType()) > 500,
        )

        return df

    def compute_composite_risk_score(self, df: DataFrame) -> DataFrame:
        """
        Lightweight rule-based composite risk score (0–100).
        This is a pre-ML heuristic — the actual score comes from the ML model.
        Used for fast-path decisions and as a feature itself.
        """
        df = df.withColumn(
            "composite_risk_score",
            (
                    when(col("is_high_risk_mcc"), lit(20)).otherwise(lit(0)) +
                    when(col("is_international"), lit(15)).otherwise(lit(0)) +
                    when(col("is_late_night"), lit(10)).otherwise(lit(0)) +
                    when(col("is_amount_spike"), lit(20)).otherwise(lit(0)) +
                    when(col("is_rapid_succession"), lit(25)).otherwise(lit(0)) +
                    when(col("is_card_not_present"), lit(10)).otherwise(lit(0)) +
                    when(col("is_cross_border_currency"), lit(10)).otherwise(lit(0)) +
                    when(col("is_high_velocity"), lit(15)).otherwise(lit(0)) +
                    when(col("prepaid_at_high_risk_mcc"), lit(20)).otherwise(lit(0)) +
                    when(col("is_micro_transaction"), lit(15)).otherwise(lit(0))
            ).cast(IntegerType()),
        )

        # Cap at 100
        df = df.withColumn(
            "composite_risk_score",
            when(col("composite_risk_score") > 100, lit(100))
            .otherwise(col("composite_risk_score")),
        )

        return df