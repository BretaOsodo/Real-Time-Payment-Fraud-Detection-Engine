import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, count, avg,
    when, coalesce,
)
from pyspark.sql.types import BooleanType, StringType

logger = logging.getLogger(__name__)


#Redis state store
class RedisStateStore:
    """
        Redis-backed state store for cross-batch lookups that Spark's
        windowed aggregations can't provide efficiently.

        Lookups:
            - Device fingerprint history (has this device been seen before?)
            - Cross-batch velocity (card transaction count from last N batches)
            - Merchant first-seen flag

        Uses Spark's mapPartitions to batch Redis calls — one connection
        per partition rather than one connection per row.
        """

    def __init__(self):
        self._redis_host = "localhost"
        self._redis_port = 6379
        self._redis_password = None
        self._redis_ssl = False

    def initialize(
            self,
            host: str = "localhost",
            port: int = 6379,
            password: Optional[str] = None,
            ssl: bool = False,
    ):
        self._redis_host = host
        self._redis_port = port
        self._redis_password = password
        self._redis_ssl = ssl
        logger.info(f"RedisStateStore configured: {host}:{port}")

    def enrich_device_history(self,df: DataFrame) -> DataFrame:
        """
                Add device_seen_before flag by looking up device fingerprint
                in Redis per partition. Uses mapPartitions for connection efficiency.
                """
        redis_host = self._redis_host
        redis_port = self._redis_port
        redis_password = self._redis_password
        redis_ssl = self._redis_ssl

        def lookup_partition(rows):
            import redis as redis_lib
            try:
                r = redis_lib.Redis(
                    host=redis_host,
                    port=redis_port,
                    password=redis_password,
                    ssl=redis_ssl,
                    socket_timeout=0.005,
                    decode_responses=True,
                )
            except Exception:
                # Redis unavailable — yield all rows with None flag
                for row in rows:
                    yield (*row, None)
                return

            for row in rows:
                device_fp = row.device_fingerprint
                customer_id = row.customer_id
                seen = None

                if device_fp and customer_id:
                    try:
                        key = f"device:seen:{customer_id}:{device_fp}"
                        seen = r.exists(key) == 1
                        if not seen:
                            r.setex(key, 7776000, "1")  # 90-day TTL
                    except Exception:
                        seen = None

                yield (*row, seen)

        from pyspark.sql.types import StructType, StructField, BooleanType as BT
        new_schema = df.schema.add(
            "device_seen_before", BT(), True
        )
        return df.rdd.mapPartitions(lookup_partition).toDF(new_schema)

    def close(self):
        logger.info("RedisStateStore: no persistent connections to close")

#Metrics Collector

class MetricsCollector:
    """
    Collects per-micro-batch metrics and exposes them via Prometheus.

    Metrics tracked:
        - fraud_pipeline_records_processed_total (by provider)
        - fraud_pipeline_enrichment_latency_seconds (histogram)
        - fraud_pipeline_high_risk_transactions_total
        - fraud_pipeline_batch_size (gauge)
    """

    def __init__(self):
        self._start_time = time.time()
        self._setup_prometheus()

    def _setup_prometheus(self):
        try:
            from prometheus_client import Counter, Histogram, Gauge, start_http_server
            self._records_total = Counter(
                "fraud_pipeline_records_processed_total",
                "Total records processed by the enrichment pipeline",
                ["provider"],
            )
            self._latency = Histogram(
                "fraud_pipeline_enrichment_latency_seconds",
                "End-to-end enrichment latency",
                ["provider"],
                buckets=[0.05, 0.1, 0.2, 0.5, 1.0, 2.0],
            )
            self._high_risk = Counter(
                "fraud_pipeline_high_risk_total",
                "Transactions with composite_risk_score > 50",
                ["provider"],
            )
            self._batch_size = Gauge(
                "fraud_pipeline_batch_size",
                "Number of records in the last micro-batch",
            )
            logger.info("Prometheus metrics initialised")
        except ImportError:
            logger.warning("prometheus_client not installed — metrics disabled")
            self._records_total = None

    def add_pipeline_metrics(self, df: DataFrame) -> DataFrame:
        """Add enrichment_timestamp for latency tracking."""
        return df.withColumn("pipeline_enrichment_ts", current_timestamp())

    def write_to_prometheus(self, df: DataFrame):
        """
        Write batch metrics to Prometheus via foreachBatch.
        Called alongside the main Kafka write query.
        """
        metrics = self

        def update_metrics(batch_df: DataFrame, epoch_id: int):
            if metrics._records_total is None:
                return
            try:
                batch_df.cache()
                total = batch_df.count()
                metrics._batch_size.set(total)

                # Per-provider counts
                provider_counts = (
                    batch_df.groupBy("provider")
                    .agg(count("*").alias("cnt"))
                    .collect()
                )
                for row in provider_counts:
                    metrics._records_total.labels(
                        provider=row["provider"]
                    ).inc(row["cnt"])

                # High-risk count
                high_risk_counts = (
                    batch_df.filter(
                        coalesce(col("composite_risk_score"), lit(0)) > 50
                    )
                    .groupBy("provider")
                    .agg(count("*").alias("cnt"))
                    .collect()
                )
                for row in high_risk_counts:
                    metrics._high_risk.labels(
                        provider=row["provider"]
                    ).inc(row["cnt"])

                batch_df.unpersist()
            except Exception as exc:
                logger.error(f"Metrics update failed: {exc}")

        return df.writeStream \
            .foreachBatch(update_metrics) \
            .outputMode("append") \
            .trigger(processingTime="10 seconds") \
            .option("checkpointLocation", "/checkpoints/metrics") \
            .start()

# Dead Letter Queue

class DeadLetterQueue:
    """
    Routes failed or unparseable records to the enrichment DLQ Kafka topic.
    Used in foreachBatch when a record cannot be enriched.
    """

    def __init__(self, dlq_topic: str = "enrichment_dlq"):
        self._dlq_topic = dlq_topic

    def route_failures(
            self,
            batch_df: DataFrame,
            bootstrap_servers: str,
            error_reason: str = "ENRICHMENT_FAILURE",
    ):
        """
        Write failed records from a batch to the DLQ topic.
        Call from foreachBatch when a filter condition identifies bad records.
        """
        try:
            from pyspark.sql.functions import struct, to_json

            dlq_df = batch_df.select(
                to_json(struct(
                    col("transaction_id"),
                    col("provider"),
                    lit(error_reason).alias("error_reason"),
                    lit(datetime.now(timezone.utc).isoformat()).alias("failed_at"),
                    col("event_timestamp").cast(StringType()),
                )).alias("value")
            )

            dlq_df.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", bootstrap_servers) \
                .option("topic", self._dlq_topic) \
                .option("kafka.acks", "all") \
                .mode("append") \
                .save()

            logger.warning(
                f"DLQ: wrote {dlq_df.count()} failed records "
                f"(reason: {error_reason})"
            )
        except Exception as exc:
            logger.error(f"DLQ write failed: {exc}")
