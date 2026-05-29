"""
Production-Grade Spark Structured Streaming Enrichment Pipeline

Architecture:
    Kafka (*_tokenised) → Spark Streaming → Enrichment → Kafka (*_enriched)

Key design principles:
    1. One Spark app for all providers (no duplication)
    2. Broadcast joins for reference data (no per-event Redis)
    3. Stateful aggregations for velocity features
    4. Exactly-once semantics with checkpointing
    5. Dynamic topic routing based on provider

Performance targets:
    - 100,000+ TPS
    - <200ms p99 end-to-end latency
    - 99.99% availability
"""
from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.avro.functions import from_avro,to_avro
import json
import logging
import time
from typing import Dict, Tuple, List

from schema_normalizer import SchemaNormalizer
from feature_engineer import FeatureEngineer
from broadcast_joins import BroadcastJoinManager
from redis_state_store import RedisStateStore
from metrics import MetricsCollector
from dead_letter_queue import DeadLetterQueue
from config.spark_config import SparkConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FraudEnrichmentPipeline:
    """
        Main enrichment pipeline for fraud detection

        Consumes from 5 tokenised topics:
            - visa_tokenised
            - mastercard_tokenised
            - mpesa_tokenised
            - pesapal_tokenised
            - flutterwave_tokenised

        Produces to 5 enriched topics:
            - visa_enriched
            - mastercard_enriched
            - mpesa_enriched
            - pesapal_enriched
            - flutterwave_enriched
        """
    def __init__(self):
        self.config=SparkConf()
        self.spark = None
        self.normalizer=SchemaNormalizer()
        self.feature_engineer = FeatureEngineer()
        self.broadcast_joins = BroadcastJoinManager()
        self.redis_store = RedisStateStore()
        self.metrics = MetricsCollector()
        self.dlq = DeadLetterQueue()

        #provide topic mapping
        self.input_topics={
            "VISA": "visa_tokenised",
            "MASTERCARD": "mastercard_tokenised",
            "MPESA": "mpesa_tokenised",
            "PESAPAL": "pesapal_tokenised",
            "FLUTTERWAVE": "flutter_tokenised"
        }

        self.output_topics = {
            "VISA": "visa_enriched",
            "MASTERCARD": "mastercard_enriched",
            "MPESA": "mpesa_enriched",
            "PESAPAL": "pesapal_enriched",
            "FLUTTERWAVE": "flutterwave_enriched"
        }

    def create_spark_session(self) -> SparkSession:
        "Create Optimized spark session for high-throughput streaming"

        return SparkSession.builder \
            .appName("FraudEnrichmentPipeline") \
            .config("spark.sql.streaming.checkpointLocation", "/checkpoints") \
            .config("spark.sql.streaming.stateStore.maintenanceInterval", "5") \
            .config("spark.sql.streaming.stateStore.minDeltasForSnapshot", "5") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "false") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.autoBroadcastJoinThreshold", "104857600") \
            .config("spark.sql.broadcastTimeout", "300") \
            .config("spark.streaming.backpressure.enabled", "true") \
            .config("spark.streaming.backpressure.initialRate", "50000") \
            .config("spark.streaming.kafka.maxRatePerPartition", "10000") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.kryo.registrationRequired", "false") \
            .config("spark.kryo.unsafe", "true") \
            .config("spark.network.timeout", "800s") \
            .config("spark.executor.heartbeatInterval", "60s") \
            .config("spark.sql.streaming.streamingQueryListener.enabled", "true") \
            .config("spark.sql.streaming.metricsEnabled", "true") \
            .getOrCreate()

    def read_from_kafka(self) -> DataFrame:
        """
               Read from all tokenised topics dynamically

               Uses regex pattern to consume all topics in one stream
        """

        topic_list=list(self.input_topics.values())
        topic_pattern=",".join(topic_list)

        raw_stream = self.spark.readStream.format("kafka")\
            .option("kafka.bootstrap.servers", self.config.KAFKA_BOOTSTRAP_SERVERS) \
            .option("kafka.group.id", "fraud-enrichment-group") \
            .option("subscribe", topic_pattern) \
            .option("startingOffsets", "latest") \
            .option("maxOffsetsPerTrigger", "50000") \
            .option("minPartitions", "200") \
            .option("failOnDataLoss", "false") \
            .option("kafka.consumer.commit.interval.ms", "5000") \
            .option("kafka.consumer.heartbeat.interval.ms", "3000") \
            .option("kafka.consumer.session.timeout.ms", "30000") \
            .option("kafka.consumer.max.poll.records", "10000") \
            .option("kafka.consumer.fetch.max.bytes", "104857600") \
            .option("kafka.consumer.max.partition.fetch.bytes", "1048576") \
            .load()

        #Extract topic to determine provider
        raw_stream=raw_stream.withColumn('topic',col('topic'))

        #Parse Avro based on topic
        parsed_stream = self._parse_avro_by_topic(raw_stream)

        return parsed_stream

    def _parse_avro_by_topic(self, df: DataFrame) -> DataFrame:
        """Parse Avro Messages using provider-specific schemas"""

        parsed_dfs=[]

        for provider, topic in self.input_topics.items():
            #filter for this provider's topic
            provider_df=df.filter(col('topic') == topic)

            #get schema for this provider
            schema_path=f"/avro/{provider.lower()}_tokenised.avsc"

            #parse Avro
            parsed= provider_df\
                .withColumn("value",from_avro(col("value"),schema_path)) \
                .withColumn("provider",lit(provider))

            parsed_dfs.append(parsed)


        #union all providers
        if len(parsed_dfs)>1:
            return parsed_dfs[0].union(*parsed_dfs[1:])

        return parsed_dfs[0]

    def apply_enrichment(self,df: DataFrame) -> DataFrame:
        """
                Apply all enrichment layers

                Order matters for feature dependencies:
                1. Schema normalization (unified format)
                2. Broadcast joins (BIN, MCC, FX, merchants)
                3. Derived risk signals
                4. Stateful velocity features
                5. Device intelligence (Redis)
                6. Location intelligence
                7. Temporal features
                8. Behavioral features
                9. Network features
        """

        #Adding watermark for late event handling
        df=df.withWatermark("event_timestamp","60 seconds")

        #Normalize the canonical schema
        df=self.normalizer.normalize(df)

        #Broadcast joins
        df=self.broadcast_joins.apply_all_joins(df)

        #Derived risk signals
        df=self._add_derived_signals(df)

