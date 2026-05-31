"""
Spark pipeline configurations.
"""

import os
from dataclasses import dataclass,field
from typing import Optional

@dataclass
class SparkConfig:

    #Kafka configurations
    KAFKA_BOOTSTRAP_SERVERS:str=os.environ.get(
        "KAFKA_BOOTSTRAP_SERVERS","localhost:9092"
    )
    KAFKA_SECURITY_PROTOCOL: str = os.environ.get(
        "KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"
    )
    KAFKA_SASL_MECHANISM: str = os.environ.get("KAFKA_SASL_MECHANISM", "PLAIN")

    #Topics
    INPUT_TOPICS: dict = field(default_factory=lambda: {
        "VISA": "visa_tokenised",
        "MASTERCARD": "mastercard_tokenised",
        "MPESA": "mpesa_tokenised",
        "PESAPAL": "pesapal_tokenised",
        "FLUTTERWAVE": "flutterwave_tokenised",
    })
    OUTPUT_TOPICS: dict = field(default_factory=lambda: {
        "VISA": "visa_enriched",
        "MASTERCARD": "mastercard_enriched",
        "MPESA": "mpesa_enriched",
        "PESAPAL": "pesapal_enriched",
        "FLUTTERWAVE": "flutterwave_enriched",
    })
    DLQ_TOPIC: str = "enrichment_dlq"

    #Spark
    CHECKPOINT_BASE: str = os.environ.get("SPARK_CHECKPOINT_DIR", "/checkpoints")
    SCHEMA_DIR: str = os.environ.get("SCHEMA_DIR", "/schema")
    APP_NAME: str = "FraudEnrichmentPipeline"

    #SparkStreaming
    MAX_OFFSETS_PER_TRIGGER: int = int(os.environ.get("MAX_OFFSETS_PER_TRIGGER", "50000"))
    TRIGGER_INTERVAL: str = os.environ.get("TRIGGER_INTERVAL", "1 second")
    WATERMARK_DELAY: str = os.environ.get("WATERMARK_DELAY", "60 seconds")
    STARTING_OFFSETS: str = os.environ.get("STARTING_OFFSETS", "earliest")

    #Enrichment
    HIGH_VALUE_THRESHOLD_USD: float = float(
        os.environ.get("HIGH_VALUE_THRESHOLD_USD", "2000.0")
    )

    #Redis
    REDIS_HOST: str = os.environ.get("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.environ.get("REDIS_PORT", "6379"))
    REDIS_PASSWORD: Optional[str] = os.environ.get("REDIS_PASSWORD")
    REDIS_SSL: bool = os.environ.get("REDIS_SSL", "false").lower() == "true"

    #prometheus
    METRICS_PORT: int = int(os.environ.get("METRICS_PORT", "8000"))
