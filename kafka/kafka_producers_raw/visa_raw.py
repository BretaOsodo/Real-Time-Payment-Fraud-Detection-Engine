from confluent_kafka import Producer
from data_generator.data_generator import TransactionDataGenerator
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#Generate the visa data
generator = TransactionDataGenerator()
visa_data = generator.generate_visa_transaction()

def produce_visa_transaction():
    producer_config = {
        'bootstrap.servers': 'localhost:9092',
        'acks': 'all',
        'retries': 10,
        'enable.idempotent': True
    }

    producer = Producer(
        producer_config,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    count = 0

    while count < 100: #simulation of 100 transactions per minutes
        producer.produce('visa_raw', value= visa_data)
        count += 1
        print(f'Record[{count}]:{visa_data}')
        producer.flush()

        logger.info(f'Successfully produced record[{count}]:{visa_data}')