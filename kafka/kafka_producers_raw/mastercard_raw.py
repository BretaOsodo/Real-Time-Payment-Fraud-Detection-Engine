from confluent_kafka import Producer
from data_generator.data_generator import TransactionDataGenerator
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def produce_mastercard_transaction():
    producer_config = {
        'bootstrap.servers': 'localhost:9092',  # external port from docker-compose
        'acks': 'all',
        'retries': 10,
        'enable.idempotence': True               # note: idempotence not idempotent
    }

    producer = Producer(producer_config)
    generator = TransactionDataGenerator()
    count = 0

    while count < 100:
        mastercard_data=generator.generate_mastercard_transaction()

        producer.produce(
            topic='mastercard_raw',
            value=json.dumps(mastercard_data).encode('utf-8')
        )

        count += 1
        producer.poll(0)   # serve delivery callbacks without blocking
        print(f'Record [{count}]: {mastercard_data}')
        logger.info(f'Successfully produced record [{count}]')

    producer.flush()       # flush once at the end, not inside the loop
    logger.info('All records produced successfully')


if __name__ == "__main__":
    produce_mastercard_transaction()