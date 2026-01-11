import logging
import time

from confluent_kafka import SerializingProducer, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from generation.utils.logging_config import setup_logging

setup_logging(logging.INFO)

log = logging.getLogger(__name__)

class KafkaConfiguration:
    NUM_PARTITIONS = 3
    REPLICATION_FACTOR = 1 # proporción 1:1 con el num de brokers
    TOPIC_NAME = "votes_raw"
    KAFKA_BROKER_DOCKER = "kafka-broker-1:19092"
    KAFKA_BROKER_LOCAL = "localhost:29092"
    AGGREGATED_TOPIC = "votes"


class KafkaUtils:

    def __init__(self):
        """"""
        pass

    def create_topics(self):
        """"""
        for topic_name in [KafkaConfiguration.TOPIC_NAME, KafkaConfiguration.AGGREGATED_TOPIC]:
            self._create_topic(topic_name)


    def _create_topic(self, topic_name):
        """"""
        admin = AdminClient({"bootstrap.servers": KafkaConfiguration.KAFKA_BROKER_LOCAL})

        metadata = admin.list_topics(timeout=10)

        if topic_name in metadata.topics:
            log.info(f"[KafkaUtils]: Topic '{topic_name}' already exists, returning")
            return True

        log.info(f"[KafkaUtils]: Creating topic '{topic_name}'...")

        new_topic = NewTopic(
            topic=topic_name,
            num_partitions=KafkaConfiguration.NUM_PARTITIONS,
            replication_factor=KafkaConfiguration.REPLICATION_FACTOR,
        )

        futures = admin.create_topics([new_topic])

        for topic, future in futures.items():
            try:
                future.result()
                log.info(f"[KafkaUtils]: Topic '{topic}' created succesfully")
                log.info("[KafkaUtils]: Waiting 5 seconds for metadata propagation...")
                time.sleep(5)
            except KafkaException as e:
                log.error(f"[KafkaUtils]: Error creating topic '{topic}': {e}")
                return False

        return True


    def get_kafka_producer(self):
        """"""
        producer_config = {
            'bootstrap.servers': KafkaConfiguration.KAFKA_BROKER_LOCAL,
            'batch.num.messages': 1000,
            'linger.ms': 10,
            'compression.type': 'gzip'
        }
        
        producer = Producer(producer_config)

        return producer


    @staticmethod
    def delivery_report(err, msg):
        if err:
            log.error(f"[KafkaUtils]: Delivery failed: {err}")
        else:
            log.info(
                f"[KafkaUtils]: Message delivered to {msg.topic()} [{msg.partition()}]"
            )