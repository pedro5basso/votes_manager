import logging
import time

from confluent_kafka import KafkaError, KafkaException, Message, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from generation.utils.logging_config import setup_logging

setup_logging(logging.INFO)

log = logging.getLogger(__name__)


class KafkaConfiguration:
    """
    Centralized configuration values for Kafka setup.

    This class defines broker addresses, topic names, and
    default topic configuration such as partitions and replication.
    """

    NUM_PARTITIONS = 3
    REPLICATION_FACTOR = 1
    KAFKA_BROKER_DOCKER = "kafka-broker-1:19092"
    KAFKA_BROKER_LOCAL = "localhost:29092"
    TOPIC_VOTES_RAW = "votes_raw"
    TOPIC_VOTES_CLEAN = "votes_clean"
    TOPIC_VOTES_SEATS_PROVINCES = "votes_seats_provinces"
    TOPICS = [
        (TOPIC_VOTES_RAW, False),
        (TOPIC_VOTES_CLEAN, False),
        (TOPIC_VOTES_SEATS_PROVINCES, True),
    ]


class KafkaUtils:
    """
    Utility class for Kafka administrative and producer-related operations.
    """

    def __init__(self):
        """
        Initializes the KafkaUtils instance.

        Currently, it does not require any instance-level configuration.
        """
        pass

    def create_topics(self):
        """
        Creates all Kafka topics defined in KafkaConfiguration.

        Topics are created only if they do not already exist.
        The cleanup policy is applied based on configuration.
        """
        for topic_name, cleanup_policy_flag in KafkaConfiguration.TOPICS:
            self._create_topic(topic_name, cleanup_policy_flag)

    def _create_topic(self, topic_name: str, cleanup_policy_flag: bool) -> bool:
        """
        Creates a single Kafka topic if it does not already exist.

            Args:
                topic_name (str): Name of the Kafka topic to create.
                cleanup_policy_flag (bool): If True, uses 'compact' cleanup policy;
                    otherwise, uses 'delete'.

        Returns:
            bool: True if the topic exists or was created successfully,
            False if an error occurred during creation.
        """
        admin = AdminClient(
            {"bootstrap.servers": KafkaConfiguration.KAFKA_BROKER_LOCAL}
        )

        metadata = admin.list_topics(timeout=10)

        if topic_name in metadata.topics:
            log.info(f"[KafkaUtils]: Topic '{topic_name}' already exists, returning")
            return True

        log.info(f"[KafkaUtils]: Creating topic '{topic_name}'...")

        cleanup_policy = "compact" if cleanup_policy_flag else "delete"
        config = {"cleanup.policy": cleanup_policy}

        new_topic = NewTopic(
            topic=topic_name,
            num_partitions=KafkaConfiguration.NUM_PARTITIONS,
            replication_factor=KafkaConfiguration.REPLICATION_FACTOR,
            config=config,
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

    def get_kafka_producer(self) -> Producer:
        """
        Creates and returns a configured Kafka producer.

        Returns:
            Producer: Configured Kafka producer instance.
        """
        producer_config = {
            "bootstrap.servers": KafkaConfiguration.KAFKA_BROKER_LOCAL,
            "retries": 3,
            "batch.num.messages": 500,
            "queue.buffering.max.messages": 1000,
            "linger.ms": 10,
            "compression.type": "gzip",
        }

        producer = Producer(producer_config)

        return producer

    @staticmethod
    def delivery_report(err: KafkaError, msg: Message):
        """
        Callback function to report the delivery result of a Kafka message.

        Args:
            err (KafkaError | None): Error information if delivery failed.
            msg (Message): Kafka message that was produced.
        """
        if err:
            log.error(f"[KafkaUtils]: Delivery failed: {err}")
