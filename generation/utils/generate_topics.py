"""
Script for managing topic creation
"""

from generation.utils.kafka import KafkaUtils

if __name__ == "__main__":
    kafka_utils = KafkaUtils()
    _ = kafka_utils.create_topics()
