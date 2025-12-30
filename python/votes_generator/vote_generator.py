import csv
import json
import logging
import math
import random
import time
import uuid
from datetime import datetime
from typing import Dict, List

from confluent_kafka import SerializingProducer
from faker import Faker

from python.utils.logging_config import setup_logging
from python.db.get_db_information import DataBaseInformationObject
from python.utils.boundary_objects import Province, AutonomousRegion

fake = Faker()

setup_logging(logging.INFO)

log = logging.getLogger(__name__)

class VoteConfiguration:
    # Total votes to generate
    COUNTRY_POBLATION = 50000000
    PERCENT_VOTE = 0.6
    # TOTAL_VOTES = int(COUNTRY_POBLATION * PERCENT_VOTE)
    TOTAL_VOTES = 1000

    # num of partitions for the topic
    MESSAGE_SIZE_BYTES = 500
    BYTES_TO_MB = 1024*1024
    VOTES_PER_SECOND = 100
    MB_PER_SECOND_PRODUCTION = (VOTES_PER_SECOND * MESSAGE_SIZE_BYTES) / BYTES_TO_MB
    MB_PER_KAFKA_PARTITION = 7.5
    _NUM_PARTITIONS = MB_PER_SECOND_PRODUCTION/MB_PER_KAFKA_PARTITION
    NUM_PARTITIONS = math.ceil(_NUM_PARTITIONS) if int(_NUM_PARTITIONS) else 1

    TOPIC_NAME = "votes_raw_v03"
    KAFKA_PORT = "localhost:9092"

    BLANK_VOTE_PROVABILITY = 0.01
    GENERATE_CSV_FILE = True


class VoteGenerator:

    def __init__(
        self,
        database_client,
        configuration
    ):
        """
        Params:
        - mysql_client: tu cliente MySQL ya inicializado
        - votes_per_second: nº total de votos a generar por segundo
        - blank_vote_probability: probabilidad de voto en blanco
        - parties: lista de partidos ficticios
        """
        self.db_info_object = DataBaseInformationObject(database_client)
        # self.country = self.db_info_object.country
        self.config = configuration
        self.votes_per_second = self.config.VOTES_PER_SECOND
        self.blank_vote_probability = self.config.BLANK_VOTE_PROVABILITY

        self.parties = self.db_info_object.get_political_parties()
        self.country = self.db_info_object.country
        self.names_mapped = self.db_info_object.mapped_names

        self.running = False
        self.provinces: List[Province] = []
        self.weighted_provinces: List[Province] = []  # provinces repeated by pop weight

        self.load_provinces_data()


    def load_provinces_data(self):
        """"""
        for region in self.country.regions:
            for province in region.provinces:
                self.provinces.append(province)

        # population weights
        self._build_population_weights()
        log.info(f"[VotesGenerator]: Population weights (expanded list): {len(self.weighted_provinces)}")


    def _build_population_weights(self):
        """"""
        total_pop = sum(p.population for p in self.provinces)
        self.weighted_provinces = []

        for p in self.provinces:
            weight = max(1, int((p.population / total_pop) * 1000))
            self.weighted_provinces.extend([p] * weight)


    def generate_vote(self) -> Dict:
        """"""
        province = random.choice(self.weighted_provinces)

        autonomic_name = self.names_mapped.get(province.name)

        location = f"{province.latitude},{province.longitude}"

        blank_vote = random.random() < self.blank_vote_probability
        political_party = None if blank_vote else random.choice(self.parties)

        vote = {
            "id": str(uuid.uuid4()),
            "blank_vote": blank_vote,
            "political_party": political_party,
            "province_code": province.code_province,
            "province_name": province.name,
            "autonomic_region_name": autonomic_name,
            "location": location,
            "timestamp": datetime.utcnow().isoformat()
        }

        return vote


    def start(self):
        """
        por defecto: imprime en pantalla
        """

        def delivery_report(err, msg):
            if err:
                log.error(f"[VotesGenerator]: Delivery failed: {err}")
            else:
                log.debug(
                    f"[VotesGenerator]: Message delivered to {msg.topic()} [{msg.partition()}]"
                )

        self.running = True
        interval = 1 / self.votes_per_second

        log.info(f"[VotesGenerator]: --- Generando votos en tiempo real ---")
        log.info(f"[VotesGenerator]: Velocidad: {self.votes_per_second} votos/segundo")
        log.info(f"[VotesGenerator]: Intervalo: {interval:.6f} s\n")

        counter_votes = 0
        votes_history = list()

        producer = SerializingProducer({
            'bootstrap.servers': self.config.KAFKA_PORT
            # 'on_delivery': delivery_report
        })

        try:

            while self.running:
                vote = self.generate_vote()
                # print(vote)

                # send vote to kafka
                producer.produce(
                    self.config.TOPIC_NAME,
                    key=vote['province_code'],
                    value=json.dumps(vote),
                    on_delivery=delivery_report
                )
                producer.poll(0)
                producer.flush()

                votes_history.append(vote)

                time.sleep(interval)

                if self.config.TOTAL_VOTES and counter_votes > self.config.TOTAL_VOTES:
                    self.stop()

                counter_votes += 1

        except BufferError as be:
            log.error(f"[VotesGenerator]: Buffer full: {be}")
            time.sleep(1)
        except Exception as e:
            log.error(f"[VotesGenerator]: Error generating votes: {e}")

        if self.config.GENERATE_CSV_FILE:
            self._generate_csv_file(votes_history)


    def stop(self):
        self.running = False


    def _generate_csv_file(self, list_votes):
        """"""
        path = r"D:\tmp\votes\votes.csv"
        if not list_votes:
            raise ValueError("Empy votes list")

        headers = set()
        for d in list_votes:
            headers.update(d.keys())
        headers = list(headers)

        with open(path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(list_votes)

        log.info(f"[VotesGenerator]: File created succesfully at {path}")
