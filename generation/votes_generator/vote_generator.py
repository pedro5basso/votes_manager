import csv
import json
import logging
import math
import random
import time
import uuid
from datetime import datetime
from typing import Dict, List

from faker import Faker

from generation.utils.logging_config import setup_logging
from generation.db.get_db_information import DataBaseInformationObject
from generation.utils.boundary_objects import Province, AutonomousRegion
from generation.utils.kafka import KafkaUtils, KafkaConfiguration

fake = Faker()

setup_logging(logging.INFO)

log = logging.getLogger(__name__)

class VoteConfiguration:
    # Total votes to generate
    COUNTRY_POBLATION = 50000000
    PERCENT_VOTE = 0.6
    VOTES_PER_SECOND = 100
    # TOTAL_VOTES = int(COUNTRY_POBLATION * PERCENT_VOTE)
    TOTAL_VOTES = 1000
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

        self.parties = self.db_info_object.get_political_parties()
        self.country = self.db_info_object.country
        self.names_mapped = self.db_info_object.mapped_names
        self.iso_codes_mapped = self.db_info_object.mapped_iso_codes

        self.kafka_utils = KafkaUtils()

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
        autonomic_iso_code = self.iso_codes_mapped.get(province.name)

        location = f"{province.latitude},{province.longitude}"

        blank_vote = random.random() < self.config.BLANK_VOTE_PROVABILITY
        political_party = None if blank_vote else random.choice(self.parties)

        vote = {
            "id": str(uuid.uuid4()),
            "blank_vote": blank_vote,
            "political_party": political_party,
            "province_name": province.name,
            "province_iso_code": province.iso_3166_2_code,
            "autonomic_region_name": autonomic_name,
            "autonomic_region_iso_code": autonomic_iso_code,
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
                log.info(
                    f"[VotesGenerator]: Message delivered to {msg.topic()} [{msg.partition()}]"
                )

        self.running = True
        interval = 1 / self.config.VOTES_PER_SECOND

        producer = self.kafka_utils.get_kafka_producer()

        log.info(f"[VotesGenerator]: --- Generating real time votes ---")
        log.info(f"[VotesGenerator]: Velocity: {self.config.VOTES_PER_SECOND} votes/second")
        log.info(f"[VotesGenerator]: Intervalo: {interval:.6f} s\n")

        counter_votes = 0
        votes_history = list()

        try:
            while self.running:
                vote = self.generate_vote()
                # print(vote)

                # send vote to kafka
                producer.produce(
                    KafkaConfiguration.TOPIC_NAME,
                    key=vote['province_iso_code'],
                    value=json.dumps(vote),
                    on_delivery=self.kafka_utils.delivery_report
                )
                producer.poll(0)
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

        finally:
            log.info("[VotesGenerator]: Flushing remaining messages...")
            producer.flush()

            if self.config.GENERATE_CSV_FILE:
                self._generate_csv_file(votes_history)


    def stop(self):
        self.running = False


    def _generate_csv_file(self, list_votes):
        """"""
        path = r"D:\tmp\votes\votes.csv"
        if not list_votes:
            log.error(f"[VotesGenerator]: Votes list is empty, not able to create a csv file...")
            return

        headers = set()
        for d in list_votes:
            headers.update(d.keys())
        headers = list(headers)

        with open(path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(list_votes)

        log.info(f"[VotesGenerator]: File created succesfully at {path}")
