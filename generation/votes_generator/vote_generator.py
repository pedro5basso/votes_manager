import json
import logging
import random
import time
import uuid
from datetime import datetime
from typing import Dict, List

from generation.coordinates.coordinates import ProvinceCoordinates
from generation.db.get_db_information import DataBaseInformationObject
from generation.utils.boundary_objects import Province
from generation.utils.kafka import KafkaConfiguration, KafkaUtils
from logs.logging_config import setup_logging

setup_logging(logging.INFO)

log = logging.getLogger(__name__)


class VoteConfiguration:
    """
    Configuration values for vote generation behavior.
    """

    TOTAL_VOTES = 1000
    VOTES_PER_SECOND = 100
    BLANK_VOTE_PROVABILITY = 0.01


class VoteGenerator:
    """
    Generates synthetic voting events and publishes them to Kafka.

    Votes are generated in real time, weighted by province population
    and party popularity.
    """

    def __init__(self, database_client, configuration):
        """
        Initializes the VoteGenerator.

        Args:
            database_client: Database client used to retrieve country,
                province, and political party information.
            configuration (VoteConfiguration): Vote generation configuration.
        """
        self.db_info_object = DataBaseInformationObject(database_client)
        # self.country = self.db_info_object.country
        self.config = configuration

        self.parties = self.db_info_object.get_political_parties()
        self._build_party_weights()
        self.country = self.db_info_object.country
        self.names_mapped = self.db_info_object.mapped_names
        self.iso_codes_mapped = self.db_info_object.mapped_iso_codes

        self.kafka_utils = KafkaUtils()
        self.coord_provinces = ProvinceCoordinates()

        self.running = False
        self.provinces: List[Province] = []
        self.weighted_provinces: List[Province] = []

        self.load_provinces_data()

    def load_provinces_data(self):
        """
        Loads provinces from the country object and builds population weights.
        """
        for region in self.country.regions:
            for province in region.provinces:
                self.provinces.append(province)

        # population weights
        self._build_population_weights()
        log.info(
            f"[VotesGenerator]: Population weights (expanded list): {len(self.weighted_provinces)}"
        )

    def _build_population_weights(self):
        """
        Builds a population-weighted list of provinces.

        Provinces are repeated proportionally to their population to allow
        weighted random selection.
        """
        total_pop = sum(p.population for p in self.provinces)
        self.weighted_provinces = []

        for p in self.provinces:
            weight = max(1, int((p.population / total_pop) * 1000))
            self.weighted_provinces.extend([p] * weight)

    def generate_vote(self) -> Dict:
        """
        Generates a single vote event.

        The vote is assigned to a province using population weights and to
        a political party using popularity weights. Blank votes are generated
        based on configuration probability.

        Returns:
            Dict: A dictionary representing the generated vote.
        """
        province = random.choice(self.weighted_provinces)

        autonomic_name = self.names_mapped.get(province.name)
        autonomic_iso_code = self.iso_codes_mapped.get(province.name)

        location = self.coord_provinces.get_coordinate_from_province(province.code_province)

        blank_vote = random.random() < self.config.BLANK_VOTE_PROVABILITY
        political_party = None if blank_vote else self._choose_party()

        vote = {
            "id": str(uuid.uuid4()),
            "blank_vote": blank_vote,
            "political_party": political_party,
            "province_name": province.name,
            "province_iso_code": province.iso_3166_2_code,
            "autonomic_region_name": autonomic_name,
            "autonomic_region_iso_code": autonomic_iso_code,
            "location": location,
            "timestamp": datetime.utcnow().isoformat(),
        }

        return vote

    def start(self):
        """
        Starts generating votes and publishing them to Kafka.

        Votes are produced at a fixed rate defined by the configuration.
        """

        self.running = True
        interval = 1 / self.config.VOTES_PER_SECOND

        producer = self.kafka_utils.get_kafka_producer()

        log.info(f"[VotesGenerator]: --- Generating real time votes ---")
        log.info(
            f"[VotesGenerator]: Velocity: {self.config.VOTES_PER_SECOND} votes/second"
        )
        log.info(f"[VotesGenerator]: Interval: {interval:.6f} s\n")

        counter_votes = 0

        try:
            while self.running:
                vote = self.generate_vote()
                # print(vote)

                # send vote to kafka
                producer.produce(
                    KafkaConfiguration.TOPIC_VOTES_RAW,
                    key=vote["id"],
                    value=json.dumps(vote),
                    on_delivery=self.kafka_utils.delivery_report,
                )
                producer.poll(0.1)
                time.sleep(interval)

                counter_votes += 1

                if self.config.TOTAL_VOTES and counter_votes >= self.config.TOTAL_VOTES:
                    self.stop()

        except BufferError as be:
            log.error(f"[VotesGenerator]: Buffer full: {be}")
            time.sleep(1)
        except Exception as e:
            log.error(f"[VotesGenerator]: Error generating votes: {e}")

        finally:
            log.info("[VotesGenerator]: Flushing remaining messages...")
            producer.flush()

    def stop(self):
        """
        Stops the vote generation loop.
        """
        self.running = False

    def _build_party_weights(self):
        """
        Builds internal lists of party names and their popularity weights.
        """
        self._party_names = [p["name"] for p in self.parties]
        self._party_weights = [p["popularity"] for p in self.parties]

    def _choose_party(self) -> str:
        """
        Selects a political party using weighted random choice.

        Returns:
            str: Name of the selected political party.
        """
        return random.choices(self._party_names, weights=self._party_weights, k=1)[0]
