import csv
import json
import logging
import random
import time
import uuid
from datetime import datetime
from typing import Dict, List

from confluent_kafka import SerializingProducer
from faker import Faker

from python.utils.logging_config import setup_logging
from python.utils.boundary_objects import AutonomousRegion, Province

fake = Faker()

setup_logging(logging.INFO)

log = logging.getLogger(__name__)

class VoteConfiguration:
    TOTAL_VOTES = 1000
    VOTES_PER_SECOND = 100
    BLANK_VOTE_PROVABILITY = 0.01
    GENERATE_CSV_FILE = True
    TOPIC_NAME = "votes_raw"
    KAFKA_PORT = "localhost:9092"

# ----------------------------------------------------
# Clase principal generadora de votos
# ----------------------------------------------------

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
        self.db_client = database_client
        self.config = configuration
        self.votes_per_second = self.config.VOTES_PER_SECOND
        self.blank_vote_probability = self.config.BLANK_VOTE_PROVABILITY

        self.parties = [
            "Gato Unido",
            "Perro Liberal",
            "Lechuga Verde",
            "Pepino Social",
            "Tiburon Popular",
            "Aguila Nacional",
            "Conejo Federal"
        ]

        self.running = False
        self.provinces: List[Province] = []
        self.weighted_provinces: List[Province] = []  # provinces repeated by pop weight

        self.load_reference_data()

    # ----------------------------------------------------
    # 1) Cargar provincias y comunidades desde MySQL
    # ----------------------------------------------------

    def load_reference_data(self):
        """"""
        log.info("[VotesGenerator]: Cargando provincias y CCAA desde MySQL...")

        # ------ Provincias ------
        query = "SELECT * FROM provincias"

        self.db_client.connect()
        rows = self.db_client.fetch_all(query)
        self.db_client.disconnect()

        self.provinces = [
            Province(
                id=row['id'],
                code_province=row['codigo_provincia'],
                name=row['nombre'],
                alternative_name=row['nombre_alternativo'],
                aarr_code=row['codigo_comunidad'],
                total_seats=row['total_diputados'],
                latitude=row['latitud'],
                longitude=row['longitud'],
                population=row['habitantes']
            )
            for row in rows
        ]

        log.info(f"[VotesGenerator]: Provincias cargadas: {len(self.provinces)}")

        # ------ Pesos por población ------
        self._build_population_weights()
        log.info(f"[VotesGenerator]: Pesos generados (lista expandida): {len(self.weighted_provinces)}")


    # ----------------------------------------------------
    # 2) Distribución ponderada por población
    # ----------------------------------------------------

    def _build_population_weights(self):
        """"""
        total_pop = sum(p.population for p in self.provinces)
        self.weighted_provinces = []

        for p in self.provinces:
            weight = max(1, int((p.population / total_pop) * 1000))
            self.weighted_provinces.extend([p] * weight)

    # ----------------------------------------------------
    # 3) Generar un único voto
    # ----------------------------------------------------

    def generate_vote(self) -> Dict:
        """"""
        province = random.choice(self.weighted_provinces)

        blank_vote = random.random() < self.blank_vote_probability
        political_party = None if blank_vote else random.choice(self.parties)

        vote = {
            "id": str(uuid.uuid4()),
            "blank_vote": blank_vote,
            "political_party": political_party,
            "province_code": province.code_province,
            "province_name": province.name,
            "timestamp": datetime.utcnow().isoformat()
        }

        return vote

    # ----------------------------------------------------
    # 4) Bucle generador en tiempo real
    # ----------------------------------------------------

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
                    key=vote['id'],
                    value=json.dumps(vote),
                    on_delivery=delivery_report
                )
                producer.poll(0)

                votes_history.append(vote)

                time.sleep(interval)

                if self.config.TOTAL_VOTES and counter_votes > self.config.TOTAL_VOTES:
                    self.stop()

                counter_votes += 1

            producer.flush()

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
