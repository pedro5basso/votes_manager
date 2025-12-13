import csv
import logging as log
import random
import time
import uuid
from datetime import datetime
from typing import Dict, List

from faker import Faker


from python.utils.boundary_objects import AutonomousRegion, Province

fake = Faker()

class VoteConfiguration:
    TOTAL_VOTES = 1000
    VOTES_PER_SECOND = 100
    BLANK_VOTE_PROVABILITY = 0.01
    GENERATE_CSV_FILE = True

# ----------------------------------------------------
# Clase principal generadora de votos
# ----------------------------------------------------

class VoteGenerator:

    def __init__(
        self,
        mysql_client,
        configuration
    ):
        """
        Params:
        - mysql_client: tu cliente MySQL ya inicializado
        - votes_per_second: nº total de votos a generar por segundo
        - blank_vote_probability: probabilidad de voto en blanco
        - parties: lista de partidos ficticios
        """

        self.mysql = mysql_client
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
        log.info("Cargando provincias y CCAA desde MySQL...")

        # ------ Provincias ------
        query = "SELECT * FROM provincias"

        self.mysql.connect()
        rows = self.mysql.fetch_all(query)
        self.mysql.disconnect()

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

        log.info(f"  ✓ Provincias cargadas: {len(self.provinces)}")

        # ------ Pesos por población ------
        self._build_population_weights()
        log.info(f"  ✓ Pesos generados (lista expandida): {len(self.weighted_provinces)}")


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

    def start(self, callback=None):
        """
        callback → función que recibe cada voto generado
        por defecto: imprime en pantalla
        """
        self.running = True
        interval = 1 / self.votes_per_second

        log.info(f"\n--- Generando votos en tiempo real ---")
        log.info(f"Velocidad: {self.votes_per_second} votos/segundo")
        log.info(f"Intervalo: {interval:.6f} s\n")

        counter_votes = 0
        votes_history = list()

        while self.running:
            vote = self.generate_vote()

            # send vote to kafka
            if callback:
                callback(vote)
            else:
                votes_history.append(vote)
                # print(vote)
            time.sleep(interval)
            if self.config.TOTAL_VOTES and counter_votes > self.config.TOTAL_VOTES:
                self.stop()

            counter_votes += 1

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

        log.info(f"File created succesfully at {path}")
