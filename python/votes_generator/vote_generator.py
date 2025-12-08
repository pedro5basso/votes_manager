from python.db.database_connector import MySQLClient, MySQLConfig
from python.utils.boundary_objects import Province, AutonomousRegion

import time
import random
import uuid
from typing import List, Dict
from datetime import datetime
from faker import Faker

fake = Faker()


# ----------------------------------------------------
# Clase principal generadora de votos
# ----------------------------------------------------

class VoteGenerator:

    def __init__(
        self,
        mysql_client,
        votes_per_second: int = 100,
        blank_vote_probability: float = 0.01,
        parties: List[str] = None
    ):
        """
        Params:
        - mysql_client: tu cliente MySQL ya inicializado
        - votes_per_second: nº total de votos a generar por segundo
        - blank_vote_probability: probabilidad de voto en blanco
        - parties: lista de partidos ficticios
        """

        self.mysql = mysql_client
        self.votes_per_second = votes_per_second
        self.blank_vote_probability = blank_vote_probability

        self.parties = parties or [
            "Gato Unido",
            "Perro Liberal",
            "Lechuga Verde",
            "Pepino Social",
            "Tiburón Popular",
            "Águila Nacional",
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
        print("Cargando provincias y CCAA desde MySQL...")

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

        print(f"  ✓ Provincias cargadas: {len(self.provinces)}")

        # ------ Pesos por población ------
        self._build_population_weights()
        print(f"  ✓ Pesos generados (lista expandida): {len(self.weighted_provinces)}")


    # ----------------------------------------------------
    # 2) Distribución ponderada por población
    # ----------------------------------------------------

    def _build_population_weights(self):
        total_pop = sum(p.population for p in self.provinces)
        self.weighted_provinces = []

        for p in self.provinces:
            # Número proporcional de "tickets"
            weight = max(1, int((p.population / total_pop) * 1000))
            self.weighted_provinces.extend([p] * weight)

    # ----------------------------------------------------
    # 3) Generar un único voto
    # ----------------------------------------------------

    def generate_vote(self) -> Dict:
        province = random.choice(self.weighted_provinces)

        blank_vote = random.random() < self.blank_vote_probability
        political_party = None if blank_vote else random.choice(self.parties)

        vote = {
            "id": str(uuid.uuid4()),
            "blank_vote": blank_vote,
            "political_party": political_party,
            "province_code": province.code_province,
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

        print(f"\n--- Generando votos en tiempo real ---")
        print(f"Velocidad: {self.votes_per_second} votos/segundo")
        print(f"Intervalo: {interval:.6f} s\n")

        while self.running:
            vote = self.generate_vote()

            if callback:
                callback(vote)
            else:
                print(vote)

            time.sleep(interval)

    def stop(self):
        self.running = False


if __name__ == "__main__":

    mysql = MySQLClient(
        host=MySQLConfig.HOST,
        user=MySQLConfig.USER,
        password=MySQLConfig.PASSWORD,
        database=MySQLConfig.DATABASE,
        port=MySQLConfig.PORT
    )

    # 2) Crear generador
    generator = VoteGenerator(mysql_client=mysql, votes_per_second=10)

    # 3) Ejecutar
    generator.start()
