import os
import logging
import mysql.connector
from dotenv import load_dotenv
from mysql.connector import Error

from generation.utils.logging_config import setup_logging

load_dotenv()

setup_logging(logging.INFO)

log = logging.getLogger(__name__)


class MySQLConfig:
    """"""
    HOST = os.getenv("DB_HOST")
    PORT = int(os.getenv("DB_PORT"))
    USER = os.getenv("DB_USER")
    PASSWORD = os.getenv("DB_PASSWORD")
    DATABASE = os.getenv("DB_NAME")


class MySQLClient:
    """"""

    def __init__(self, host, user, password, database, port):
        """"""
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.connection = None

    def connect(self):
        """"""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                port=self.port,
            )
            log.info("[DB]: Connected correctly.")
        except Error as e:
            log.error(f"[DB]: Connexion error: {e}")

    def disconnect(self):
        """"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            log.info("[DB]: Connexion closed.")

    def fetch_all(self, query, params=None):
        """"""
        cursor = self.connection.cursor(dictionary=True)
        cursor.execute(query, params or ())
        result = cursor.fetchall()
        cursor.close()
        return result

    def fetch_one(self, query, params=None):
        """"""
        cursor = self.connection.cursor(dictionary=True)
        cursor.execute(query, params or ())
        result = cursor.fetchone()
        cursor.close()
        return result

    def execute(self, query, params=None):
        """"""
        cursor = self.connection.cursor()
        cursor.execute(query, params or ())
        self.connection.commit()
        cursor.close()
