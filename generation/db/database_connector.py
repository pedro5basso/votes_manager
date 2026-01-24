import logging
import os
from typing import List, Dict, Optional
import mysql.connector
from dotenv import load_dotenv
from logs.logging_config import setup_logging
from mysql.connector import Error

load_dotenv()

setup_logging(logging.INFO)

log = logging.getLogger(__name__)


class MySQLConfig:
    """
    Configuration values for MySQL database connection.

    Values are loaded from environment variables.
    """

    LOCALHOST = os.getenv("DB_LOCALHOST")
    DOCKER_HOST = os.getenv("DB_DOCKERHOST")
    PORT = int(os.getenv("DB_PORT"))
    USER = os.getenv("DB_USER")
    PASSWORD = os.getenv("DB_PASSWORD")
    DATABASE = os.getenv("DB_NAME")


class MySQLClient:
    """
    Simple MySQL client wrapper for common database operations.
    """

    def __init__(self, host: str, user: str, password: str, database: str, port: int):
        """
        Initializes the MySQL client.

        Args:
            host (str): Database host.
            user (str): Database user.
            password (str): Database password.
            database (str): Database name.
            port (int): Database port.
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.connection = None

    def connect(self):
        """
        Establishes a connection to the MySQL database.
        Raises:
            Exception if the connection fails.
        """
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
            raise e

    def disconnect(self):
        """
        Closes the database connection if it is open.
        """
        if self.connection and self.connection.is_connected():
            self.connection.close()
            log.info("[DB]: Connexion closed.")

    def fetch_all(self, query: str, params: tuple = None) -> List[Dict]:
        """
        Executes a SELECT query and returns all resulting rows.

        Args:
            query (str): SQL query to execute.
            params (tuple, optional): Query parameters.

        Returns:
            list[dict]: List of rows represented as dictionaries.
        """
        cursor = self.connection.cursor(dictionary=True)
        cursor.execute(query, params or ())
        result = cursor.fetchall()
        cursor.close()
        return result

    def fetch_one(self, query: str, params: tuple = None) -> Optional[Dict]:
        """
        Executes a SELECT query and returns a single row.

        Args:
            query (str): SQL query to execute.
            params (tuple, optional): Query parameters.

        Returns:
            dict | None: Single row as a dictionary, or None if no result.
        """
        cursor = self.connection.cursor(dictionary=True)
        cursor.execute(query, params or ())
        result = cursor.fetchone()
        cursor.close()
        return result

    def execute(self, query: str, params: tuple = None):
        """
        Executes a write operation (INSERT, UPDATE, DELETE).

        Args:
            query (str): SQL query to execute.
            params (tuple, optional): Query parameters.
        """
        cursor = self.connection.cursor()
        cursor.execute(query, params or ())
        self.connection.commit()
        cursor.close()
