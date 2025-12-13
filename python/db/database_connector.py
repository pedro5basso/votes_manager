import os

import mysql.connector
from dotenv import load_dotenv
from mysql.connector import Error

load_dotenv()


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
            print("Conexión establecida correctamente.")
        except Error as e:
            print(f"Error de conexión: {e}")

    def disconnect(self):
        """"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("Conexión cerrada.")

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
