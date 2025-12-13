from python.db.database_connector import MySQLClient, MySQLConfig
from python.votes_generator.vote_generator import VoteGenerator, VoteConfiguration

if __name__ == "__main__":
    mysql = MySQLClient(
        host=MySQLConfig.HOST,
        user=MySQLConfig.USER,
        password=MySQLConfig.PASSWORD,
        database=MySQLConfig.DATABASE,
        port=MySQLConfig.PORT
    )

    # configuracion
    vote_configuration = VoteConfiguration()

    # 2) Crear generador
    generator = VoteGenerator(mysql_client=mysql, configuration=vote_configuration)

    # 3) Ejecutar
    generator.start()
