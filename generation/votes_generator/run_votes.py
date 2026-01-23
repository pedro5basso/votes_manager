from generation.db.database_connector import MySQLClient, MySQLConfig
from generation.votes_generator.vote_generator import VoteConfiguration, VoteGenerator

if __name__ == "__main__":

    # creting database configuration class
    mysql = MySQLClient(
        host=MySQLConfig.HOST,
        user=MySQLConfig.USER,
        password=MySQLConfig.PASSWORD,
        database=MySQLConfig.DATABASE,
        port=MySQLConfig.PORT,
    )

    # creating votes configuration class
    vote_configuration = VoteConfiguration()

    # creating votes generator class
    generator = VoteGenerator(database_client=mysql, configuration=vote_configuration)

    # starting votes generator
    generator.start()
