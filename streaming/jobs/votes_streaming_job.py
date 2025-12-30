import logging
import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, from_json, window, to_timestamp, lower

load_dotenv()
from python.utils.logging_config import setup_logging
from python.votes_generator.vote_generator import VoteConfiguration
from streaming.schemas.vote_schema import vote_schema

setup_logging(logging.INFO)

log = logging.getLogger(__name__)

class DataLakeConfig:
    PATH_BASE = "file:///D:/dev/UCM-BD_DE/votes_manager"
    PATH_BRONZE = f"{PATH_BASE}/datalake/bronze/votes"
    PATH_SILVER = f"{PATH_BASE}/datalake/silver/votes"
    PATH_GOLD = f"{PATH_BASE}/datalake/gold/votes"
    PATH_PROVINCES = f'{PATH_GOLD}/votes_by_province'
    PATH_AUTONOMIC_REG = f'{PATH_GOLD}/votes_by_autonomic_reg'
    PATH_POLITICAL_PARTIES = f'{PATH_GOLD}/votes_by_political_party'
    CHECKPOINT_BRONZE = f"{PATH_BASE}/checkpoints/bronze/votes"
    CHECKPOINT_SILVER = f"{PATH_BASE}/checkpoints/silver/votes"
    CHECKPOINT_GOLD = f"{PATH_BASE}/checkpoints/gold/votes"
    CHECKPOINT_PROVINCES = f'{CHECKPOINT_GOLD}/provinces'
    CHECKPOINT_AUTONOMIC_REG = f'{CHECKPOINT_GOLD}/autonomic_reg'
    CHECKPOINT_POLITICAL_PARTIES = f'{CHECKPOINT_GOLD}/political_parties'
    

class StreamingJob:
    
    def __init__(self):
        """"""
        self.scala_version=os.getenv("SCALA_VERSION")
        self.spark_version=os.getenv("SPARk_VERSION")
        self.spark = self._create_spark_session()
        self.spark.sparkContext.setLogLevel("WARN")


    def _create_spark_session(self):
        """"""
        return (
            SparkSession.builder
            .appName("VotesManager")
            .config(
                "streaming.jars.packages",
                f"org.apache.streaming:streaming-sql-kafka-0-10_{self.scala_version}:{self.spark_version}"
            )
            .getOrCreate()
        )


    def execute_job(self):
        """"""
        log.info("[StreamingJob]: Executing job")
        # getting raw data from kafka
        raw_df = self._read_votes_from_kafka()

        # Bronze
        bronze_df = self._parse_votes(raw_df)
        bronze_query = self._write_on_datalake(bronze_df, DataLakeConfig.PATH_BRONZE, DataLakeConfig.CHECKPOINT_BRONZE)

        silver_df = (
            bronze_df
            .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")) \
            .withColumn("political_party_lower", lower(col("political_party")))
        )

        silver_query = (
            silver_df
            .writeStream
            .format("parquet")
            .option("path", DataLakeConfig.PATH_SILVER)
            .option("checkpointLocation", DataLakeConfig.CHECKPOINT_SILVER)
            .outputMode("append")
            .start()
        )

        # votes_by_party = (
        #     silver_df
        #     .withWatermark("timestamp", "30 seconds")
        #     .groupBy(
        #         window("timestamp", "10 seconds"),
        #         col("political_party")
        #     )
        #     .agg(count("*").alias("votes"))
        # )

        # votes_by_party_es = (
        #     votes_by_party
        #     .withColumn("window_start", col("window.start"))
        #     .withColumn("window_end", col("window.end"))
        #     .drop("window")
        # )

        query = (
            silver_df
            .writeStream
            .outputMode("append")
            .foreachBatch(self._write_to_elasticsearch)
            .option("path", DataLakeConfig.PATH_GOLD)
            .option("checkpointLocation", f"{DataLakeConfig.CHECKPOINT_GOLD}/votes_by_party")
            .start()
        )

        bronze_query.awaitTermination()
        silver_query.awaitTermination()
        query.awaitTermination()


    def _debug_console_df(self, df):
        """"""
        # DEBUG: escribir en consola
        query = (
            df.writeStream
            .outputMode("append")
            .format("console")
            .option("truncate", False)
            .start()
        )

        query.awaitTermination()


    def _read_votes_from_kafka(self):
        """"""
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", VoteConfiguration.TOPIC_NAME)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )


    def _parse_votes(self, df):
        """"""
        return (
            df.selectExpr("CAST(value AS STRING) as json")
              .select(from_json(col("json"), vote_schema).alias("data"))
              .select("data.*")
        )


    def _write_on_datalake(self, df, layer_path, checkpoint_path):
        """"""
        return (
            df
            .writeStream
            .format("parquet")
            .option("path", layer_path)
            .option("checkpointLocation", checkpoint_path)
            .outputMode("append")
            .start()
        )


    def _write_to_elasticsearch(self, batch_df, batch_id):
        """"""
        (
            batch_df
            .write
            .format("org.elasticsearch.spark.sql")
            .option("es.nodes", "localhost")
            .option("es.port", "9200")
            .option("es.resource", "votes_01")
            .option("es.nodes.wan.only", "true")
            .mode("append")
            .save()
        )
