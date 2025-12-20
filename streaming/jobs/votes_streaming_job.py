from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count
)
import os

from dotenv import load_dotenv

load_dotenv()

from streaming.schemas.vote_schema import vote_schema

class DataLakeConfig:
    BASE_PATH = "file:///D:/dev/UCM-BD_DE/votes_manager"
    BRONZE_PATH = f"{BASE_PATH}/datalake/bronze/votes"
    SILVER_PATH = f"{BASE_PATH}/datalake/silver/votes"
    GOLD_PATH = f"{BASE_PATH}/datalake/gold/votes"
    CHECKPOINT_BRONZE = f"{BASE_PATH}/checkpoints/bronze/votes"
    CHECKPOINT_SILVER = f"{BASE_PATH}/checkpoints/silver/votes"
    

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


    def _read_votes_from_kafka(self):
        """"""
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "votes_raw")
            .option("startingOffsets", "latest")
            .load()
        )


    def _parse_votes(self, df):
        """"""
        return (
            df.selectExpr("CAST(value AS STRING) as json")
              .select(from_json(col("json"), vote_schema).alias("data"))
              .select("data.*")
        )


    # def write_debug_csv(df, batch_id):
    #     (
    #         df
    #         .coalesce(1)
    #         .write
    #         .mode("append")
    #         .option("header", "true")
    #         .csv("file:///tmp/debug_votes")
    #     )


    def execute_job(self):
        """"""
        raw_df = self._read_votes_from_kafka()
        votes_df = self._parse_votes(raw_df)

        # DEBUG: escribir en consola
        query = (
            votes_df.writeStream
            .outputMode("append")
            .format("console")
            .option("truncate", False)
            .start()
        )

        query.awaitTermination()

        # bronze_df = (
        #     raw_df
        #     .selectExpr(
        #         "CAST(value AS STRING) AS raw_json",
        #         "timestamp AS kafka_timestamp"
        #     )
        # )
        # bronze_query = (
        #     bronze_df
        #     .writeStream
        #     .format("parquet")
        #     .option("path", DataLakeConfig.BRONZE_PATH)
        #     .option("checkpointLocation", DataLakeConfig.CHECKPOINT_BRONZE)
        #     .outputMode("append")
        #     .start()
        # )
        #
        # bronze_query.awaitTermination()

        # silver_df = (
        #     bronze_df
        #     .withColumn("vote", from_json(col("raw_json"), vote_schema))
        #     .select("vote.*", "kafka_timestamp")
        #     .filter(col("id").isNotNull())
        # )
        #
        # silver_query = (
        #     silver_df
        #     .writeStream
        #     .format("parquet")
        #     .option("path", "datalake/silver/votes")
        #     .option("checkpointLocation", "checkpoints/silver/votes")
        #     .outputMode("append")
        #     .start()
        # )
        #
        # votes_by_party = (
        #     silver_df
        #     .withWatermark("timestamp", "30 seconds")
        #     .groupBy(
        #         window("timestamp", "10 seconds"),
        #         col("political_party")
        #     )
        #     .agg(count("*").alias("votes"))
        # )
        #
        # votes_by_province = (
        #     silver_df
        #     .withWatermark("timestamp", "30 seconds")
        #     .groupBy(
        #         window("timestamp", "10 seconds"),
        #         col("province_code")
        #     )
        #     .count()
        # )
        #
        # votes_by_party.writeStream \
        #     .format("console") \
        #     .outputMode("update") \
        #     .start()
