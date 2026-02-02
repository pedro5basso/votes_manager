from generation.utils.kafka import KafkaConfiguration
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from streaming.datalake.datalake_configuration import DataLakeConfig
from streaming.schemas.vote_schema import vote_schema, vote_schema_norm
from generation.db.get_db_information import DataBaseInformationObject
from generation.db.database_connector import MySQLClient, MySQLConfig


class SparkJob:
    """
    Spark Structured Streaming job for real-time vote processing.

    This job consumes votes from Kafka, persists them across bronze and
    silver data lake layers, publishes cleaned votes back to Kafka,
    and computes seat allocation per province using the D'Hondt method.
    """

    def __init__(self):
        """
        Initializes the Spark session and reference datasets.

        Configures Spark for Kafka integration, streaming checkpoints,
        and state storage. Also loads the reference data used for
        seat allocation.
        """
        self.spark = (
            SparkSession.builder.appName("VotesProcessingJob")
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6",
            )
            .config(
                "spark.sql.streaming.checkpointLocation", DataLakeConfig.PATH_CHECKPOINT
            )
            .config(
                "spark.sql.streaming.stateStore.stateStoreDir",
                DataLakeConfig.PATH_STATES,
            )
            .config("spark.sql.shuffle.partitions", 20)
        ).getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")

        mysql_client = MySQLClient(
            host=MySQLConfig.DOCKER_HOST,
            user=MySQLConfig.USER,
            password=MySQLConfig.PASSWORD,
            database=MySQLConfig.DATABASE,
            port=MySQLConfig.PORT,
        )
        self.databaseObject = DataBaseInformationObject(mysql_client)


        self.df_seats_ref = self.spark.createDataFrame(
            self.databaseObject.get_seats_by_province(), ["province_name", "total_seats"]
        )

    def run_job(self):
        """
        Executes the streaming pipeline.

        This method:
        - Reads raw votes from Kafka
        - Writes raw data to the bronze layer
        - Normalizes and deduplicates votes into the silver layer
        - Publishes cleaned votes to Kafka
        - Aggregates votes and computes seats in real time
        """
        # stream for kafka messages
        stream_kafka = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KafkaConfiguration.KAFKA_BROKER_DOCKER)
            .option("subscribe", KafkaConfiguration.TOPIC_VOTES_RAW)
            .option("startingOffsets", "earliest")
            .load()
        )

        # parsing messages to df format
        df_raw_votes_from_kafka = (
            stream_kafka.selectExpr("CAST(value AS STRING)")
            .select(F.from_json(F.col("value"), vote_schema).alias("data"))
            .select("data.*")
        )

        # writing on bronze layer
        stream_bronze = (
            df_raw_votes_from_kafka.writeStream.format("parquet")
            .option("path", DataLakeConfig.PATH_BRONZE)
            .option("checkpointLocation", DataLakeConfig.CHECKPOINT_BRONZE)
            .outputMode("append")
            .start()
        )

        # removing duplicates ids
        df_votes_from_kafka_no_duplicated_id = df_raw_votes_from_kafka.dropDuplicates(
            ["id"]
        ).select(
            "id",
            "timestamp",
            "political_party",
            "province_name",
            "autonomic_region_name",
            "blank_vote",
            "location",
            "province_iso_code",
            "autonomic_region_iso_code",
        )

        # adapting blank votes
        df_votes_processed = df_votes_from_kafka_no_duplicated_id.withColumn(
            "political_party_norm",
            F.when(F.col("blank_vote") == True, F.lit("BLANK")).otherwise(
                F.col("political_party")
            ),
        )

        # writing on silver
        stream_silver = (
            df_votes_processed.writeStream.format("parquet")
            .option("path", DataLakeConfig.PATH_SILVER_NORMALIZED)
            .option("checkpointLocation", DataLakeConfig.CHECKPOINT_SILVER_NORMALIZED)
            .outputMode("append")
            .start()
        )

        # adding hash id for ElasticSearch
        df_votes_processed_with_doc_id = df_votes_processed.withColumn(
            "doc_id",
            F.sha2(
                F.concat_ws(
                    "||", "political_party", "province_name", "autonomic_region_name"
                ),
                256,
            ),
        )

        # sending messages to votes_clean topic on kafka
        stream_aggregation = (
            df_votes_processed_with_doc_id.withColumn(
                "key", F.col("doc_id").cast("string")
            )
            .withColumn(
                "value",
                F.to_json(
                    F.struct(
                        F.col("doc_id").alias("doc_id"),
                        F.col("id").cast("string").alias("id"),
                        "political_party",
                        "location",
                        "timestamp",
                        "province_iso_code",
                        "province_name",
                        "blank_vote",
                        "autonomic_region_iso_code",
                        "autonomic_region_name",
                    )
                ),
            )
            .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
            .writeStream.format("kafka")
            .outputMode("append")
            .option("kafka.bootstrap.servers", KafkaConfiguration.KAFKA_BROKER_DOCKER)
            .option("topic", KafkaConfiguration.TOPIC_VOTES_CLEAN)
            .option("checkpointLocation", DataLakeConfig.CHECKPOINT_GOLD_VOTES_CLEAN)
            .start()
        )

        # getting all votes from silver layer to compute seats with all available votes
        stream_votes_silver = (
            self.spark.readStream.schema(vote_schema_norm)
            .format("parquet")
            .load(DataLakeConfig.PATH_SILVER_NORMALIZED)
        )

        # making aggregation on real time (Stateful Streaming)
        # keeping the total count on memory/checkpoint
        # adding regions and provinces in order to be able to compute total seats by area
        df_votes_aggregated = (
            stream_votes_silver.groupBy("autonomic_region_name","province_name", "political_party_norm")
            .count()
            .withColumnRenamed("count", "votes_per_party")
        )

        # computing seats based on all the votes generated.
        # the computation will be calculated with a time trigger
        stream_seats = (
            df_votes_aggregated.writeStream.outputMode(
                "complete"
            )  # sending all information updated on each trigger
            .foreachBatch(self.compute_seats_from_votes)
            .trigger(processingTime="1 minute")
            .option(
                "checkpointLocation",
                DataLakeConfig.CHECKPOINT_GOLD_PARTIES_PROVINCES_SEATS,
            )
            .start()
        )

        # ending streams
        stream_bronze.awaitTermination()
        stream_silver.awaitTermination()
        stream_aggregation.awaitTermination()
        stream_seats.awaitTermination()

    def compute_seats_from_votes(self, df_batch: DataFrame, batch_id: int):
        """
        Computes seat allocation per province using the D'Hondt method.

        This method is executed on each micro-batch and:
        - Filters invalid and blank votes
        - Applies the 3% threshold rule
        - Computes quotients and rankings
        - Assigns seats per party and province
        - Persists results to the gold layer
        - Publishes seat updates to Kafka

        Args:
            df_batch (DataFrame): Aggregated votes for the current batch.
            batch_id (int): Identifier of the micro-batch.
        """
        if df_batch.isEmpty():
            return

        # recompute totals by province on the input batch
        window_totals = Window.partitionBy("province_name")
        df_batch_with_totals = df_batch.withColumn(
            "total_votes_province", F.sum("votes_per_party").over(window_totals)
        )

        # filtering 3% threshold and blank votes
        df_valids = df_batch_with_totals.filter(
            (F.col("political_party_norm") != "BLANK")
            & ((F.col("votes_per_party") / F.col("total_votes_province")) >= 0.03)
        ).cache()

        # join with seats (Broadcast)
        df_dhondt = df_valids.join(F.broadcast(self.df_seats_ref), on="province_name")

        # explode: generating dividers
        df_quotients = df_dhondt.withColumn(
            "divisor", F.explode(F.sequence(F.lit(1), F.col("total_seats")))
        ).withColumn("quotient", F.col("votes_per_party") / F.col("divisor"))

        # ranking and selection. Making sure of the 350 limit
        # ordering by quotient, using votes_per_party for breaking the tie
        window_ranking = Window.partitionBy("province_name").orderBy(
            F.desc("quotient"), F.desc("votes_per_party")
        )

        # computing who gets the seat
        df_winners = (
            df_quotients.withColumn("ranking", F.row_number().over(window_ranking))
            .filter(F.col("ranking") <= F.col("total_seats"))
            .groupBy("autonomic_region_name","province_name", "political_party_norm")
            .agg(F.count("*").alias("seats"))
        )

        # we need a list with ALL the political parties who are able to win a seat on the province
        # to set them a 0 if they do not won a seat
        df_candidates = df_valids.select(
            "autonomic_region_name","province_name", "political_party_norm"
        ).distinct()

        # making LEFT JOIN. If it is not on winners, seats will be null -> setting it to 0
        df_final_result = df_candidates.join(
            df_winners, on=["autonomic_region_name","province_name", "political_party_norm"], how="left"
        ).fillna({"seats": 0})

        # saving on GOLD
        (
            df_final_result.write.format("parquet")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(DataLakeConfig.PATH_GOLD_PARTIES_PROVINCES_SEATS)
        )

        # making a specific id in order to make Elasticsearch able to overwrite the seat information.
        # example ID: "Madrid_Perro Liberal"
        df_kafka_output = df_final_result.withColumn(
            "doc_id",
            F.concat_ws("_", F.col("province_name"), F.col("political_party_norm")),
        ).withColumn("updated_at", F.current_timestamp())

        # making JSON and sending
        df_kafka_message = df_kafka_output.select(
            F.col("doc_id").alias("key"),
            F.to_json(
                F.struct(
                    F.col("doc_id"),
                    F.col("province_name"),
                    F.col("political_party_norm").alias("political_party"),
                    F.col("autonomic_region_name"),
                    F.col("seats"),  # able to be 0
                    F.col("updated_at"),
                )
            ).alias("value"),
        )

        df_kafka_message.write.format("kafka").option(
            "kafka.bootstrap.servers", KafkaConfiguration.KAFKA_BROKER_DOCKER
        ).option("topic", KafkaConfiguration.TOPIC_VOTES_SEATS_PROVINCES).save()

        # removing from memory
        df_valids.unpersist()
