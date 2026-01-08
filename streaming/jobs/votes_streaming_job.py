import logging
import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

load_dotenv()
from python.utils.logging_config import setup_logging
from python.votes_generator.vote_generator import VoteConfiguration
from streaming.schemas.vote_schema import vote_schema

setup_logging(logging.INFO)

log = logging.getLogger(__name__)


class DataLakeConfig:
    PATH_BASE = "file:///D:/dev/UCM-BD_DE/votes_manager"

    PATH_BRONZE = f"{PATH_BASE}/datalake/bronze/votes"
    CHECKPOINT_BRONZE = f"{PATH_BASE}/checkpoints/bronze/votes"

    PATH_SILVER = f"{PATH_BASE}/datalake/silver"
    CHECKPOINT_SILVER = f"{PATH_BASE}/checkpoints/silver"

    PATH_SILVER_NORMALIZED = f"{PATH_SILVER}/votes_normalized"
    CHECKPOINT_SILVER_NORMALIZED = f"{CHECKPOINT_SILVER}/votes_normalized"

    PATH_GOLD = f"{PATH_BASE}/datalake/gold"
    CHECKPOINT_GOLD = f"{PATH_BASE}/checkpoints/gold/votes"

    PATH_GOLD_PARTIES_PROVINCES = f"{PATH_GOLD}/votes_parties_provinces"
    CHECKPOINT_GOLD_PARTIES_PROVINCES = f"{CHECKPOINT_GOLD}/votes_parties_provinces"

    PATH_GOLD_PARTIES_REGIONS = f"{PATH_GOLD}/votes_parties_region"
    CHECKPOINT_GOLD_PARTIES_REGIONS = f"{CHECKPOINT_GOLD}/votes_parties_region"

    PATH_GOLD_TOTAL_PARTIES = f"{PATH_GOLD}/votes_total_party"
    CHECKPOINT_GOLD_TOTAL_PARTIES = f"{CHECKPOINT_GOLD}/votes_total_party"


class StreamingJob:

    def __init__(self):
        """"""
        self.spark = self._create_spark_session()
        self.spark.sparkContext.setLogLevel("WARN")


    def _create_spark_session(self):
        """"""

        kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_{os.getenv('SCALA_VERSION')}:{os.getenv('SPARK_VERSION')}"
        es_package = f"org.elasticsearch:elasticsearch-spark-30_{os.getenv('SCALA_VERSION')}:{os.getenv('ELASTIC_VERSION')}"
        return (
            SparkSession.builder.appName("VotesManager")
            .config(
                "spark.jars.packages",
                f"{kafka_package},{es_package}",
            )
            .getOrCreate()
        )

    def execute_job(self):
        """"""
        log.info("[StreamingJob]: Executing job")
        # getting raw data from kafka
        raw_df = self._read_votes_from_kafka()

        ##  BRONZE
        bronze_df = self._parse_votes(raw_df)

        bronze_writing = self._write_on_layer(
            bronze_df,
            DataLakeConfig.PATH_BRONZE,
            DataLakeConfig.CHECKPOINT_BRONZE,
            output_mode="append",
        )

        ## SILVER
        silver_reading = (
            self.spark.readStream.schema(vote_schema).format("parquet").load(DataLakeConfig.PATH_BRONZE)
        )

        silver_reading_df = (
            silver_reading
            .withWatermark("timestamp", "2 minutes")
            .dropDuplicates(["id"])
            .select(
                "id",
                "timestamp",
                "political_party",
                "province_name",
                "autonomic_region_name",
                "blank_vote",
                "location",
                "province_iso_code",
                "autonomic_region_iso_code"
            )
        )

        # adapting blank votes
        normalized_blank_votes = self._normalize_blank_votes(silver_reading_df)

        silver_writing = self._write_on_layer(
            normalized_blank_votes,
            DataLakeConfig.PATH_SILVER_NORMALIZED,
            DataLakeConfig.CHECKPOINT_SILVER_NORMALIZED,
            output_mode="append",
        )

        ## GOLD
        gold_reading = (
            self.spark.readStream.schema(vote_schema).format("parquet").load(DataLakeConfig.PATH_SILVER_NORMALIZED)
        )

        _votes_with_id_df = (
            gold_reading
            .withColumn(
            "doc_id",
                F.sha2(F.concat_ws(
                    "||",
                    "political_party",
                    "province_name",
                    "autonomic_region_name"
                ), 256)
            )
        )

        votes_with_id_df = _votes_with_id_df.dropDuplicates(["doc_id"])

        gold_writing = (
            votes_with_id_df
            .writeStream
            .foreachBatch(self._write_to_elasticsearch)
            .outputMode("append")
            .option("checkpointLocation", DataLakeConfig.CHECKPOINT_GOLD)
            .start()
        )

        # writing on gold and ES
        # gold_writing = (
        #     votes_parties_provinces
        #     .writeStream
        #     .foreachBatch(self.write_gold_all)
        #     .outputMode("complete")
        #     .option("checkpointLocation", DataLakeConfig.CHECKPOINT_GOLD)
        #     .start()
        # )
        #

        # ending queries
        bronze_writing.awaitTermination()
        silver_writing.awaitTermination()
        gold_writing.awaitTermination()



    def _normalize_blank_votes(self, df):
        """"""
        return df.withColumn(
            "political_party",
            F.when(F.col("blank_vote") == True, F.lit("BLANK")).otherwise(
                F.col("political_party")
            ),
        )

    def _debug_console_df(self, df):
        """"""
        # DEBUG: escribir en consola
        query = (
            df.writeStream.outputMode("append")
            .format("console")
            .option("truncate", False)
            .start()
        )

        query.awaitTermination()

    def _read_votes_from_kafka(self):
        """"""
        return (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", VoteConfiguration.TOPIC_NAME)
            .option("startingOffsets", "earliest") # repasar valor
            .option("failOnDataLoss", "false")
            .load()
        )

    def _parse_votes(self, df):
        """"""
        return (
            df.selectExpr("CAST(value AS STRING) as json")
            .select(F.from_json(F.col("json"), vote_schema).alias("data"))
            .select("data.*")
        )


    def _write_on_layer(self, df, layer_path, checkpoint_path, output_mode="append"):
        """"""
        return (
            df
            .writeStream
            .format("parquet")
            .option("path", layer_path)
            .option("checkpointLocation", checkpoint_path)
            .outputMode(output_mode)
            .start()
        )

    def _write_to_elasticsearch(self, batch_df, batch_id):
        """"""
        log.info(f"[StreamingJob]: Writing Gold Batch ID: {batch_id} - Count: {batch_df.count()}")

        es_conf = {
            "es.nodes": "localhost",
            "es.port": "9200",
            "es.nodes.wan.only": "true", # when set to True, Spark treats the listed es.nodes as standalone and avoids node discovery
            "es.index.auto.create": "true"
        }

        (
            batch_df.write
             .format("org.elasticsearch.spark.sql")
             .mode("append")
             .options(**es_conf)
             .option("es.resource", "votes_index")
             .option("es.mapping.id", "doc_id")
             .option("es.write.operation", "index")
             .save()
        )


    def _write_gold_and_es(self, index_name, gold_path):
        def _write(batch_df, batch_id):
            (
                batch_df
                .write
                .mode("overwrite")
                .parquet(f"{gold_path}")
            )
            (
                batch_df
                .write
                .format("org.elasticsearch.spark.sql")
                .mode("overwrite")
                .option("es.resource", index_name)
                .option("es.nodes", "localhost")
                .option("es.port", "9200")
                .option("es.mapping.id", "doc_id")
                .save()
            )

        return _write


    def write_gold_all(self, batch_df, batch_id):
        """
        Esta función se ejecuta por cada micro-batch.
        Batch_df ya viene agregado por Partido/Provincia/Region desde la query principal.
        """
        log.info(f"[StreamingJob]: Writing Gold Batch ID: {batch_id} - Count: {batch_df.count()}")

        # Cachear el DF porque lo vas a usar 4 veces (1 parquet + 3 ES)
        batch_df.persist()

        # 1. Guardar histórico en Parquet (Overwrite sobre la ruta base puede ser peligroso en streaming si no particionas, pero para TFM vale)
        batch_df.write \
            .mode("overwrite") \
            .parquet(DataLakeConfig.PATH_GOLD_PARTIES_PROVINCES)

        # Configuración común de ES
        es_conf = {
            "es.nodes": "localhost",
            "es.port": "9200",
            "es.nodes.wan.only": "true",  # CRÍTICO para Docker vs Localhost
            "es.index.auto.create": "true"
        }

        # NOTA: Si tu Elastic tiene seguridad (usuario/pass) actívala aquí:
        # es_conf["es.net.http.auth.user"] = "elastic"
        # es_conf["es.net.http.auth.pass"] = "changeme"

        # 2. ES: Nivel Provincia
        (batch_df.write
         .format("org.elasticsearch.spark.sql")
         .mode("append")  # 'Overwrite' en ES borra el índice completo o falla. Usar 'Append' con IDs para upsert.
         .options(**es_conf)
         .option("es.resource", "votes_party_province")
         .option("es.mapping.id", "doc_id")  # Usar el ID generado para evitar duplicados (Upsert)
         .option("es.write.operation", "upsert")
         .save())

        # 3. ES: Agregación Nivel Región
        votes_party_region = (
            batch_df
            .groupBy("political_party", "autonomic_region_name")
            .agg(F.sum("count").alias("total_votes"))
            # Generar ID determinista para poder hacer upsert en ES
            .withColumn("region_id", F.sha2(F.concat_ws("||", "political_party", "autonomic_region_name"), 256))
        )

        (votes_party_region.write
         .format("org.elasticsearch.spark.sql")
         .mode("append")
         .options(**es_conf)
         .option("es.resource", "votes_party_region")
         .option("es.mapping.id", "region_id")
         .option("es.write.operation", "upsert")
         .save())

        # 4. ES: Agregación Nivel Nacional (Total Partido)
        votes_party_total = (
            batch_df
            .groupBy("political_party")
            .agg(F.sum("count").alias("total_votes"))
            .withColumn("party_id", F.sha2("political_party", 256))
        )

        (votes_party_total.write
         .format("org.elasticsearch.spark.sql")
         .mode("append")
         .options(**es_conf)
         .option("es.resource", "votes_party_total")
         .option("es.mapping.id", "party_id")
         .option("es.write.operation", "upsert")
         .save())

        batch_df.unpersist()
