from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from generation.utils.kafka import KafkaConfiguration
from streaming.schemas.vote_schema import vote_schema

CHECKPOINT_DIR = '/mnt/spark-checkpoints'
STATES = '/mnt/spark-state'


def write_to_elasticsearch(batch_df, batch_id):
    """"""
    es_conf = {
        "es.nodes": "elasticsearch",
        "es.port": "9200",
        "es.nodes.wan.only": "true",
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


spark = (
        SparkSession.builder
        .appName("Testing")
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,org.elasticsearch:elasticsearch-spark-30_2.12:8.13.2')
        .config('spark.sql.streaming.checkpointLocation', CHECKPOINT_DIR)
        .config('spark.sql.streaming.stateStore.stateStoreDir', STATES)
        .config('spark.sql.shuffle.partitions', 20)
    ).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

kafka_stream = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KafkaConfiguration.KAFKA_BROKER_DOCKER)
        .option('subscribe', KafkaConfiguration.TOPIC_NAME)
        .option('startingOffsets', 'earliest')
        .load()
)

_votes_raw_df = (
    kafka_stream.selectExpr("CAST(value AS STRING)")
    .select(F.from_json(F.col('value'), vote_schema).alias("data"))
    .select("data.*")
)


votes_raw_df = (
    _votes_raw_df
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

aggregated_df = (
    _votes_raw_df.withColumn(
    "political_party",
    F.when(F.col("blank_vote") == True, F.lit("BLANK")).otherwise(
        F.col("political_party")
        )
    )
)

_votes_with_id_df = (
    aggregated_df
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


query = (
    _votes_with_id_df
    .writeStream
    .foreachBatch(write_to_elasticsearch)
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/vote_index")
    .start().awaitTermination()
)

# aggregation_query = aggregated_df.withColumn("key", F.col("political_party").cast("string")) \
#     .withColumn("value", F.to_json(F.struct(
#         F.col("political_party"),
#         F.col("location"),
#         F.col("timestamp"),
#         F.col("province_iso_code"),
#         F.col("province_name"),
#         F.col("autonomic_region_iso_code"),
#         F.col("autonomic_region_name")
#     ))).selectExpr("key", "value") \
#         .writeStream \
#         .format('kafka') \
#         .outputMode('update') \
#         .option('kafka.bootstrap.servers', KafkaConfiguration.KAFKA_BROKER_DOCKER) \
#         .option('topic', KafkaConfiguration.AGGREGATED_TOPIC) \
#         .option('checkpointLocation', f'{CHECKPOINT_DIR}/aggregates') \
#         .start().awaitTermination()
