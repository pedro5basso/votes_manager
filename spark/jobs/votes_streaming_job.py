from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json
)


from spark.schemas.vote_schema import vote_schema

def create_spark_session():
    return (
        SparkSession.builder
        .appName("VotesManager")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.6"
        )
        .getOrCreate()
    )


def read_votes_from_kafka(spark):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "votes_raw")
        .option("startingOffsets", "latest")
        .load()
    )


def parse_votes(df):
    return (
        df.selectExpr("CAST(value AS STRING) as json")
          .select(from_json(col("json"), vote_schema).alias("data"))
          .select("data.*")
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("INFO")

    raw_df = read_votes_from_kafka(spark)
    votes_df = parse_votes(raw_df)

    # DEBUG: escribir en consola
    query = (
        votes_df.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
