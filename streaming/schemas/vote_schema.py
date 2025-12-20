from pyspark.sql.types import (
    StructType, StructField,
    StringType, BooleanType, TimestampType
)

vote_schema = StructType([
    StructField("id", StringType(), False),
    StructField("blank_vote", BooleanType(), False),
    StructField("political_party", StringType(), True),
    StructField("province_code", StringType(), False),
    StructField("province_name", StringType(), False),
    StructField("timestamp", TimestampType(), False)
])
