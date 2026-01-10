from pyspark.sql.types import (
    StructType, StructField,
    StringType, BooleanType, TimestampType
)

vote_schema = StructType([
    StructField("id", StringType(), False),
    StructField("blank_vote", BooleanType(), False),
    StructField("political_party", StringType(), True),
    StructField("province_name", StringType(), False),
    StructField("province_iso_code", StringType(), False),
    StructField("autonomic_region_name", StringType(), False),
    StructField("autonomic_region_iso_code", StringType(), False),
    StructField("location",StringType(),False),
    StructField("timestamp", TimestampType(), False)
])
