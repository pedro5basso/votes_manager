from pyspark.sql.types import (BooleanType, StringType, StructField,
                               StructType, TimestampType)

# schema for raw votes
vote_schema = StructType(
    [
        StructField("id", StringType(), False),
        StructField("blank_vote", BooleanType(), False),
        StructField("political_party", StringType(), True),
        StructField("province_name", StringType(), False),
        StructField("province_iso_code", StringType(), False),
        StructField("autonomic_region_name", StringType(), False),
        StructField("autonomic_region_iso_code", StringType(), False),
        StructField("location", StringType(), False),
        StructField("timestamp", TimestampType(), False),
    ]
)

# schema for normalized votes
vote_schema_norm = StructType(
    [
        StructField("id", StringType(), False),
        StructField("blank_vote", BooleanType(), False),
        StructField("political_party_norm", StringType(), True),
        StructField("province_name", StringType(), False),
        StructField("province_iso_code", StringType(), False),
        StructField("autonomic_region_name", StringType(), False),
        StructField("autonomic_region_iso_code", StringType(), False),
        StructField("location", StringType(), False),
        StructField("timestamp", TimestampType(), False),
    ]
)
