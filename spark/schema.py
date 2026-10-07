from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType
)

generic_schema = StructType([
    StructField("entity_id", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("sensor_type", StringType(), False),
    StructField("value", DoubleType(), False),
    StructField("location", StringType(), True)
])
