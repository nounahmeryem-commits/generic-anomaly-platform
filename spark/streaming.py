from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

from schema import generic_schema


# --------------------------------------------------
# 1. Création de la session Spark
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("GenericAnomalyStreaming")
    .getOrCreate()
)


# --------------------------------------------------
# 2. Lecture des données depuis Kafka
# --------------------------------------------------

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "sensor-data")
    .option("startingOffsets", "earliest")
    .load()
)


# --------------------------------------------------
# 3. Conversion du message Kafka en JSON
# --------------------------------------------------

messages = df.selectExpr(
    "CAST(value AS STRING) AS json_data"
)


# --------------------------------------------------
# 4. Application du schéma générique
# --------------------------------------------------

data = (
    messages
    .select(
        from_json(
            col("json_data"),
            generic_schema
        ).alias("data")
    )
    .select("data.*")
)


# --------------------------------------------------
# 5. Fonction d'écriture vers MongoDB
# --------------------------------------------------

def write_to_mongodb(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    (
        batch_df.write
        .format("mongodb")
        .mode("append")
        .option(
            "spark.mongodb.write.connection.uri",
            "mongodb://mongodb:27017"
        )
        .option(
            "spark.mongodb.write.database",
            "anomaly_platform"
        )
        .option(
            "spark.mongodb.write.collection",
            "sensor_data"
        )
        .save()
    )


# --------------------------------------------------
# 6. Streaming vers MongoDB
# --------------------------------------------------

query = (
    data.writeStream
    .foreachBatch(write_to_mongodb)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/tmp/generic-anomaly-mongodb-checkpoint"
    )
    .start()
)


# --------------------------------------------------
# 7. Attendre la fin du streaming
# --------------------------------------------------

query.awaitTermination()
