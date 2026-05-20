from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
KAFKA_TOPICS = os.getenv("KAFKA_TOPICS", "ran_telemetry,weather_events")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "towerhealth")
POSTGRES_USER = os.getenv("POSTGRES_USER", "towerhealth")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "towerhealth")
CHECKPOINT_DIR = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/towerhealth-checkpoints")


def postgres_jdbc_options() -> dict[str, str]:
    return {
        "url": f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}",
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
    }


def write_batch_to_postgres(batch_df, batch_id: int) -> None:
    """Persist one Spark micro-batch into Postgres through JDBC."""
    if batch_df.rdd.isEmpty():
        return

    jdbc_options = postgres_jdbc_options()
    existing_offsets = (
        batch_df.sparkSession.read
        .format("jdbc")
        .options(**jdbc_options)
        .option(
            "dbtable",
            "(SELECT topic, kafka_partition, kafka_offset FROM kafka_events) AS existing_offsets",
        )
        .load()
    )
    new_rows = batch_df.join(
        existing_offsets,
        on=["topic", "kafka_partition", "kafka_offset"],
        how="left_anti",
    )

    if new_rows.rdd.isEmpty():
        return

    (
        new_rows.write
        .format("jdbc")
        .options(**jdbc_options)
        .option("dbtable", "kafka_events")
        .mode("append")
        .save()
    )


def main() -> None:
    """Read Kafka as an unbounded stream and store each event in Postgres."""
    spark = (
        SparkSession.builder
        .appName("towerhealth-kafka-to-postgres")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        # startingOffsets=latest means this job processes new messages only.
        # Existing topic history is skipped when the checkpoint is new.
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPICS)
        .option("startingOffsets", "latest")
        .load()
    )

    events_df = kafka_df.select(
        # Keep Kafka metadata beside the raw JSON value so we can audit exactly
        # which topic partition and offset produced each database row.
        col("topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("key").cast("string").alias("message_key"),
        col("value").cast("string").alias("message_value"),
        col("timestamp").alias("event_time"),
        current_timestamp().alias("ingested_at"),
    )

    query = (
        events_df.writeStream
        .foreachBatch(write_batch_to_postgres)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .outputMode("append")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
