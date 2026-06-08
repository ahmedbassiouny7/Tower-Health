import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, explode, when, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, IntegerType

# 1. الإعدادات
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
KAFKA_TOPICS = os.getenv("KAFKA_TOPICS", "ran_telemetry")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "towerhealth")
POSTGRES_USER = os.getenv("POSTGRES_USER", "towerhealth")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "towerhealth")
RAN_CHECKPOINT_DIR = os.getenv("SPARK_RAN_CHECKPOINT_DIR", "/tmp/spark_checkpoints/ran_final_v6")
TRANSPORT_CHECKPOINT_DIR = os.getenv("SPARK_TRANSPORT_CHECKPOINT_DIR", "/tmp/spark_checkpoints/transport_final_v1")

# 2. الـ Schema
schema = StructType([
    StructField("message_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("ran_metadata", StructType([
        StructField("site_id", StringType()),
        StructField("site_name", StringType()),
        StructField("location", StructType([
            StructField("latitude", DoubleType()),
            StructField("longitude", DoubleType())
        ])),
        StructField("vendor", StringType())
    ])),
    StructField("cells", ArrayType(StructType([
        StructField("cell_id", StringType()),
        StructField("technology", StringType()),
        StructField("status", StringType()),
        StructField("active_users", IntegerType()),
        StructField("throughput_downlink_mbps", DoubleType()),
        StructField("rsrp_dbm", DoubleType()),
        StructField("rsrq_db", DoubleType()),
        StructField("sinr_db", DoubleType()),
        StructField("cqi_avg", DoubleType()),
        StructField("handover_success_rate_percent", DoubleType())
    ]))),
    StructField("radio_units", ArrayType(StructType([
        StructField("temperature_c", DoubleType())
    ]))),
    StructField("power_system", StructType([
        StructField("batteries", ArrayType(StructType([
            StructField("status", StringType()),
            StructField("charge_percent", DoubleType())
        ])))
    ])),
    StructField("transport_links", ArrayType(StructType([
        StructField("link_id", StringType()),
        StructField("type", StringType()),
        StructField("status", StringType()),
        StructField("op_state", StringType()),
        StructField("latency_ms", DoubleType()),
        StructField("jitter_ms", DoubleType()),
        StructField("packet_loss_percent", DoubleType()),
        StructField("throughput_mbps", DoubleType()),
        StructField("utilization_percent", DoubleType())
    ]))),
    StructField("alert_summary", StructType([
        StructField("highest_severity", StringType())
    ]))
])

def _jdbc_write(batch_df, table_name):
    batch_df.write.format("jdbc") \
        .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", table_name) \
        .mode("append").save()

def write_ran_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    _jdbc_write(batch_df, "processed_ran_metrics")

def write_transport_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    _jdbc_write(batch_df, "transport_metrics")

def main():
    # 1. إنشاء الـ Session أولاً
    spark = SparkSession.builder \
        .appName("TowerHealth-RAN") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .config("spark.executor.memory", "2g") \
        .config("spark.cores.max", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 2. القراءة من كافكا
    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPICS) \
        .option("maxOffsetsPerTrigger", 1000) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false").load()

    # 3. فك الـ JSON
    parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # 4. الـ Enrichment
    enriched_df = parsed_df.select(
        "timestamp", "message_id", "ran_metadata", "alert_summary",
        expr("aggregate(radio_units, CAST(0.0 AS DOUBLE), (acc, x) -> acc + CAST(x.temperature_c AS DOUBLE)) / size(radio_units)").alias("avg_ru_temp"),
        col("power_system.batteries")[0].getField("charge_percent").alias("battery_charge"),
        col("power_system.batteries")[0].getField("status").alias("battery_status"),
        col("cells").alias("cells_array")
    )
    
    # 5. الـ Explode
    exploded_df = enriched_df.withColumn("cell", explode(col("cells_array")))

    # 6. الـ Final Select
    final_df = exploded_df.select(
        "timestamp", "message_id",
        col("ran_metadata.site_id").alias("site_id"),
        col("ran_metadata.site_name").alias("site_name"),
        col("ran_metadata.vendor").alias("vendor"),
        col("ran_metadata.location.latitude").alias("lat"),
        col("ran_metadata.location.longitude").alias("lon"),
        col("alert_summary.highest_severity").alias("alert_severity"),
        "avg_ru_temp", "battery_charge", "battery_status",
        col("cell.cell_id").alias("cell_id"),
        col("cell.technology").alias("tech"),
        col("cell.status").alias("cell_status"),
        col("cell.active_users").alias("users"),
        col("cell.throughput_downlink_mbps").alias("downlink_mbps"),
        col("cell.rsrp_dbm").alias("rsrp"),
        col("cell.rsrq_db").alias("rsrq"),
        col("cell.sinr_db").alias("sinr"),
        col("cell.cqi_avg").alias("cqi"),
        col("cell.handover_success_rate_percent").alias("ho_success_rate")
    ).withColumn(
        "signal_quality",
        when(col("rsrp") > -80, "Excellent").when(col("rsrp") > -95, "Good").otherwise("Poor")
    ).withColumn("ingested_at", current_timestamp())

    # 7. Transport links — سطر لكل link
    transport_df = parsed_df.select(
        "timestamp", "message_id",
        col("ran_metadata.site_id").alias("site_id"),
        col("ran_metadata.site_name").alias("site_name"),
        explode(col("transport_links")).alias("link")
    ).select(
        "timestamp", "message_id", "site_id", "site_name",
        col("link.link_id").alias("link_id"),
        col("link.type").alias("link_type"),
        col("link.status").alias("link_status"),
        col("link.latency_ms").alias("latency_ms"),
        col("link.jitter_ms").alias("jitter_ms"),
        col("link.packet_loss_percent").alias("packet_loss_percent"),
        col("link.throughput_mbps").alias("throughput_mbps"),
        col("link.utilization_percent").alias("utilization_percent"),
    ).withColumn(
        "severity",
        when(col("utilization_percent") > 95, "CRITICAL")
        .when(col("packet_loss_percent") > 1, "CRITICAL")
        .when(col("latency_ms") > 50, "WARNING")
        .otherwise("NORMAL")
    ).withColumn(
        "link_quality_score",
        100 - (col("latency_ms") * 1.5) - (col("jitter_ms") * 2) - (col("packet_loss_percent") * 20)
    ).withColumn("ingested_at", current_timestamp())

    # 8. الـ Sinks
    ran_query = final_df.writeStream \
        .foreachBatch(write_ran_batch) \
        .option("checkpointLocation", RAN_CHECKPOINT_DIR) \
        .trigger(processingTime="30 seconds") \
        .start()

    transport_query = transport_df.writeStream \
        .foreachBatch(write_transport_batch) \
        .option("checkpointLocation", TRANSPORT_CHECKPOINT_DIR) \
        .trigger(processingTime="30 seconds") \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()



# from __future__ import annotations
# import os
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, current_timestamp, from_json, explode, when, concat, lit
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType
# # 1. الإعدادات (نفس اللي في صورتك بالظبط)
# KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
# KAFKA_TOPICS = os.getenv("KAFKA_TOPICS", "ran_telemetry") # ركزي على RAN حالياً
# POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
# POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
# POSTGRES_DB = os.getenv("POSTGRES_DB", "towerhealth")
# POSTGRES_USER = os.getenv("POSTGRES_USER", "towerhealth")
# POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "towerhealth")
# CHECKPOINT_DIR = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/towerhealth-checkpoints")

# # 2. تعريف الـ Schema لفك الـ JSON المعقد (الـ Nested JSON)
# transport_schema = StructType([
#     StructField("link_id", StringType(), True),
#     StructField("type", StringType(), True),
#     StructField("status", StringType(), True),
#     StructField("op_state", StringType(), True),
#     StructField("latency_ms", DoubleType(), True),
#     StructField("jitter_ms", DoubleType(), True),
#     StructField("packet_loss_percent", DoubleType(), True),
#     StructField("throughput_mbps", DoubleType(), True),
#     StructField("utilization_percent", DoubleType(), True),
# ])

# ran_schema = StructType([
#     StructField("timestamp", StringType(), True),
#     StructField("ran_metadata", StructType([StructField("site_id", StringType(), True)]), True),
#     StructField("transport_links", ArrayType(transport_schema), True)
# ])

# def postgres_jdbc_options() -> dict[str, str]:
#     return {
#         "url": f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}",
#         "user": POSTGRES_USER,
#         "password": POSTGRES_PASSWORD,
#         "driver": "org.postgresql.Driver",
#     }

# def write_batch_to_postgres(batch_df, batch_id: int) -> None:
#     """كتابة كل Batch لجدول التحليلات في Postgres."""
#     if batch_df.rdd.isEmpty():
#         return
#     jdbc_options = postgres_jdbc_options()
#     (
#         batch_df.write
#         .format("jdbc")
#         .options(**jdbc_options)
#         .option("dbtable", "transport_metrics") # ده الجدول اللي الداشبورد هيقرأ منه
#         .mode("append")
#         .save()
#     )

# def main() -> None:
#     spark = SparkSession.builder \
#         .appName("towerhealth-transformation-job") \
#         .getOrCreate()
    
#     spark.sparkContext.setLogLevel("WARN")

#     # 3. القراءة من Kafka Stream
#     kafka_df = spark.readStream \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
#         .option("subscribe", KAFKA_TOPICS) \
#         .option("startingOffsets", "earliest") \
#         .option("failOnDataLoss", "false") \
#         .load()

#     # 4. فك الـ JSON وتطبيق منطق الـ Explode (شغل زميلتك)
#     parsed_df = kafka_df.select(
#         from_json(col("value").cast("string"), ran_schema).alias("data")
#     ).select("data.*")

#     # تحويل كل وصلة نقل (Link) لسطر منفصل عشان نعرف نحللها
#     exploded_df = parsed_df.select(
#         col("timestamp"),
#         col("ran_metadata.site_id").alias("site_id"),
#         explode(col("transport_links")).alias("transport")
#     )

#     # 5. حساب الـ KPIs والـ Severity والـ Quality Score
#     final_df = exploded_df.select(
#         "timestamp", "site_id",
#         col("transport.link_id").alias("link_id"),
#         col("transport.latency_ms").alias("latency_ms"),
#         col("transport.jitter_ms").alias("jitter_ms"),
#         col("transport.packet_loss_percent").alias("packet_loss_percent"),
#         col("transport.utilization_percent").alias("utilization_percent")
#     ).withColumn(
#         "severity",
#         when(col("utilization_percent") > 95, "CRITICAL")
#         .when(col("packet_loss_percent") > 1, "CRITICAL")
#         .when(col("latency_ms") > 50, "WARNING")
#         .otherwise("NORMAL")
#     ).withColumn(
#         "link_quality_score",
#         100 - (col("latency_ms") * 1.5) - (col("jitter_ms") * 2) - (col("packet_loss_percent") * 20)
#     ).withColumn(
#         "alert_message",
#         concat(lit("Network issue at site "), col("site_id"), lit(" on link "), col("link_id"))
#     ).withColumn("ingested_at", current_timestamp())

#     # 6. التشغيل (The Sink)
#     query = final_df.writeStream \
#         .foreachBatch(write_batch_to_postgres) \
#         .option("checkpointLocation", CHECKPOINT_DIR) \
#         .outputMode("append") \
#         .start()

#     query.awaitTermination()

# if __name__ == "__main__":
#     main()
