import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# 1. الإعدادات
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
POSTGRES_DB = "towerhealth"

# 2. الـ Schema (بما فيها الـ precip_mm للمطر)
# عدلي الـ Schema عشان تطابق الـ Fields اللي طالعة من الكود

weather_schema = StructType([
    StructField("event_timestamp", StringType()),
    StructField("source_system", StringType()),
    StructField("location_query", StringType()),
    StructField("ran_site_id", StringType()),
    StructField("ran_site_name", StringType()),
    StructField("ran_region", StringType()),
    StructField("weather_temperature_c", DoubleType()),
    StructField("weather_humidity_pct", IntegerType()),
    StructField("weather_rainfall_mm", DoubleType()),
    StructField("weather_wind_speed_kmh", DoubleType()),
    StructField("weather_condition", StringType()),
    StructField("weather_observed_at", StringType()),
    StructField("weather_fetched_at", StringType()),
    StructField("weather_location_name", StringType()),
    StructField("weather_region", StringType()),
    StructField("weather_country", StringType()),
    StructField("weather_latitude", DoubleType()),
    StructField("weather_longitude", DoubleType())
])

def write_weather_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty(): return
    batch_df.write.format("jdbc") \
        .option("url", f"jdbc:postgresql://postgres:5432/{POSTGRES_DB}") \
        .option("user", "towerhealth") \
        .option("password", "towerhealth") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "weather_metrics") \
        .mode("append").save()

def main():
    # نفس الإعدادات اللي خلت الـ RAN يشتغل Smooth
    spark = SparkSession.builder \
        .appName("TowerHealth-Weather-Stream") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .config("spark.executor.memory", "512m") \
        .config("spark.cores.max", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 3. القراءة من كافكا
    weather_raw = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", "weather_events") \
        .option("startingOffsets", "earliest").load()

    # 4. فك الـ JSON والـ Transformations
    weather_parsed = weather_raw.select(
        from_json(col("value").cast("string"), weather_schema).alias("data")
    ).select("data.*")

    # التعديل هنا على أسماء الأعمدة
    weather_final = weather_parsed.withColumn(
        "is_raining", when(col("weather_rainfall_mm") > 0, True).otherwise(False)
    ).withColumn(
        "rain_intensity",
        when(col("weather_rainfall_mm") == 0, "None")
        .when(col("weather_rainfall_mm") < 2.5, "Light")
        .when(col("weather_rainfall_mm") < 7.6, "Moderate")
        .otherwise("Heavy")
    ).withColumn("ingested_at", current_timestamp())

    # 5. الـ Sink مع التريجر الهادي (30 ثانية)
    query = weather_final.writeStream \
        .foreachBatch(write_weather_batch) \
        .option("checkpointLocation", "/tmp/spark_checkpoints/weather_final_v1") \
        .trigger(processingTime='30 seconds') \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()