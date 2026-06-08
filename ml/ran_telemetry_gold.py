import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import monotonically_increasing_id

# ---------------------------------------------------------------------------
# 1. Spark setup
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("Telecom_RAN_Gold_Layer_Pipeline")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 2. Paths and IO helpers
# ---------------------------------------------------------------------------
S3_SILVER_BASE = "s3a://tower-iti-project/silver/ran_telemetry_normalized/"
S3_GOLD_BASE = "s3a://tower-iti-project/gold/ran_telemetry_bi/"


def get_silver_path(table_name):
    return f"{S3_SILVER_BASE}{table_name}/"


def write_to_gold(df, table_name, partition_cols=None):
    path = f"{S3_GOLD_BASE}{table_name}/"
    writer = df.write.mode("overwrite").format("parquet")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(path)
    print(f"Successfully written {table_name} to Gold Layer: {path}")


print("Loading Silver Tables from S3...")
df_site_snapshot = spark.read.parquet(get_silver_path("site_snapshot"))
df_cells = spark.read.parquet(get_silver_path("cells"))
df_antennas = spark.read.parquet(get_silver_path("antennas"))
df_radio_units = spark.read.parquet(get_silver_path("radio_units"))
df_baseband_units = spark.read.parquet(get_silver_path("baseband_units"))
df_transport_links = spark.read.parquet(get_silver_path("transport_links"))
df_alerts = spark.read.parquet(get_silver_path("alerts"))
df_batteries = spark.read.parquet(get_silver_path("batteries"))
df_rectifiers = spark.read.parquet(get_silver_path("rectifiers"))
df_environment_sensors = spark.read.parquet(get_silver_path("environment_sensors"))


def add_time_buckets(df):
    return (
        df.withColumn("window_bucket", F.window(F.col("snapshot_time"), "15 minutes"))
        .withColumn("gold_snapshot_time", F.col("window_bucket.start"))
        .withColumn("gold_date", F.to_date(F.col("gold_snapshot_time")))
    )


def get_last_status_window(partition_keys):
    return Window.partitionBy(*partition_keys, "gold_snapshot_time").orderBy(
        F.col("sequence_number").desc()
    )
# --- Dim Site ---
df_dim_site = df_site_snapshot.select(
    "site_id",
    "site_name",
    "region",
    "vendor",
    "latitude",
    "longitude"
).distinct() \
    .withColumn("site_sk", F.md5(F.col("site_id"))) \
    .select(
        "site_sk",
        "site_id",
        "site_name",
        "region",
        "vendor",
        "latitude",
        "longitude"
    )

print("\n--- [SHOWING SAMPLE] dim_site Output Data with All Coordinates & Vendor ---")
df_dim_site.show(truncate=False)
write_to_gold(df_dim_site, "dim_site")

# ---  DIM_CELL ---
df_dim_cell = df_cells.select(
    "cell_id",
    "site_id",
    "technology",
    "bandwidth_mhz",
    "carrier_frequency_mhz"
).distinct() \
    .withColumn("cell_sk", F.md5(F.concat_ws("|", F.col("site_id"), F.col("cell_id")))) \
    .select(
        "cell_sk",
        "cell_id",
        "site_id",
        "technology",
        "bandwidth_mhz",
        "carrier_frequency_mhz"
    )

print("--- [SHOWING DATA] dim_cell Output Sample ---")
df_dim_cell.show(truncate=False)


write_to_gold(df_dim_cell, "dim_cell")

# --- Dim Date ---
# Generate a full date range independent of silver data
from pyspark.sql.functions import sequence, explode, to_date, lit

date_range = spark.sql("""
    SELECT explode(sequence(
        to_date('2026-01-01'),
        to_date('2027-12-31'),
        interval 1 day
    )) AS ts
""")

df_dim_date = date_range.select(
    F.date_format("ts", "yyyyMMdd").cast("int").alias("date_sk"),
    F.to_date("ts").alias("Date"),
    F.date_format("ts", "EEEE").alias("DaySuffix"),
    F.year("ts").alias("Year"),
    F.date_format("ts", "EEE").alias("DayOfWeek"),
    F.dayofmonth("ts").alias("DayOfMonth"),
    F.weekofyear("ts").alias("WeekOfYear"),
    F.month("ts").alias("WeekOfMonth"),
    F.month("ts").alias("Month"),
    F.date_format("ts", "MMMM").alias("MonthName"),
    F.quarter("ts").alias("quarter"),
    F.concat(F.lit("Q"), F.quarter("ts")).alias("quarterName"),
    F.date_format(F.to_date("ts"), "yyyy-MM-dd").alias("standardDate")
).dropDuplicates(["date_sk"])

print("--- [SHOWING DATA] dim_date Output Sample ---")
df_dim_date.show(5, truncate=False)
print(f"dim_date total rows: {df_dim_date.count():,}")

write_to_gold(df_dim_date, "dim_date")

# --- Dim Time ---

# Generate full day at 30-second intervals
df_dim_datetime = spark.sql("""
    SELECT explode(sequence(
        to_timestamp('1970-01-01 00:00:00'),
        to_timestamp('1970-01-01 23:59:30'),
        interval 30 seconds
    )) AS ts
""")

df_dim_datetime = df_dim_datetime.select(
    # Primary Key (unique per 30 sec slot)
    F.date_format("ts", "HHmmss").cast("int").alias("datetime_sk"),

    # Full timestamp label
    F.date_format("ts", "HH:mm:ss").alias("time_label"),

    # Components
    F.hour("ts").alias("hour"),
    F.minute("ts").alias("minute"),
    F.second("ts").alias("second"),

    # Day segmentation
    F.when(F.hour("ts") < 6, "Night")
     .when(F.hour("ts") < 12, "Morning")
     .when(F.hour("ts") < 18, "Afternoon")
     .otherwise("Evening").alias("day_part"),

    # Business hours flag
    F.when((F.hour("ts") >= 8) & (F.hour("ts") < 18),
           "Business Hours").otherwise("Off Hours").alias("business_hours"),

    # Optional useful buckets (VERY important for BI)
    (F.hour("ts") * 120 + F.minute("ts") * 2 + (F.second("ts") / 30)).cast("int").alias("slot_30s_index")
).dropDuplicates(["datetime_sk"])

print("--- [SHOWING DATA] dim_datetime sample ---")
df_dim_datetime.show(10, truncate=False)

print(f"dim_datetime total rows: {df_dim_datetime.count():,}")
# should be 2880 rows (24h * 60min * 2)

write_to_gold(df_dim_datetime, "dim_time")

# --- Dim RU (Radio Units) ---
df_dim_ru = df_radio_units.select(
    "ru_id",
    "sector_id",
).distinct() \
    .withColumn("RU_sk", F.md5(F.col("ru_id"))) \
    .select(
        "RU_sk",
        "ru_id",
        "sector_id",
    )

print("--- [SHOWING DATA] dim_RU Output Sample ---")
df_dim_ru.show(truncate=False)

write_to_gold(df_dim_ru, "dim_RU")


# --- Dim Antenna ---
df_dim_antenna = df_antennas.select(
    "antenna_id",
    "sector_id",
    "mimo_layers",
    "azimuth_degree",
    "tilt_degree"
).distinct() \
    .withColumn("antenna_sk", F.md5(F.col("antenna_id"))) \
    .select(
        "antenna_sk",
        "antenna_id",
        "sector_id",
        "mimo_layers",
        "azimuth_degree",
        "tilt_degree",

    )

print("--- [SHOWING DATA] dim_Antenna Output Sample ---")
df_dim_antenna.show(truncate=False)

write_to_gold(df_dim_antenna, "dim_Antenna")


# --- Dim Link ---
df_dim_link = df_transport_links.select(
    "link_id",
    "link_type"
).distinct() \
    .withColumn("link_sk", F.md5(F.col("link_id"))) \
    .select(
        "link_sk",
        "link_id",
        "link_type"
    )

print("--- [SHOWING DATA] dim_Link Output Sample ---")
df_dim_link.show(truncate=False)

write_to_gold(df_dim_link, "dim_Link")


# ==========================================
# 5a. FACT_RAN
# Aggregates per (site_id, 15-min window)
# Sources: radio_units, baseband_units, antennas,
#          transport_links, batteries, rectifiers,
#          site_snapshot, environment_sensors
# ==========================================
print("\n" + "="*50)
print("Processing Fact_RAN")
print("="*50)

from pyspark.sql import Window

def mode_of(df, group_cols, col_name):
    """
    Returns df with a new column  mode_{col_name}  = most frequent value
    of col_name within each group defined by group_cols.
    col_name must already exist and be uniquely named in df.
    """
    w_count = Window.partitionBy(group_cols + [col_name])
    w_rank  = Window.partitionBy(group_cols).orderBy(F.col(f"_cnt_{col_name}").desc())
    w_prop  = Window.partitionBy(group_cols)
    return (
        df
        .withColumn(f"_cnt_{col_name}", F.count(col_name).over(w_count))
        .withColumn(f"_rnk_{col_name}", F.row_number().over(w_rank))
        .withColumn(f"mode_{col_name}",
            F.max(F.when(F.col(f"_rnk_{col_name}") == 1, F.col(col_name))).over(w_prop))
        .drop(f"_cnt_{col_name}", f"_rnk_{col_name}")
    )

SITE_WIN = ["site_id", "gold_snapshot_time"]

# 1. RADIO UNITS
# silver cols: ru_id, sector_id, status, op_state,
#              tx_power_watts, rx_signal_dbm, voltage_volt,
#              temperature_c, throughput_mbps, packet_error_rate, vswr
df_ru = (
    add_time_buckets(df_radio_units)
    .withColumnRenamed("status",   "ru_status")
    .withColumnRenamed("op_state", "ru_op_state")
)
df_ru = mode_of(df_ru, SITE_WIN + ["ru_id"], "ru_status")
df_ru = mode_of(df_ru, SITE_WIN + ["ru_id"], "ru_op_state")

w_ru  = Window.partitionBy("site_id", "gold_snapshot_time").orderBy("ru_id")
df_ru = df_ru.withColumn("ru_slot", F.dense_rank().over(w_ru))

ru_exprs = []
for slot in [1, 2, 3]:
    s = str(slot)
    ru_exprs += [
        F.max(F.when(F.col("ru_slot") == slot, F.col("ru_id")            )).alias(f"RU{s}_Sk"),
        F.max(F.when(F.col("ru_slot") == slot, F.col("sector_id")        )).alias(f"RU{s}_sector_key"),
        F.max(F.when(F.col("ru_slot") == slot, F.col("mode_ru_status")   )).alias(f"RU{s}_status"),
        F.max(F.when(F.col("ru_slot") == slot, F.col("mode_ru_op_state") )).alias(f"RU{s}_op_state"),
        F.round(F.avg(F.when(F.col("ru_slot") == slot, F.col("temperature_c")    )), 2).alias(f"RU{s}_temperature_c"),
        F.round(F.avg(F.when(F.col("ru_slot") == slot, F.col("tx_power_watts")   )), 2).alias(f"RU{s}_tx_power_watts"),
        F.round(F.avg(F.when(F.col("ru_slot") == slot, F.col("rx_signal_dbm")    )), 2).alias(f"RU{s}_rx_signal_strength_dbm"),
        F.round(F.avg(F.when(F.col("ru_slot") == slot, F.col("vswr")             )), 2).alias(f"RU{s}_vswr"),
        F.round(F.avg(F.when(F.col("ru_slot") == slot, F.col("current_ampere")     )), 2).alias(f"RU{s}_current_ampere"),
        F.round(F.avg(F.when(F.col("ru_slot") == slot, F.col("voltage_volt")     )), 2).alias(f"RU{s}_voltage_volt"),
        F.round(F.avg(F.when(F.col("ru_slot") == slot, F.col("packet_error_rate"))), 4).alias(f"RU{s}_packet_error_rate"),
        F.round(F.avg(F.when(F.col("ru_slot") == slot, F.col("throughput_mbps")  )), 2).alias(f"RU{s}_throughput_mbps"),
    ]

df_ru_agg = (
    df_ru
    .groupBy("site_id", "gold_snapshot_time", "gold_date")
    .agg(*ru_exprs)
)

# 2. BASEBAND UNITS
# silver cols: bbu_id, status, op_state, active_users,
#              cpu_pct, memory_pct, disk_pct,
#              control_latency_ms, user_latency_ms, process_latency_ms
df_bbu = (
    add_time_buckets(df_baseband_units)
    .withColumnRenamed("status",   "bbu_status")
    .withColumnRenamed("op_state", "bbu_op_state")
)
df_bbu = mode_of(df_bbu, SITE_WIN, "bbu_status")
df_bbu = mode_of(df_bbu, SITE_WIN, "bbu_op_state")

df_bbu_agg = (
    df_bbu
    .groupBy("site_id", "gold_snapshot_time", "gold_date")
    .agg(
        F.max("bbu_id"             ).alias("BBU_Sk"),
        F.max("mode_bbu_status"    ).alias("BBU_status"),
        F.max("mode_bbu_op_state"  ).alias("BBU_op_state"),
        F.round(F.avg("cpu_pct"),            2).alias("BBU_cpu_utilization_percent"),
        F.round(F.avg("memory_pct"),         2).alias("BBU_memory_utilization_percent"),
        F.round(F.avg("disk_pct"),           2).alias("BBU_disk_usage_percent"),
        F.round(F.avg("process_latency_ms"), 2).alias("BBU_process_latency_ms"),
        F.round(F.avg("active_users"),       0).cast("int").alias("BBU_active_users"),
        F.round(F.avg("control_latency_ms"), 2).alias("BBU_control_plane_latency_ms"),
    )
)

# 3. ANTENNAS
# silver cols: antenna_id, sector_id, status, op_state,
#              mimo_layers, azimuth_degree, tilt_degree, rssi_dbm, snr_db
df_ant = (
    add_time_buckets(df_antennas)
    .withColumnRenamed("status",   "ant_status")
    .withColumnRenamed("op_state", "ant_op_state")
)
df_ant = mode_of(df_ant, SITE_WIN + ["antenna_id"], "ant_status")
df_ant = mode_of(df_ant, SITE_WIN + ["antenna_id"], "ant_op_state")

w_ant  = Window.partitionBy("site_id", "gold_snapshot_time").orderBy("antenna_id")
df_ant = df_ant.withColumn("ant_slot", F.dense_rank().over(w_ant))

ant_exprs = []
for slot in [1, 2, 3]:
    s = str(slot)
    ant_exprs += [
        F.max(F.when(F.col("ant_slot") == slot, F.col("antenna_id")          )).alias(f"antenna{s}_Sk"),
        F.max(F.when(F.col("ant_slot") == slot, F.col("sector_id")           )).alias(f"antenna{s}_sector_key"),
        F.max(F.when(F.col("ant_slot") == slot, F.col("mode_ant_status")     )).alias(f"antenna{s}_status"),
        F.max(F.when(F.col("ant_slot") == slot, F.col("mode_ant_op_state")   )).alias(f"antenna{s}_opState"),
        # F.round(F.avg(F.when(F.col("ant_slot") == slot, F.col("tilt_degree")   )), 2).alias(f"antenna{s}_tilt_degree"),
        # F.round(F.avg(F.when(F.col("ant_slot") == slot, F.col("azimuth_degree"))), 2).alias(f"antenna{s}_azimuth_degree"),
        F.round(F.avg(F.when(F.col("ant_slot") == slot, F.col("rssi_dbm")      )), 2).alias(f"antenna{s}_rssi_dbm"),
        F.round(F.avg(F.when(F.col("ant_slot") == slot, F.col("snr_db")        )), 2).alias(f"antenna{s}_snr_db"),
    ]

df_ant_agg = (
    df_ant
    .groupBy("site_id", "gold_snapshot_time", "gold_date")
    .agg(*ant_exprs)
)

# 4. TRANSPORT LINKS
# silver cols: link_id, link_type, status, op_state,
#              throughput_mbps, utilization_percent,
#              latency_ms, jitter_ms, packet_loss_percent
df_lnk = (
    add_time_buckets(df_transport_links)
    .withColumnRenamed("status",   "lnk_status")
    .withColumnRenamed("op_state", "lnk_op_state")
)
df_lnk = mode_of(df_lnk, SITE_WIN + ["link_id"], "lnk_status")
df_lnk = mode_of(df_lnk, SITE_WIN + ["link_id"], "lnk_op_state")

w_lnk  = Window.partitionBy("site_id", "gold_snapshot_time").orderBy("link_id")
df_lnk = df_lnk.withColumn("lnk_slot", F.dense_rank().over(w_lnk))

lnk_exprs = []
for slot in [1, 2]:
    s = str(slot)
    lnk_exprs += [
        F.max(F.when(F.col("lnk_slot") == slot, F.col("link_id")              )).alias(f"link{s}_Sk"),
        F.max(F.when(F.col("lnk_slot") == slot, F.col("mode_lnk_status")      )).alias(f"link{s}_status"),
        F.max(F.when(F.col("lnk_slot") == slot, F.col("mode_lnk_op_state")    )).alias(f"link{s}_opState"),
        F.round(F.avg(F.when(F.col("lnk_slot") == slot, F.col("latency_ms")          )), 2).alias(f"link{s}_latency_ms"),
        F.round(F.avg(F.when(F.col("lnk_slot") == slot, F.col("jitter_ms")           )), 2).alias(f"link{s}_jitter_ms"),
        F.round(F.avg(F.when(F.col("lnk_slot") == slot, F.col("packet_loss_percent") )), 4).alias(f"link{s}_packet_loss_percent"),
        F.round(F.avg(F.when(F.col("lnk_slot") == slot, F.col("throughput_mbps")     )), 2).alias(f"link{s}_throughput_mbps"),
        F.round(F.avg(F.when(F.col("lnk_slot") == slot, F.col("utilization_percent") )), 2).alias(f"link{s}_utilization_percent"),
    ]

df_lnk_agg = (
    df_lnk
    .groupBy("site_id", "gold_snapshot_time", "gold_date")
    .agg(*lnk_exprs)
)

# 5. BATTERIES
# silver cols: battery_id, status, op_state, charge_pct, temperature_c
df_bat = (
    add_time_buckets(df_batteries)
    .withColumnRenamed("status",   "bat_status")
    .withColumnRenamed("op_state", "bat_op_state")
)
df_bat = mode_of(df_bat, SITE_WIN + ["battery_id"], "bat_status")
df_bat = mode_of(df_bat, SITE_WIN + ["battery_id"], "bat_op_state")

w_bat  = Window.partitionBy("site_id", "gold_snapshot_time").orderBy("battery_id")
df_bat = df_bat.withColumn("bat_slot", F.dense_rank().over(w_bat))

bat_exprs = []
for slot in [1, 2]:
    s = str(slot)
    bat_exprs += [
        F.max(F.when(F.col("bat_slot") == slot, F.col("battery_id")          )).alias(f"battery{s}_sk"),
        F.max(F.when(F.col("bat_slot") == slot, F.col("mode_bat_status")     )).alias(f"battery{s}_status"),
        F.max(F.when(F.col("bat_slot") == slot, F.col("mode_bat_op_state")   )).alias(f"battery{s}_opState"),
        F.round(F.avg(F.when(F.col("bat_slot") == slot, F.col("charge_pct")    )), 2).alias(f"battery{s}_charge_percent"),
        F.round(F.avg(F.when(F.col("bat_slot") == slot, F.col("temperature_c") )), 2).alias(f"battery{s}_temperature_c"),
    ]

df_bat_agg = (
    df_bat
    .groupBy("site_id", "gold_snapshot_time", "gold_date")
    .agg(*bat_exprs)
)

# 6. RECTIFIERS
# silver cols: rectifier_id, status, op_state,
#              current_ampere, output_voltage_volt
df_rect = (
    add_time_buckets(df_rectifiers)
    .withColumnRenamed("status",   "rect_status")
    .withColumnRenamed("op_state", "rect_op_state")
)
df_rect = mode_of(df_rect, SITE_WIN + ["rectifier_id"], "rect_status")

w_rect  = Window.partitionBy("site_id", "gold_snapshot_time").orderBy("rectifier_id")
df_rect = df_rect.withColumn("rect_slot", F.dense_rank().over(w_rect))

rect_exprs = []
for slot in [1, 2]:
    s = str(slot)
    rect_exprs += [
        F.max(F.when(F.col("rect_slot") == slot, F.col("rectifier_id")         )).alias(f"rect{s}_sk"),
        F.max(F.when(F.col("rect_slot") == slot, F.col("mode_rect_status")     )).alias(f"rect{s}_status"),
        F.round(F.avg(F.when(F.col("rect_slot") == slot, F.col("output_voltage_volt"))), 2).alias(f"rect{s}_output_voltage_volt"),
        F.round(F.avg(F.when(F.col("rect_slot") == slot, F.col("current_ampere")     )), 2).alias(f"rect{s}_current_ampere"),
    ]

df_rect_agg = (
    df_rect
    .groupBy("site_id", "gold_snapshot_time", "gold_date")
    .agg(*rect_exprs)
)

# 7. SITE SNAPSHOT
# silver cols: env_status, env_op_state (already prefixed - no rename needed)
#              gen_status, fuel_level_pct, runtime_hours,
#              door_status, smoke_detected
df_site = add_time_buckets(df_site_snapshot)
df_site = mode_of(df_site, SITE_WIN, "env_status")
df_site = mode_of(df_site, SITE_WIN, "env_op_state")

df_site_agg = (
    df_site
    .groupBy("site_id", "gold_snapshot_time", "gold_date")
    .agg(
        F.max("mode_env_status").alias("env_status"),
        F.max("mode_env_op_state").alias("env_opState"),
        F.max("gen_status").alias("generator_status"),
        F.round(F.avg("fuel_level_pct"), 2).alias("gen_fuel_level_percent"),
        F.round(F.avg("runtime_hours"), 2).alias("gen_runtime_hours"),
        F.max("door_status").alias("door_status"),
        F.max("smoke_detected").alias("smoke_detected")
    )
    .withColumn(
        "RAN_sk",
        F.md5(F.concat_ws("|", F.col("site_id"), F.col("gold_snapshot_time").cast("string")))
    )
)

# 8. ENVIRONMENT SENSORS
# silver cols: sensor_type, sensor_id, value, unit, status
# temperature - pivot slot 1 & 2
# humidity - single average
df_env = (
    add_time_buckets(df_environment_sensors)
    .withColumnRenamed("status", "sensor_status")
)

w_sensor = Window.partitionBy("site_id", "gold_snapshot_time").orderBy("sensor_id")

df_temp_agg = (
    df_env
    .filter(F.col("sensor_type") == "TEMPERATURE")
    .withColumn("sensor_slot", F.dense_rank().over(w_sensor))
    .groupBy("site_id", "gold_snapshot_time", "gold_date")
    .agg(
        F.max(F.when(F.col("sensor_slot") == 1, F.col("sensor_id"))).alias("temp_sensor1_Sk"),
        F.round(F.avg(F.when(F.col("sensor_slot") == 1, F.col("value"))), 2).alias("temp_sensor1_value_c"),
        F.max(F.when(F.col("sensor_slot") == 2, F.col("sensor_id"))).alias("temp_sensor2_Sk"),
        F.round(F.avg(F.when(F.col("sensor_slot") == 2, F.col("value"))), 2).alias("temp_sensor2_value_c"),
    )
)

df_hum_agg = (
    df_env
    .filter(F.col("sensor_type") == "HUMIDITY")
    .groupBy("site_id", "gold_snapshot_time", "gold_date")
    .agg(
        F.round(F.avg("value"), 2).alias("Humd_sensor_value_percent"),
    )
)

# 9. JOIN ALL SOURCES & FINAL SELECT
df_fact_ran = (
    df_site_agg
    .join(df_ru_agg,   ["site_id", "gold_snapshot_time", "gold_date"], "left")
    .join(df_bbu_agg,  ["site_id", "gold_snapshot_time", "gold_date"], "left")
    .join(df_ant_agg,  ["site_id", "gold_snapshot_time", "gold_date"], "left")
    .join(df_lnk_agg,  ["site_id", "gold_snapshot_time", "gold_date"], "left")
    .join(df_bat_agg,  ["site_id", "gold_snapshot_time", "gold_date"], "left")
    .join(df_rect_agg, ["site_id", "gold_snapshot_time", "gold_date"], "left")
    .join(df_temp_agg, ["site_id", "gold_snapshot_time", "gold_date"], "left")
    .join(df_hum_agg,  ["site_id", "gold_snapshot_time", "gold_date"], "left")
    .withColumn("date_key",
        F.date_format("gold_snapshot_time", "yyyyMMdd").cast("int"))
    .withColumn("time_key",
        F.date_format("gold_snapshot_time", "HHmm").cast("int"))
    .withColumn("power_status",
        F.coalesce(F.col("rect1_status"), F.col("rect2_status")))
    .withColumnRenamed("gold_snapshot_time", "timestamp")
    .select(
        "RAN_sk", "date_key", "timestamp", "time_key", "site_id",
        "RU1_Sk", "RU1_sector_key",
        "RU1_status", "RU1_op_state",
        "RU1_temperature_c", "RU1_tx_power_watts",
        "RU1_rx_signal_strength_dbm", "RU1_vswr",
        "RU1_current_ampere", "RU1_voltage_volt",
        "RU1_packet_error_rate", "RU1_throughput_mbps",
        "RU2_Sk", "RU2_sector_key",
        "RU2_status", "RU2_op_state",
        "RU2_temperature_c", "RU2_tx_power_watts",
        "RU2_rx_signal_strength_dbm", "RU2_vswr",
        "RU2_current_ampere", "RU2_voltage_volt",
        "RU2_packet_error_rate", "RU2_throughput_mbps",
        "RU3_Sk", "RU3_sector_key",
        "RU3_status", "RU3_op_state",
        "RU3_temperature_c", "RU3_tx_power_watts",
        "RU3_rx_signal_strength_dbm", "RU3_vswr",
        "RU3_current_ampere", "RU3_voltage_volt",
        "RU3_packet_error_rate", "RU3_throughput_mbps",
        "BBU_Sk", "BBU_status", "BBU_op_state",
        "BBU_cpu_utilization_percent",
        "BBU_memory_utilization_percent",
        "BBU_disk_usage_percent",
        "BBU_process_latency_ms",
        "BBU_active_users",
        "BBU_control_plane_latency_ms",
        "antenna1_Sk", "antenna1_sector_key",
        "antenna1_status", "antenna1_opState",
        "antenna1_rssi_dbm", "antenna1_snr_db",
        "antenna2_Sk", "antenna2_sector_key",
        "antenna2_status", "antenna2_opState",
        "antenna2_rssi_dbm", "antenna2_snr_db",
        "antenna3_Sk", "antenna3_sector_key",
        "antenna3_status", "antenna3_opState",
        "antenna3_rssi_dbm", "antenna3_snr_db",
        "link1_Sk", "link1_status", "link1_opState",
        "link1_latency_ms", "link1_jitter_ms",
        "link1_packet_loss_percent",
        "link1_throughput_mbps", "link1_utilization_percent",
        "link2_Sk", "link2_status", "link2_opState",
        "link2_latency_ms", "link2_jitter_ms",
        "link2_packet_loss_percent",
        "link2_throughput_mbps", "link2_utilization_percent",
        "power_status",
        "rect1_sk", "rect1_status",
        "rect1_output_voltage_volt", "rect1_current_ampere",
        "rect2_sk", "rect2_status",
        "rect2_output_voltage_volt", "rect2_current_ampere",
        "battery1_sk", "battery1_status", "battery1_opState",
        "battery1_charge_percent", "battery1_temperature_c",
        "battery2_sk", "battery2_status", "battery2_opState",
        "battery2_charge_percent", "battery2_temperature_c",
        "generator_status", "gen_fuel_level_percent", "gen_runtime_hours",
        "env_status", "env_opState",
        "temp_sensor1_Sk", "temp_sensor1_value_c",
        "temp_sensor2_Sk", "temp_sensor2_value_c",
        "Humd_sensor_value_percent",
        "door_status", "smoke_detected",
        "gold_date",
    )
)

print("--- [SHOWING DATA] Fact_RAN Output Sample ---")
df_fact_ran.show(5, truncate=False)
print(f"Fact_RAN row count : {df_fact_ran.count():,}")
print(f"Fact_RAN col count : {len(df_fact_ran.columns)}")

write_to_gold(df_fact_ran, "Fact_RAN", partition_cols=["gold_date"])



# ==========================================
# 5b. FACT_CELLS (COMPATIBLE MODE LOGIC & ROUNDING)
# ==========================================
print("\n" + "="*50)
print("Processing Fact_Cells with Compatible Mode and Telemetry Rounding")
print("="*50)

from pyspark.sql import Window

# --- Mode calculation for cell_status and op_state ---
df_cells_bucketed = add_time_buckets(df_cells)

window_status_count = Window.partitionBy("site_id", "cell_id", "gold_snapshot_time", "cell_status")
window_op_count = Window.partitionBy("site_id", "cell_id", "gold_snapshot_time", "cell_op_state")

df_with_counts = df_cells_bucketed \
    .withColumn("status_count", F.count("cell_status").over(window_status_count)) \
    .withColumn("op_state_count", F.count("cell_op_state").over(window_op_count))

window_mode_rank = Window.partitionBy("site_id", "cell_id", "gold_snapshot_time").orderBy(F.col("status_count").desc())
window_op_mode_rank = Window.partitionBy("site_id", "cell_id", "gold_snapshot_time").orderBy(F.col("op_state_count").desc())

df_modes = df_with_counts \
    .withColumn("status_rank", F.row_number().over(window_mode_rank)) \
    .withColumn("op_rank", F.row_number().over(window_op_mode_rank))

df_cell_modes_final = df_modes \
    .withColumn("mode_cell_status", F.max(F.when(F.col("status_rank") == 1, F.col("cell_status"))).over(Window.partitionBy("site_id", "cell_id", "gold_snapshot_time"))) \
    .withColumn("mode_cell_op_state", F.max(F.when(F.col("op_rank") == 1, F.col("cell_op_state"))).over(Window.partitionBy("site_id", "cell_id", "gold_snapshot_time")))

# --- Main Aggregation ---
df_fact_cells = df_cell_modes_final \
    .groupBy("site_id", "cell_id", "sector_id", "technology", "gold_snapshot_time", "gold_date") \
    .agg(
        F.first("message_id").alias("RAN_msg_Sk"),
        F.avg("connected_users").cast("int").alias("connected_users"),
        F.avg("active_users").cast("int").alias("active_users"),
        F.max("connected_users").cast("int").alias("peak_users"),
        F.round(F.avg("prb_utilization_percent"), 2).alias("prb_utilization_percent"),
        F.round(F.avg("throughput_downlink_mbps"), 2).alias("throughput_downlink_mbps"),
        F.round(F.avg("throughput_uplink_mbps"), 2).alias("throughput_uplink_mbps"),
        F.round(F.avg("rsrp_dbm"), 2).alias("rsrp_dbm"),
        F.round(F.avg("rsrq_db"), 2).alias("rsrq_db"),
        F.round(F.avg("sinr_db"), 2).alias("sinr_db"),
        F.round(F.avg("cqi_avg"), 2).alias("cqi_avg"),
        F.round(F.avg("spectral_efficiency_bps_per_hz"), 3).alias("spectral_efficiency_bps_per_hz"),
        F.round(F.avg("bler_downlink_percent"), 2).alias("bler_downlink_percent"),
        F.round(F.avg("bler_uplink_percent"), 2).alias("bler_uplink_percent"),
        F.round(F.avg("harq_retransmission_rate_percent"), 2).alias("harq_retransmission_rate_percent"),
        F.round(F.avg("latency_downlink_ms"), 2).alias("latency_downlink_ms"),
        F.round(F.avg("latency_uplink_ms"), 2).alias("latency_uplink_ms"),
        F.sum("handover_attempts").alias("handover_attempts"),
        F.sum("handover_failures").alias("handover_failures"),
        F.sum("rrc_connection_attempts").alias("rrc_connection_attempts"),
        F.round(F.avg("rrc_success_rate_percent"), 2).alias("rrc_success_rate_percent"),
        F.round(F.avg("erab_setup_success_rate_percent"), 2).alias("erab_setup_success_rate_percent"),
        F.round(F.avg("call_drop_rate_percent"), 2).alias("call_drop_rate_percent"),
        F.round(F.avg("abnormal_release_rate_percent"), 2).alias("abnormal_release_rate_percent"),
        F.first("mode_cell_status").alias("cell_status"),
        F.first("mode_cell_op_state").alias("op_state"),
        F.max(F.when(F.col("prb_utilization_percent") > 80, 1).otherwise(0)).alias("congestion_flag"),
    ) \
    .withColumn("date_key", F.date_format("gold_snapshot_time", "yyyyMMdd").cast("int")) \
    .withColumn("time_key", F.date_format("gold_snapshot_time", "HHmm").cast("int")) \
    .withColumn("traffic_volume_gb",
        F.round(((F.col("throughput_downlink_mbps") + F.col("throughput_uplink_mbps")) * 900) / 8 / 1024, 3)) \
    .withColumn("handover_success_rate_percent",
        F.round(
            F.when(F.col("handover_attempts") > 0,
                (1 - F.col("handover_failures") / F.col("handover_attempts")) * 100
            ).otherwise(F.lit(None)), 2
        )) \
    .withColumn("cell_key", F.md5(F.concat_ws("|", F.col("site_id"), F.col("cell_id")))) \
    .withColumnRenamed("sector_id", "sector_bk")\
    .withColumn("RAN_key", F.md5(F.col("site_id"))) \
    .withColumnRenamed("gold_snapshot_time", "timestamp") \
    .select(
        "RAN_msg_Sk", "date_key", "timestamp", "time_key", "gold_date",
        "cell_key", "RAN_key", "sector_bk", "active_users", "connected_users", "peak_users", "prb_utilization_percent",
        "throughput_downlink_mbps", "throughput_uplink_mbps",
        "rsrp_dbm", "rsrq_db", "sinr_db", "cqi_avg",
        "spectral_efficiency_bps_per_hz", "bler_downlink_percent", "bler_uplink_percent",
        "harq_retransmission_rate_percent", "latency_downlink_ms", "latency_uplink_ms",
        "handover_attempts", "handover_failures", "handover_success_rate_percent",
        "rrc_connection_attempts", "rrc_success_rate_percent",
        "erab_setup_success_rate_percent", "call_drop_rate_percent",
        "abnormal_release_rate_percent", "traffic_volume_gb",
        "congestion_flag", "cell_status", "op_state"
    )

print("--- [SHOWING DATA] Fact_Cells Output Sample ---")
df_fact_cells.show(truncate=False)
print(f"Fact_Cells row count: {df_fact_cells.count():,}")

write_to_gold(df_fact_cells, "Fact_Cells", partition_cols=["gold_date"])

# ==========================================
# 5c. FACT_ALARMS
# One row per alert event - no aggregation.
# explode_outer in silver already ensures one row per alert.
# ==========================================
from pyspark.sql.functions import monotonically_increasing_id

df_fact_alarms = df_alerts \
    .filter(F.col("alert_id").isNotNull()) \
    .withColumn("date_key",
        F.date_format("snapshot_time", "yyyyMMdd").cast("int")) \
    .withColumn("time_key",
        F.date_format("snapshot_time", "HHmm").cast("int")) \
    .withColumn("gold_date", F.to_date("snapshot_time")) \
    .withColumn("RAN_key", F.md5(F.col("site_id"))) \
    .select(
        F.col("message_id"),
        F.col("RAN_key").alias("site_key"),
        "date_key",
        "time_key",
        F.col("category").alias("alarm_category"),
        F.col("component_id").alias("component_key"),
        "severity",
        F.col("message").alias("alarm_msg"),
        "snapshot_time",
        "gold_date",
    ) \
    .withColumn("alarm_sk", monotonically_increasing_id())
print("--- [SHOWING DATA] Fact_Alarms Output Sample ---")
df_fact_alarms.show(10, truncate=False)
print(f"Fact_Alarms row count: {df_fact_alarms.count():,}")

write_to_gold(df_fact_alarms, "Fact_Alarms", partition_cols=["gold_date"])

print("Gold layer job complete.")
spark.stop()
