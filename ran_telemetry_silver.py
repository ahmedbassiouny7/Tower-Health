import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    explode,
    explode_outer,
    input_file_name,
    lit,
    to_timestamp,
    upper,
    when,
)
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# 1. Spark setup
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("TowerHealth-Silver")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 2. Paths
# ---------------------------------------------------------------------------
RAW_PREFIX           = "s3a://tower-iti-project/raw-data/ran_telemetry/"
OUTPUT_BASE          = "s3a://tower-iti-project/silver/ran_telemetry_normalized"
PROCESSED_FILES_PATH = f"{OUTPUT_BASE}/_state/processed_files"

# ---------------------------------------------------------------------------
# 3. Schema
# ---------------------------------------------------------------------------
RAN_SCHEMA = StructType([
    StructField("message_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("sequence_number", LongType(), True),
    StructField("ran_metadata", StructType([
        StructField("site_id", StringType(), True),
        StructField("site_name", StringType(), True),
        StructField("location", StructType([
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
        ]), True),
        StructField("region", StringType(), True),
        StructField("vendor", StringType(), True),
        StructField("technology", ArrayType(StringType()), True),
    ]), True),
    StructField("environment", StructType([
        StructField("status", StringType(), True),
        StructField("op_state", StringType(), True),
        StructField("temperature_sensors", ArrayType(StructType([
            StructField("sensor_id", StringType(), True),
            StructField("value_c", DoubleType(), True),
        ])), True),
        StructField("humidity_sensors", ArrayType(StructType([
            StructField("sensor_id", StringType(), True),
            StructField("value_percent", DoubleType(), True),
        ])), True),
        StructField("door_status", StringType(), True),
        StructField("smoke_detected", BooleanType(), True),
    ]), True),
    StructField("antennas", ArrayType(StructType([
        StructField("antenna_id", StringType(), True),
        StructField("sector_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("op_state", StringType(), True),
        StructField("mimo_layers", IntegerType(), True),
        StructField("azimuth_degree", IntegerType(), True),
        StructField("tilt_degree", DoubleType(), True),
        StructField("rssi_dbm", DoubleType(), True),
        StructField("snr_db", DoubleType(), True),
    ])), True),
    StructField("baseband_units", ArrayType(StructType([
        StructField("bbu_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("op_state", StringType(), True),
        StructField("active_users", IntegerType(), True),
        StructField("cpu_utilization_percent", DoubleType(), True),
        StructField("memory_utilization_percent", DoubleType(), True),
        StructField("disk_usage_percent", DoubleType(), True),
        StructField("control_plane_latency_ms", DoubleType(), True),
        StructField("user_plane_latency_ms", DoubleType(), True),
        StructField("process_latency_ms", DoubleType(), True),
    ])), True),
    StructField("transport_links", ArrayType(StructType([
        StructField("link_id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("status", StringType(), True),
        StructField("op_state", StringType(), True),
        StructField("throughput_mbps", DoubleType(), True),
        StructField("utilization_percent", DoubleType(), True),
        StructField("latency_ms", DoubleType(), True),
        StructField("jitter_ms", DoubleType(), True),
        StructField("packet_loss_percent", DoubleType(), True),
    ])), True),
    StructField("cells", ArrayType(StructType([
        StructField("cell_id", StringType(), True),
        StructField("sector_id", StringType(), True),
        StructField("technology", StringType(), True),
        StructField("status", StringType(), True),
        StructField("op_state", StringType(), True),
        StructField("bandwidth_mhz", IntegerType(), True),
        StructField("carrier_frequency_mhz", IntegerType(), True),
        StructField("connected_users", IntegerType(), True),
        StructField("active_users", IntegerType(), True),
        StructField("throughput_downlink_mbps", DoubleType(), True),
        StructField("throughput_uplink_mbps", DoubleType(), True),
        StructField("latency_downlink_ms", DoubleType(), True),
        StructField("latency_uplink_ms", DoubleType(), True),
        StructField("prb_utilization_percent", DoubleType(), True),
        StructField("sinr_db", DoubleType(), True),
        StructField("rsrp_dbm", DoubleType(), True),
        StructField("rsrq_db", DoubleType(), True),
        StructField("cqi_avg", DoubleType(), True),
        StructField("handover_success_rate_percent", DoubleType(), True),
        StructField("handover_attempts", IntegerType(), True),
        StructField("handover_failures", IntegerType(), True),
        StructField("rrc_success_rate_percent", DoubleType(), True),
        StructField("rrc_connection_attempts", IntegerType(), True),
        StructField("erab_setup_success_rate_percent", DoubleType(), True),
        StructField("call_drop_rate_percent", DoubleType(), True),
        StructField("abnormal_release_rate_percent", DoubleType(), True),
        StructField("harq_retransmission_rate_percent", DoubleType(), True),
        StructField("bler_uplink_percent", DoubleType(), True),
        StructField("bler_downlink_percent", DoubleType(), True),
        StructField("spectral_efficiency_bps_per_hz", DoubleType(), True),
    ])), True),
    StructField("radio_units", ArrayType(StructType([
        StructField("ru_id", StringType(), True),
        StructField("sector_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("op_state", StringType(), True),
        StructField("tx_power_watts", DoubleType(), True),
        StructField("rx_signal_strength_dbm", DoubleType(), True),
        StructField("current_ampere", DoubleType(), True),
        StructField("voltage_volt", DoubleType(), True),
        StructField("temperature_c", DoubleType(), True),
        StructField("throughput_mbps", DoubleType(), True),
        StructField("packet_error_rate", DoubleType(), True),
        StructField("vswr", DoubleType(), True),
    ])), True),
    StructField("power_system", StructType([
        StructField("status", StringType(), True),
        StructField("batteries", ArrayType(StructType([
            StructField("battery_id", StringType(), True),
            StructField("status", StringType(), True),
            StructField("op_state", StringType(), True),
            StructField("charge_percent", DoubleType(), True),
            StructField("temperature_c", DoubleType(), True),
        ])), True),
        StructField("rectifiers", ArrayType(StructType([
            StructField("rectifier_id", StringType(), True),
            StructField("status", StringType(), True),
            StructField("op_state", StringType(), True),
            StructField("current_ampere", DoubleType(), True),
            StructField("output_voltage_volt", DoubleType(), True),
        ])), True),
        StructField("generator", StructType([
            StructField("status", StringType(), True),
            StructField("fuel_level_percent", DoubleType(), True),
            StructField("runtime_hours", DoubleType(), True),
        ]), True),
    ]), True),
    StructField("alerts", ArrayType(StructType([
        StructField("alert_id", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("category", StringType(), True),
        StructField("component_type", StringType(), True),
        StructField("component_id", StringType(), True),
        StructField("code", StringType(), True),
        StructField("message", StringType(), True),
        StructField("value", StringType(), True),
    ])), True),
    StructField("alert_summary", StructType([
        StructField("total", IntegerType(), True),
        StructField("critical", IntegerType(), True),
        StructField("warning", IntegerType(), True),
        StructField("info", IntegerType(), True),
        StructField("highest_severity", StringType(), True),
    ]), True),
])

# ---------------------------------------------------------------------------
# 4. Incremental file detection
# ---------------------------------------------------------------------------
def parse_s3_uri(uri):
    bucket_and_key = uri.split("://", 1)[1]
    bucket, _, key = bucket_and_key.partition("/")
    return bucket, key

def list_s3_json_files(prefix_uri):
    bucket, prefix = parse_s3_uri(prefix_uri)
    client = boto3.client("s3")
    paginator = client.get_paginator("list_objects_v2")
    files = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                files.append(f"s3a://{bucket}/{obj['Key']}")
    return sorted(files)

def read_processed_files(path):
    try:
        return {
            row.source_file
            for row in spark.read.parquet(path).select("source_file").distinct().collect()
        }
    except Exception:
        return set()

all_files       = list_s3_json_files(RAW_PREFIX)
processed_files = read_processed_files(PROCESSED_FILES_PATH)
new_files       = [f for f in all_files if f not in processed_files]

print(f"Raw files found      : {len(all_files)}")
print(f"Already processed    : {len(processed_files)}")
print(f"New files to process : {len(new_files)}")

if not new_files:
    print("No new files. Job complete.")
else:
    # ---------------------------------------------------------------------------
    # 5. Load new files
    # ---------------------------------------------------------------------------
    df_raw = (
        spark.read
        .schema(RAN_SCHEMA)
        .json(new_files)
        .withColumn("source_file", input_file_name())
    )
    print(f"New snapshots loaded : {df_raw.count()}")

    # ---------------------------------------------------------------------------
    # 6. Base column lists
    # BASE_COLS        — used in the FIRST select (raw nested column names)
    # BASE_PASSTHROUGH — used in the SECOND select after explode (flat aliases)
    # ---------------------------------------------------------------------------
    BASE_COLS = [
        col("message_id"),
        col("source_file"),
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("snapshot_time"),
        col("sequence_number"),
        col("ran_metadata.site_id").alias("site_id"),
        col("ran_metadata.site_name").alias("site_name"),
        col("ran_metadata.region").alias("region"),
    ]

    BASE_PASSTHROUGH = [
        col("message_id"),
        col("source_file"),
        col("snapshot_time"),
        col("sequence_number"),
        col("site_id"),
        col("site_name"),
        col("region"),
    ]

    # ---------------------------------------------------------------------------
    # 7. Transformations → 10 normalized silver tables
    # ---------------------------------------------------------------------------

    # -- 1. site_snapshot (1 row per snapshot) -----------------------------------
    # technologies is kept as ARRAY<STRING> (not flattened to a string).
    # Sensor aggregates move to the environment_sensors table.
    df_site_snapshot = df_raw.select(
        *BASE_COLS,
        col("ran_metadata.vendor").alias("vendor"),
        col("ran_metadata.technology").alias("technologies"),
        col("ran_metadata.location.latitude").alias("latitude"),
        col("ran_metadata.location.longitude").alias("longitude"),
        col("environment.status").alias("env_status"),
        col("environment.op_state").alias("env_op_state"),
        col("environment.smoke_detected"),
        col("environment.door_status"),
        col("power_system.generator.status").alias("gen_status"),
        col("power_system.generator.fuel_level_percent").alias("fuel_level_pct"),
        col("power_system.generator.runtime_hours"),
        col("alert_summary.total").alias("total_alerts"),
        col("alert_summary.critical").alias("critical_alerts"),
        col("alert_summary.warning").alias("warning_alerts"),
        col("alert_summary.info").alias("info_alerts"),
        col("alert_summary.highest_severity"),
    )

    # -- 2. environment_sensors (1 row per sensor per snapshot) ------------------
    # Temperature and humidity sensors are unioned into one table.
    # status is derived from thresholds: temperature >40°C=CRITICAL, >37°C=HIGH;
    # humidity >80%=HIGH.
    df_temp_sensors = df_raw.select(
        *BASE_COLS,
        explode(col("environment.temperature_sensors")).alias("sensor"),
    ).select(
        *BASE_PASSTHROUGH,
        lit("TEMPERATURE").alias("sensor_type"),
        col("sensor.sensor_id"),
        col("sensor.value_c").alias("value"),
        lit("C").alias("unit"),
        when(col("sensor.value_c") > 40, "CRITICAL")
        .when(col("sensor.value_c") > 37, "HIGH")
        .otherwise("OK").alias("status"),
    )

    df_hum_sensors = df_raw.select(
        *BASE_COLS,
        explode(col("environment.humidity_sensors")).alias("sensor"),
    ).select(
        *BASE_PASSTHROUGH,
        lit("HUMIDITY").alias("sensor_type"),
        col("sensor.sensor_id"),
        col("sensor.value_percent").alias("value"),
        lit("percent").alias("unit"),
        when(col("sensor.value_percent") > 80, "HIGH")
        .otherwise("OK").alias("status"),
    )

    df_environment_sensors = df_temp_sensors.unionByName(df_hum_sensors)

    # -- 3. cells (1 row per cell per snapshot) ----------------------------------
    df_cells = df_raw.select(
        *BASE_COLS,
        explode(col("cells")).alias("cell"),
    ).select(
        *BASE_PASSTHROUGH,
        col("cell.cell_id"),
        col("cell.sector_id"),
        col("cell.technology"),
        col("cell.status").alias("cell_status"),
        col("cell.op_state").alias("cell_op_state"),
        col("cell.bandwidth_mhz"),
        col("cell.carrier_frequency_mhz"),
        col("cell.connected_users"),
        col("cell.active_users"),
        col("cell.throughput_downlink_mbps"),
        col("cell.throughput_uplink_mbps"),
        col("cell.latency_downlink_ms"),
        col("cell.latency_uplink_ms"),
        col("cell.prb_utilization_percent"),
        col("cell.sinr_db"),
        col("cell.rsrp_dbm"),
        col("cell.rsrq_db"),
        col("cell.cqi_avg"),
        col("cell.handover_success_rate_percent"),
        col("cell.handover_attempts"),
        col("cell.handover_failures"),
        col("cell.rrc_success_rate_percent"),
        col("cell.rrc_connection_attempts"),
        col("cell.erab_setup_success_rate_percent"),
        col("cell.call_drop_rate_percent"),
        col("cell.abnormal_release_rate_percent"),
        col("cell.harq_retransmission_rate_percent"),
        col("cell.bler_uplink_percent"),
        col("cell.bler_downlink_percent"),
        col("cell.spectral_efficiency_bps_per_hz"),
    )

    # -- 4. antennas (1 row per antenna per snapshot) ----------------------------
    df_antennas = df_raw.select(
        *BASE_COLS,
        explode(col("antennas")).alias("ant"),
    ).select(
        *BASE_PASSTHROUGH,
        col("ant.antenna_id"),
        col("ant.sector_id"),
        col("ant.status"),
        col("ant.op_state"),
        col("ant.mimo_layers"),
        col("ant.azimuth_degree"),
        col("ant.tilt_degree"),
        col("ant.rssi_dbm"),
        col("ant.snr_db"),
    )

    # -- 5. radio_units (1 row per RU per snapshot) ------------------------------
    # rx_signal_strength_dbm is aliased to rx_signal_dbm to match the schema spec.
    df_radio_units = df_raw.select(
        *BASE_COLS,
        explode(col("radio_units")).alias("ru"),
    ).select(
        *BASE_PASSTHROUGH,
        col("ru.ru_id"),
        col("ru.sector_id"),
        col("ru.status"),
        col("ru.op_state"),
        col("ru.tx_power_watts"),
        col("ru.rx_signal_strength_dbm").alias("rx_signal_dbm"),
        col("ru.current_ampere"),
        col("ru.voltage_volt"),
        col("ru.temperature_c"),
        col("ru.throughput_mbps"),
        col("ru.packet_error_rate"),
        col("ru.vswr"),
    )

    # -- 6. baseband_units (1 row per BBU per snapshot) --------------------------
    df_baseband_units = df_raw.select(
        *BASE_COLS,
        explode(col("baseband_units")).alias("bbu"),
    ).select(
        *BASE_PASSTHROUGH,
        col("bbu.bbu_id"),
        col("bbu.status"),
        col("bbu.op_state"),
        col("bbu.active_users"),
        col("bbu.cpu_utilization_percent").alias("cpu_pct"),
        col("bbu.memory_utilization_percent").alias("memory_pct"),
        col("bbu.disk_usage_percent").alias("disk_pct"),
        col("bbu.control_plane_latency_ms").alias("control_latency_ms"),
        col("bbu.user_plane_latency_ms").alias("user_latency_ms"),
        col("bbu.process_latency_ms"),
    )

    # -- 7. batteries (1 row per battery per snapshot) ---------------------------
    # Raw battery objects have: battery_id, status, op_state, charge_percent,
    # temperature_c. There is no voltage_volt field in the raw data.
    df_batteries = df_raw.select(
        *BASE_COLS,
        explode(col("power_system.batteries")).alias("bat"),
    ).select(
        *BASE_PASSTHROUGH,
        col("bat.battery_id"),
        col("bat.status"),
        col("bat.op_state"),
        col("bat.charge_percent").alias("charge_pct"),
        col("bat.temperature_c"),
    )

    # -- 8. rectifiers (1 row per rectifier per snapshot) ------------------------
    df_rectifiers = df_raw.select(
        *BASE_COLS,
        explode(col("power_system.rectifiers")).alias("rec"),
    ).select(
        *BASE_PASSTHROUGH,
        col("rec.rectifier_id"),
        col("rec.status"),
        col("rec.op_state"),
        col("rec.current_ampere"),
        col("rec.output_voltage_volt"),
    )

    # -- 9. transport_links (1 row per link per snapshot) ------------------------
    # link_type is uppercased to normalise "fiber"/"microwave" from raw data.
    df_transport_links = df_raw.select(
        *BASE_COLS,
        explode(col("transport_links")).alias("link"),
    ).select(
        *BASE_PASSTHROUGH,
        col("link.link_id"),
        upper(col("link.type")).alias("link_type"),
        col("link.status"),
        col("link.op_state"),
        col("link.throughput_mbps"),
        col("link.utilization_percent"),
        col("link.latency_ms"),
        col("link.jitter_ms"),
        col("link.packet_loss_percent"),
    )

    # -- 10. alerts (1 row per alert per snapshot) --------------------------------
    # Raw alert.value is a string containing either a number ("68.4") or a
    # categorical token ("DOWN", "FAILED"). Split into two typed columns:
    #   alert_value_num — DOUBLE when the value parses as a number, else NULL
    #   alert_value_str — STRING when the value does NOT parse as a number, else NULL
    df_alerts = df_raw.select(
        *BASE_COLS,
        explode_outer(col("alerts")).alias("alert"),
    ).select(
        *BASE_PASSTHROUGH,
        col("alert.alert_id"),
        col("alert.severity"),
        col("alert.category"),
        col("alert.component_type"),
        col("alert.component_id"),
        col("alert.code"),
        col("alert.message"),
        col("alert.value").cast(DoubleType()).alias("alert_value_num"),
        when(
            col("alert.value").cast(DoubleType()).isNull() & col("alert.value").isNotNull(),
            col("alert.value"),
        ).cast(StringType()).alias("alert_value_str"),
    )

    # ---------------------------------------------------------------------------
    # 8. Write silver tables (append — incremental safe)
    # ---------------------------------------------------------------------------
    tables = {
        "site_snapshot":       df_site_snapshot,
        "environment_sensors": df_environment_sensors,
        "cells":               df_cells,
        "antennas":            df_antennas,
        "radio_units":         df_radio_units,
        "baseband_units":      df_baseband_units,
        "batteries":           df_batteries,
        "rectifiers":          df_rectifiers,
        "transport_links":     df_transport_links,
        "alerts":              df_alerts,
    }

    for name, df in tables.items():
        path = f"{OUTPUT_BASE}/{name}"
        df.write.mode("append").partitionBy("region").parquet(path)
        print(f"  written -> {path}")

    # ---------------------------------------------------------------------------
    # 9. Update processed-files manifest
    # ---------------------------------------------------------------------------
    (
        spark.createDataFrame([(f,) for f in new_files], ["source_file"])
        .withColumn("processed_at", current_timestamp())
        .write.mode("append").parquet(PROCESSED_FILES_PATH)
    )

    print(f"\nJob complete. {len(new_files)} file(s) processed, 10 normalized silver tables updated.")
