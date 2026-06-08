from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# 1. Spark setup
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("TowerHealth-ML-Prep")
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
# 2. Paths
# ---------------------------------------------------------------------------
SILVER_CELLS_PATH = "s3a://tower-iti-project/silver/ran_telemetry_normalized/cells"
ML_INPUT_PATH = "s3a://tower-iti-project/gold/ran_ml_input/"

# ---------------------------------------------------------------------------
# 3. ML input builder
#
# Full read + overwrite on every run. The ML table is hourly aggregated, and a
# single hour can be filled by snapshots that arrive across multiple silver
# runs. Rebuilding from the complete cells table keeps every site-hour complete
# and makes this step idempotent.
#
# Wide-column contract (cell_N_<suffix>) must stay aligned with
# ran_cell_model_features.json / 03_predict.py.
# ---------------------------------------------------------------------------
ML_SUFFIXES = [
    "status",
    "op_state",
    "active_users",
    "connected_users",
    "prb_utilization_percent",
    "throughput_downlink_mbps",
    "throughput_uplink_mbps",
    "spectral_efficiency_bps_per_hz",
    "rsrp_dbm",
    "rsrq_db",
    "sinr_db",
    "cqi_avg",
    "bler_downlink_percent",
    "bler_uplink_percent",
    "harq_retransmission_rate_percent",
    "latency_downlink_ms",
    "latency_uplink_ms",
    "handover_attempts",
    "handover_success_rate_percent",
    "handover_failures",
    "rrc_connection_attempts",
    "rrc_success_rate_percent",
    "erab_setup_success_rate_percent",
    "call_drop_rate_percent",
    "abnormal_release_rate_percent",
    "technology",
    "bandwidth_mhz",
]


def mode_col(frame, group_cols, col_name):
    """Most frequent value of a categorical column within each group."""
    w_cnt = Window.partitionBy(group_cols + [col_name])
    w_rank = Window.partitionBy(group_cols).orderBy(
        F.col(f"__cnt_{col_name}").desc(),
        F.col(col_name).asc(),
    )
    w_prop = Window.partitionBy(group_cols)

    return (
        frame
        .withColumn(f"__cnt_{col_name}", F.count(col_name).over(w_cnt))
        .withColumn(f"__rnk_{col_name}", F.row_number().over(w_rank))
        .withColumn(
            f"mode_{col_name}",
            F.max(F.when(F.col(f"__rnk_{col_name}") == 1, F.col(col_name))).over(w_prop),
        )
        .drop(f"__cnt_{col_name}", f"__rnk_{col_name}")
    )


def build_ml_input():
    print("\n[ml-prep] Building ML input from Silver cells ...")

    frame = spark.read.parquet(SILVER_CELLS_PATH)
    print(f"[ml-prep] Silver cells loaded : {frame.count():,} rows")

    frame = (
        frame
        .withColumnRenamed("cell_status", "status")
        .withColumnRenamed("cell_op_state", "op_state")
        .withColumn("hour_ts", F.date_trunc("hour", F.col("snapshot_time")))
    )

    group = ["site_id", "cell_id", "hour_ts"]
    frame = mode_col(frame, group, "status")
    frame = mode_col(frame, group, "op_state")

    df_hourly = (
        frame
        .groupBy("site_id", "cell_id", "hour_ts")
        .agg(
            F.first("mode_status").alias("status"),
            F.first("mode_op_state").alias("op_state"),
            F.first("technology").alias("technology"),
            F.first("bandwidth_mhz").alias("bandwidth_mhz"),
            F.round(F.avg("active_users"), 0).cast("int").alias("active_users"),
            F.round(F.avg("connected_users"), 0).cast("int").alias("connected_users"),
            F.round(F.avg("prb_utilization_percent"), 4).alias("prb_utilization_percent"),
            F.round(F.avg("throughput_downlink_mbps"), 4).alias("throughput_downlink_mbps"),
            F.round(F.avg("throughput_uplink_mbps"), 4).alias("throughput_uplink_mbps"),
            F.round(F.avg("spectral_efficiency_bps_per_hz"), 4).alias(
                "spectral_efficiency_bps_per_hz"
            ),
            F.round(F.avg("rsrp_dbm"), 4).alias("rsrp_dbm"),
            F.round(F.avg("rsrq_db"), 4).alias("rsrq_db"),
            F.round(F.avg("sinr_db"), 4).alias("sinr_db"),
            F.round(F.avg("cqi_avg"), 4).alias("cqi_avg"),
            F.round(F.avg("bler_downlink_percent"), 4).alias("bler_downlink_percent"),
            F.round(F.avg("bler_uplink_percent"), 4).alias("bler_uplink_percent"),
            F.round(F.avg("harq_retransmission_rate_percent"), 4).alias(
                "harq_retransmission_rate_percent"
            ),
            F.round(F.avg("latency_downlink_ms"), 4).alias("latency_downlink_ms"),
            F.round(F.avg("latency_uplink_ms"), 4).alias("latency_uplink_ms"),
            F.round(F.avg("handover_success_rate_percent"), 4).alias(
                "handover_success_rate_percent"
            ),
            F.round(F.avg("rrc_success_rate_percent"), 4).alias("rrc_success_rate_percent"),
            F.round(F.avg("erab_setup_success_rate_percent"), 4).alias(
                "erab_setup_success_rate_percent"
            ),
            F.round(F.avg("call_drop_rate_percent"), 4).alias("call_drop_rate_percent"),
            F.round(F.avg("abnormal_release_rate_percent"), 4).alias(
                "abnormal_release_rate_percent"
            ),
            F.sum("handover_attempts").alias("handover_attempts"),
            F.sum("handover_failures").alias("handover_failures"),
            F.sum("rrc_connection_attempts").alias("rrc_connection_attempts"),
        )
    )
    print(f"[ml-prep] Hourly aggregated   : {df_hourly.count():,} rows")

    w_slot = Window.partitionBy("site_id", "hour_ts").orderBy("cell_id")
    df_slotted = df_hourly.withColumn("cell_slot", F.dense_rank().over(w_slot))

    pivot_exprs = [
        F.max(F.when(F.col("cell_slot") == slot, F.col(suffix))).alias(
            f"cell_{slot}_{suffix}"
        )
        for slot in [1, 2, 3]
        for suffix in ML_SUFFIXES
    ]

    df_wide = (
        df_slotted
        .groupBy("site_id", "hour_ts")
        .agg(*pivot_exprs)
        .withColumnRenamed("hour_ts", "timestamp")
        .withColumn("gold_date", F.to_date("timestamp"))
        .orderBy("site_id", "timestamp")
    )
    print(f"[ml-prep] Wide format         : {df_wide.count():,} rows, {len(df_wide.columns)} columns")

    df_wide.write.mode("overwrite").partitionBy("gold_date").parquet(ML_INPUT_PATH)
    print(f"[ml-prep] ML input written -> {ML_INPUT_PATH}")


if __name__ == "__main__":
    build_ml_input()
    print("\nJob complete. ML input refreshed.")
