-- =====================================================================
-- Tower Health — Snowflake Gold Layer Setup  (plain SQL)
-- Converted from gold_setup notebook. Run top-to-bottom in a worksheet.
--
-- Changes vs the notebook:
--   * Removed dim_sector and dim_technology (dropped from the project).
--   * Stripped %%sql notebook magics.
--   * Added DROP statements up front for a clean rebuild.
--   * Validation join uses the inferred, double-quoted columns.
--
-- Result: 11 external tables (7 dims + 4 facts).
-- Prereqs already created: storage integration S3_GOLD_INT, stage GOLD_S3_STAGE.
-- =====================================================================

-- ---------------------------------------------------------------------
-- 0. Context
-- ---------------------------------------------------------------------
USE DATABASE YOUR_SNOWFLAKE_DATABASE;
USE SCHEMA PUBLIC;

-- ---------------------------------------------------------------------
-- 1. Clean slate (safe to run even if objects don't exist)
--    View first — it depends on DIM_RU.
-- ---------------------------------------------------------------------
DROP VIEW IF EXISTS DIM_RU_V;

DROP EXTERNAL TABLE IF EXISTS Fact_Alarms;
DROP EXTERNAL TABLE IF EXISTS Fact_Cells;
DROP EXTERNAL TABLE IF EXISTS Fact_RAN;
DROP EXTERNAL TABLE IF EXISTS Fact_ML_Predictions;
DROP EXTERNAL TABLE IF EXISTS dim_site;
DROP EXTERNAL TABLE IF EXISTS dim_cell;
DROP EXTERNAL TABLE IF EXISTS dim_date;
DROP EXTERNAL TABLE IF EXISTS dim_time;
DROP EXTERNAL TABLE IF EXISTS dim_RU;
DROP EXTERNAL TABLE IF EXISTS dim_Antenna;
DROP EXTERNAL TABLE IF EXISTS dim_Link;
-- (old leftovers, harmless if absent)
DROP EXTERNAL TABLE IF EXISTS dim_sector;
DROP EXTERNAL TABLE IF EXISTS dim_technology;

-- ---------------------------------------------------------------------
-- 2. File format for all Parquet external tables
-- ---------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT TOWER_PARQUET_FMT
  TYPE = PARQUET;

-- ---------------------------------------------------------------------
-- 3. Optional sanity checks (uncomment if you want to verify the stage)
-- ---------------------------------------------------------------------
-- LIST @GOLD_S3_STAGE/ran_telemetry_bi/;
-- SELECT *
-- FROM TABLE(INFER_SCHEMA(
--   LOCATION => '@GOLD_S3_STAGE/ran_telemetry_bi/dim_site/',
--   FILE_FORMAT => 'TOWER_PARQUET_FMT'));

-- =====================================================================
-- 4. DIMENSION TABLES (7)
-- =====================================================================

-- dim_site — 4 Egyptian tower locations (Alexandria, Cairo, Giza, North Sinai)
CREATE OR REPLACE EXTERNAL TABLE dim_site
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(INFER_SCHEMA(
      LOCATION => '@GOLD_S3_STAGE/ran_telemetry_bi/dim_site/',
      FILE_FORMAT => 'TOWER_PARQUET_FMT'))
  )
  LOCATION = @GOLD_S3_STAGE/ran_telemetry_bi/dim_site/
  FILE_FORMAT = TOWER_PARQUET_FMT
  AUTO_REFRESH = FALSE
  PATTERN = '.*[.]parquet';

-- dim_cell — 12 cells across all sites (4G + 5G)
CREATE OR REPLACE EXTERNAL TABLE dim_cell
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(INFER_SCHEMA(
      LOCATION => '@GOLD_S3_STAGE/ran_telemetry_bi/dim_cell/',
      FILE_FORMAT => 'TOWER_PARQUET_FMT'))
  )
  LOCATION = @GOLD_S3_STAGE/ran_telemetry_bi/dim_cell/
  FILE_FORMAT = TOWER_PARQUET_FMT
  AUTO_REFRESH = FALSE
  PATTERN = '.*[.]parquet';

-- dim_date — calendar dimension
CREATE OR REPLACE EXTERNAL TABLE dim_date
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(INFER_SCHEMA(
      LOCATION => '@GOLD_S3_STAGE/ran_telemetry_bi/dim_date/',
      FILE_FORMAT => 'TOWER_PARQUET_FMT'))
  )
  LOCATION = @GOLD_S3_STAGE/ran_telemetry_bi/dim_date/
  FILE_FORMAT = TOWER_PARQUET_FMT
  AUTO_REFRESH = FALSE
  PATTERN = '.*[.]parquet';

-- dim_time — time-of-day dimension
CREATE OR REPLACE EXTERNAL TABLE dim_time
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(INFER_SCHEMA(
      LOCATION => '@GOLD_S3_STAGE/ran_telemetry_bi/dim_time/',
      FILE_FORMAT => 'TOWER_PARQUET_FMT'))
  )
  LOCATION = @GOLD_S3_STAGE/ran_telemetry_bi/dim_time/
  FILE_FORMAT = TOWER_PARQUET_FMT
  AUTO_REFRESH = FALSE
  PATTERN = '.*[.]parquet';

-- dim_RU — Radio Unit dimension
CREATE OR REPLACE EXTERNAL TABLE dim_RU
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(INFER_SCHEMA(
      LOCATION => '@GOLD_S3_STAGE/ran_telemetry_bi/dim_RU/',
      FILE_FORMAT => 'TOWER_PARQUET_FMT'))
  )
  LOCATION = @GOLD_S3_STAGE/ran_telemetry_bi/dim_RU/
  FILE_FORMAT = TOWER_PARQUET_FMT
  AUTO_REFRESH = FALSE
  PATTERN = '.*[.]parquet';

-- dim_Antenna — Antenna dimension (MIMO config)
CREATE OR REPLACE EXTERNAL TABLE dim_Antenna
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(INFER_SCHEMA(
      LOCATION => '@GOLD_S3_STAGE/ran_telemetry_bi/dim_Antenna/',
      FILE_FORMAT => 'TOWER_PARQUET_FMT'))
  )
  LOCATION = @GOLD_S3_STAGE/ran_telemetry_bi/dim_Antenna/
  FILE_FORMAT = TOWER_PARQUET_FMT
  AUTO_REFRESH = FALSE
  PATTERN = '.*[.]parquet';

-- dim_Link — Backhaul link dimension (Fiber / Microwave)
CREATE OR REPLACE EXTERNAL TABLE dim_Link
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(INFER_SCHEMA(
      LOCATION => '@GOLD_S3_STAGE/ran_telemetry_bi/dim_Link/',
      FILE_FORMAT => 'TOWER_PARQUET_FMT'))
  )
  LOCATION = @GOLD_S3_STAGE/ran_telemetry_bi/dim_Link/
  FILE_FORMAT = TOWER_PARQUET_FMT
  AUTO_REFRESH = FALSE
  PATTERN = '.*[.]parquet';

-- =====================================================================
-- 5. FACT TABLES (Parquet, partitioned by gold_date)
-- =====================================================================

-- Fact_RAN — wide equipment-health fact, one row per 15-min snapshot per site
CREATE OR REPLACE EXTERNAL TABLE Fact_RAN
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(INFER_SCHEMA(
      LOCATION => '@GOLD_S3_STAGE/ran_telemetry_bi/Fact_RAN/',
      FILE_FORMAT => 'TOWER_PARQUET_FMT'))
  )
  LOCATION = @GOLD_S3_STAGE/ran_telemetry_bi/Fact_RAN/
  FILE_FORMAT = TOWER_PARQUET_FMT
  AUTO_REFRESH = FALSE
  PATTERN = '.*[.]parquet';

-- Fact_Cells — per-cell KPIs (RSRP/RSRQ/SINR/throughput/handover/etc.)
CREATE OR REPLACE EXTERNAL TABLE Fact_Cells
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(INFER_SCHEMA(
      LOCATION => '@GOLD_S3_STAGE/ran_telemetry_bi/Fact_Cells/',
      FILE_FORMAT => 'TOWER_PARQUET_FMT'))
  )
  LOCATION = @GOLD_S3_STAGE/ran_telemetry_bi/Fact_Cells/
  FILE_FORMAT = TOWER_PARQUET_FMT
  AUTO_REFRESH = FALSE
  PATTERN = '.*[.]parquet';

-- Fact_Alarms — one row per alarm event (carries site_key = md5(site_id) = site_sk)
CREATE OR REPLACE EXTERNAL TABLE Fact_Alarms
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(INFER_SCHEMA(
      LOCATION => '@GOLD_S3_STAGE/ran_telemetry_bi/Fact_Alarms/',
      FILE_FORMAT => 'TOWER_PARQUET_FMT'))
  )
  LOCATION = @GOLD_S3_STAGE/ran_telemetry_bi/Fact_Alarms/
  FILE_FORMAT = TOWER_PARQUET_FMT
  AUTO_REFRESH = FALSE
  PATTERN = '.*[.]parquet';

-- =====================================================================
-- 6. ML PREDICTIONS (CSV — explicit schema, date from filename)
-- =====================================================================
CREATE OR REPLACE FILE FORMAT CSV_PREDICTIONS
  TYPE = 'CSV'
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'null', 'NULL')
  EMPTY_FIELD_AS_NULL = TRUE;

CREATE OR REPLACE EXTERNAL TABLE Fact_ML_Predictions (
    TIMESTAMP_COL         TIMESTAMP_NTZ  AS (TO_TIMESTAMP_NTZ(VALUE:c1::STRING)),
    SITE_ID               STRING         AS (VALUE:c2::STRING),
    CELL_ID               STRING         AS (VALUE:c3::STRING),
    FAILURE_PROBABILITY   FLOAT          AS (VALUE:c4::FLOAT),
    PREDICTED_FAILURE     SMALLINT       AS (VALUE:c5::SMALLINT),
    RISK_LEVEL            STRING         AS (VALUE:c6::STRING),
    PREDICTION_DATE       DATE           AS (TO_DATE(LEFT(SPLIT_PART(METADATA$FILENAME, '/', -1), 10)))
)
  WITH LOCATION = @GOLD_S3_STAGE/ran_ml_predictions/
  FILE_FORMAT = (FORMAT_NAME = 'CSV_PREDICTIONS')
  PATTERN = '.*_predictions[.]csv'
  AUTO_REFRESH = FALSE;

-- =====================================================================
-- 7. VALIDATION
-- =====================================================================
SHOW EXTERNAL TABLES;

SELECT * FROM dim_site;

SELECT COUNT(*) AS total_alarms FROM Fact_Alarms;

-- THE alarm->site join. Should now return 4 towers (not empty).
SELECT s."site_name", s."region", COUNT(*) AS critical_alarm_count
FROM YOUR_SNOWFLAKE_DATABASE.PUBLIC.Fact_Alarms a
JOIN YOUR_SNOWFLAKE_DATABASE.PUBLIC.dim_site  s
  ON a."site_key" = s."site_sk"
WHERE a."severity" = 'CRITICAL'
GROUP BY s."site_name", s."region"
ORDER BY critical_alarm_count DESC;

-- =====================================================================
-- 8. REFRESH (use after future Spark/ML runs — AUTO_REFRESH is off)
-- =====================================================================
ALTER EXTERNAL TABLE Fact_RAN            REFRESH;
ALTER EXTERNAL TABLE Fact_Cells          REFRESH;
ALTER EXTERNAL TABLE Fact_Alarms         REFRESH;
ALTER EXTERNAL TABLE Fact_ML_Predictions REFRESH;
ALTER EXTERNAL TABLE dim_site            REFRESH;
ALTER EXTERNAL TABLE dim_cell            REFRESH;
ALTER EXTERNAL TABLE dim_date            REFRESH;
ALTER EXTERNAL TABLE dim_time            REFRESH;
ALTER EXTERNAL TABLE dim_RU              REFRESH;
ALTER EXTERNAL TABLE dim_Antenna         REFRESH;
ALTER EXTERNAL TABLE dim_Link            REFRESH;
