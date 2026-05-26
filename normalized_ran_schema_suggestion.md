# Suggested Normalized RAN Telemetry Tables

## Recommendation

The best middle-ground design is **8 normalized silver tables**.

This is better than the 13/14-table version because it avoids creating too many small tables. It is also better than the 5-table version because it does not merge very different things into wide tables full of null values.

## Proposed Tables

| # | Table | Granularity | Purpose |
|---|---|---|---|
| 1 | `site_snapshot` | 1 row per site snapshot | Site metadata, environment state, generator state, alert summary |
| 2 | `environment_sensors` | 1 row per sensor per snapshot | Temperature and humidity sensors together |
| 3 | `cells` | 1 row per cell per snapshot | Main radio KPIs and service quality |
| 4 | `antennas` | 1 row per antenna per snapshot | Antenna RF and physical configuration |
| 5 | `radio_units` | 1 row per RU per snapshot | Radio unit power, signal, temperature, VSWR |
| 6 | `baseband_units` | 1 row per BBU per snapshot | Compute and latency metrics |
| 7 | `power_system` | 1 row per power component per snapshot | Batteries, rectifiers, and generator |
| 8 | `alerts` | 1 row per alert | Alert events linked to components |

## Why This Design

- `site_snapshot` keeps one clean row for each tower snapshot.
- `environment_sensors` combines temperature and humidity because they are both simple shelter sensors.
- `cells` stays separate because it is the most important analytical table.
- `antennas`, `radio_units`, and `baseband_units` stay separate because they describe different parts of the radio network.
- `power_system` combines batteries, rectifiers, and generator because they belong to the same power domain.
- `alerts` stays separate because alerts are event-style records and can point to any component.

## 1. `site_snapshot`

| message_id | snapshot_time | site_id | site_name | region | vendor | technologies | env_status | gen_status | total_alerts | highest_severity |
|---|---|---|---|---|---|---|---|---|---:|---|
| msg-001 | 2024-06-01 08:00:00 | SITE_EG_01 | Cairo North Tower | cairo | Ericsson | LTE, NR | OK | STANDBY | 2 | WARNING |
| msg-002 | 2024-06-01 08:05:00 | SITE_EG_01 | Cairo North Tower | cairo | Ericsson | LTE, NR | OK | STANDBY | 0 | NONE |
| msg-003 | 2024-06-01 08:10:00 | SITE_EG_02 | Giza West Tower | giza | Huawei | LTE | WARNING | ON | 3 | CRITICAL |
| msg-004 | 2024-06-01 08:15:00 | SITE_EG_03 | Alex Coast Tower | alex | Nokia | LTE, NR | OK | STANDBY | 1 | INFO |
| msg-005 | 2024-06-01 08:20:00 | SITE_EG_04 | Mansoura Core Tower | dakahlia | Ericsson | NR | OK | STANDBY | 1 | WARNING |

## 2. `environment_sensors`

| message_id | site_id | sensor_type | sensor_id | value | unit | status |
|---|---|---|---|---:|---|---|
| msg-001 | SITE_EG_01 | TEMPERATURE | TEMP_1 | 37.8 | C | OK |
| msg-001 | SITE_EG_01 | TEMPERATURE | TEMP_2 | 38.2 | C | HIGH |
| msg-001 | SITE_EG_01 | HUMIDITY | HUM_1 | 62.4 | percent | OK |
| msg-003 | SITE_EG_02 | TEMPERATURE | TEMP_1 | 44.6 | C | HIGH |
| msg-003 | SITE_EG_02 | HUMIDITY | HUM_1 | 81.2 | percent | HIGH |

## 3. `cells`

| message_id | site_id | cell_id | sector_id | technology | cell_status | connected_users | dl_mbps | sinr_db | call_drop_pct |
|---|---|---|---|---|---|---:|---:|---:|---:|
| msg-001 | SITE_EG_01 | CELL_LTE_1 | SEC_1 | LTE | UP | 142 | 87.3 | 14.2 | 0.12 |
| msg-001 | SITE_EG_01 | CELL_LTE_2 | SEC_2 | LTE | UP | 110 | 64.8 | 12.8 | 0.08 |
| msg-001 | SITE_EG_01 | CELL_NR_1 | SEC_3 | NR | UP | 45 | 410.5 | 18.6 | 0.03 |
| msg-003 | SITE_EG_02 | CELL_LTE_1 | SEC_1 | LTE | DOWN | 0 | 0.0 | null | null |
| msg-005 | SITE_EG_04 | CELL_NR_1 | SEC_1 | NR | UP | 70 | 520.2 | 20.1 | 0.02 |

## 4. `antennas`

| message_id | site_id | antenna_id | sector_id | status | op_state | mimo_layers | azimuth_degree | tilt_degree | rssi_dbm | snr_db |
|---|---|---|---|---|---|---:|---:|---:|---:|---:|
| msg-001 | SITE_EG_01 | ANT_1 | SEC_1 | UP | HEALTHY | 4 | 0 | 3.0 | -65.2 | 18.4 |
| msg-001 | SITE_EG_01 | ANT_2 | SEC_2 | UP | HEALTHY | 4 | 120 | 3.5 | -70.1 | 15.2 |
| msg-001 | SITE_EG_01 | ANT_3 | SEC_3 | UP | HEALTHY | 8 | 240 | 2.5 | -68.5 | 17.1 |
| msg-003 | SITE_EG_02 | ANT_1 | SEC_1 | DOWN | FAILED | 4 | 0 | 4.0 | null | null |
| msg-005 | SITE_EG_04 | ANT_1 | SEC_1 | UP | DEGRADED | 8 | 30 | 2.0 | -75.4 | 10.8 |

## 5. `radio_units`

| message_id | site_id | ru_id | sector_id | status | op_state | tx_power_watts | rx_signal_dbm | temperature_c | voltage_volt | vswr |
|---|---|---|---|---|---|---:|---:|---:|---:|---:|
| msg-001 | SITE_EG_01 | RU_1 | SEC_1 | UP | HEALTHY | 40.0 | -66.1 | 32.4 | 48.1 | 1.35 |
| msg-001 | SITE_EG_01 | RU_2 | SEC_2 | UP | HEALTHY | 38.5 | -69.8 | 33.0 | 48.0 | 1.28 |
| msg-001 | SITE_EG_01 | RU_3 | SEC_3 | UP | HEALTHY | 41.2 | -67.3 | 34.1 | 48.2 | 1.41 |
| msg-003 | SITE_EG_02 | RU_1 | SEC_1 | UP | DEGRADED | 35.0 | -82.0 | 45.8 | 47.5 | 2.10 |
| msg-005 | SITE_EG_04 | RU_1 | SEC_1 | UP | HEALTHY | 43.0 | -63.7 | 31.2 | 48.3 | 1.22 |

## 6. `baseband_units`

| message_id | site_id | bbu_id | status | op_state | active_users | cpu_pct | memory_pct | disk_pct | control_latency_ms | user_latency_ms |
|---|---|---|---|---|---:|---:|---:|---:|---:|---:|
| msg-001 | SITE_EG_01 | BBU_MAIN | UP | HEALTHY | 211 | 72.4 | 64.1 | 55.3 | 8.2 | 12.5 |
| msg-002 | SITE_EG_01 | BBU_MAIN | UP | HEALTHY | 198 | 68.9 | 61.7 | 55.4 | 7.9 | 11.8 |
| msg-003 | SITE_EG_02 | BBU_MAIN | UP | DEGRADED | 95 | 91.5 | 82.4 | 70.1 | 18.6 | 30.2 |
| msg-004 | SITE_EG_03 | BBU_MAIN | UP | HEALTHY | 134 | 57.8 | 49.2 | 42.0 | 6.4 | 10.1 |
| msg-005 | SITE_EG_04 | BBU_MAIN | UP | HEALTHY | 70 | 45.3 | 40.6 | 38.9 | 5.8 | 8.4 |

## 7. `power_system`

| message_id | site_id | component_type | component_id | status | op_state | charge_pct | temp_c | current_ampere | voltage_volt | fuel_level_pct |
|---|---|---|---|---|---|---:|---:|---:|---:|---:|
| msg-001 | SITE_EG_01 | BATTERY | BAT_1 | UP | HEALTHY | 94.2 | 28.1 | null | null | null |
| msg-001 | SITE_EG_01 | BATTERY | BAT_2 | UP | HEALTHY | 91.7 | 27.4 | null | null | null |
| msg-001 | SITE_EG_01 | RECTIFIER | REC_1 | UP | HEALTHY | null | null | 45.3 | 48.2 | null |
| msg-003 | SITE_EG_02 | RECTIFIER | REC_1 | DOWN | FAILED | null | null | 0.0 | 0.0 | null |
| msg-003 | SITE_EG_02 | GENERATOR | GEN_MAIN | ON | ACTIVE | null | null | null | null | 18.5 |

## 8. `alerts`

| message_id | site_id | alert_id | severity | category | component_type | component_id | code | alert_value |
|---|---|---|---|---|---|---|---|---|
| msg-001 | SITE_EG_01 | ALT_001 | WARNING | performance | CELL | CELL_LTE_1 | HIGH_PRB | 68.4 |
| msg-001 | SITE_EG_01 | ALT_002 | WARNING | environment | SENSOR | TEMP_2 | HIGH_TEMP | 38.2 |
| msg-003 | SITE_EG_02 | ALT_003 | CRITICAL | power | RECTIFIER | REC_1 | COMPONENT_DOWN | DOWN |
| msg-003 | SITE_EG_02 | ALT_004 | CRITICAL | radio | CELL | CELL_LTE_1 | CELL_DOWN | DOWN |
| msg-005 | SITE_EG_04 | ALT_005 | WARNING | radio | ANTENNA | ANT_1 | LOW_SNR | 10.8 |

## Join Keys

Use `message_id` as the main join key between all tables.

Alternative join key:

```text
site_id + sequence_number
```

## Final Opinion

Use **8 tables**.

It is normalized enough for clean analytics, but not so normalized that every dashboard query needs many joins.
