# Revised Normalized RAN Telemetry Schema

## Recommendation

Use **10 normalized silver tables**.

This is the best middle ground between the very detailed 13/14-table design and the very compact 5-table design.

The important changes from the earlier 8-table idea are:

- Split `power_system` into separate `batteries` and `rectifiers` tables.
- Keep generator fields in `site_snapshot`, because there is usually only one generator per site snapshot.
- Keep `transport_links` as its own table, because it exists as a separate array in the real raw RAN JSON.
- Split alert values into numeric and string columns, because the raw `alerts.value` field contains mixed types.

The example rows below are **illustrative examples**, not rows extracted directly from `ran_telemetry+0+0000000340.json`. The schema design is based on the structure of that real file.

## Proposed Tables

| # | Table | Granularity | Purpose |
|---|---|---|---|
| 1 | `site_snapshot` | 1 row per site snapshot | Site metadata, environment state, generator state, alert summary |
| 2 | `environment_sensors` | 1 row per sensor per snapshot | Temperature and humidity sensors |
| 3 | `cells` | 1 row per cell per snapshot | Main radio KPIs and service quality |
| 4 | `antennas` | 1 row per antenna per snapshot | Antenna RF and physical configuration |
| 5 | `radio_units` | 1 row per RU per snapshot | Radio unit power, signal, temperature, VSWR |
| 6 | `baseband_units` | 1 row per BBU per snapshot | Compute and latency metrics |
| 7 | `batteries` | 1 row per battery per snapshot | Battery charge, temperature, and health |
| 8 | `rectifiers` | 1 row per rectifier per snapshot | Rectifier current, voltage, and health |
| 9 | `transport_links` | 1 row per transport link per snapshot | Backhaul link throughput, utilization, latency, jitter, packet loss |
| 10 | `alerts` | 1 row per alert | Alert events linked to sites/components |

## Shared Columns

Every table should include these columns:

```text
message_id
source_file
snapshot_time
sequence_number
site_id
site_name
region
```

This keeps the tables easy to join and easy to query by time.

The sample tables below are shortened for readability. In the real silver output, all tables should still include the shared columns above.

## Type Notes

- `technologies` should be `ARRAY<STRING>`, for example `["4G", "5G"]`, not a comma-separated string.
- Valid `op_state` values in the real data include `HEALTHY`, `DEGRADED`, `FAILED`, and `RECOVERING`.
- Do not use `WARNING` as an `op_state`; warning belongs in `alerts.severity`.
- The real battery objects currently have `charge_percent`, `temperature_c`, `battery_id`, `status`, and `op_state`. They do **not** currently include `voltage_volt`.
- Because raw alert values mix numbers and strings, store them as `alert_value_num DOUBLE` and `alert_value_str STRING`.

## Primary Keys

| Table | Suggested key |
|---|---|
| `site_snapshot` | `message_id` |
| `environment_sensors` | `message_id + sensor_type + sensor_id` |
| `cells` | `message_id + cell_id` |
| `antennas` | `message_id + antenna_id` |
| `radio_units` | `message_id + ru_id` |
| `baseband_units` | `message_id + bbu_id` |
| `batteries` | `message_id + battery_id` |
| `rectifiers` | `message_id + rectifier_id` |
| `transport_links` | `message_id + link_id` |
| `alerts` | `message_id + alert_id` |

## 1. `site_snapshot`

| message_id | snapshot_time | site_id | site_name | region | vendor | technologies | env_status | gen_status | fuel_level_pct | total_alerts | highest_severity |
|---|---|---|---|---|---|---|---|---|---:|---:|---|
| msg-001 | 2024-06-01 08:00:00 | SITE_EG_01 | Cairo North Tower | cairo | Ericsson | ["4G", "5G"] | UP | OFF | 87.5 | 2 | WARNING |
| msg-002 | 2024-06-01 08:05:00 | SITE_EG_01 | Cairo North Tower | cairo | Ericsson | ["4G", "5G"] | UP | OFF | 87.4 | 0 | NONE |
| msg-003 | 2024-06-01 08:10:00 | SITE_EG_02 | Giza West Tower | giza | Huawei | ["4G"] | UP | ON | 18.5 | 3 | CRITICAL |
| msg-004 | 2024-06-01 08:15:00 | SITE_EG_03 | Alex Coast Tower | alex | Nokia | ["4G", "5G"] | UP | OFF | 76.2 | 1 | INFO |
| msg-005 | 2024-06-01 08:20:00 | SITE_EG_04 | Mansoura Core Tower | dakahlia | Ericsson | ["5G"] | UP | OFF | 92.1 | 1 | WARNING |
| msg-006 | 2024-06-01 08:25:00 | SITE_EG_05 | Tanta Metro Tower | gharbia | Huawei | ["4G"] | UP | OFF | 69.8 | 0 | NONE |
| msg-007 | 2024-06-01 08:30:00 | SITE_EG_06 | Luxor East Tower | luxor | Nokia | ["4G"] | UP | ON | 35.0 | 2 | WARNING |
| msg-008 | 2024-06-01 08:35:00 | SITE_EG_07 | Aswan South Tower | aswan | Ericsson | ["4G", "5G"] | UP | OFF | 83.3 | 0 | NONE |
| msg-009 | 2024-06-01 08:40:00 | SITE_EG_08 | Suez Port Tower | suez | Huawei | ["5G"] | UP | OFF | 74.6 | 1 | WARNING |
| msg-010 | 2024-06-01 08:45:00 | SITE_EG_09 | Ismailia Canal Tower | ismailia | Nokia | ["4G", "5G"] | DOWN | ON | 12.4 | 4 | CRITICAL |

## 2. `environment_sensors`

| message_id | snapshot_time | site_id | sensor_type | sensor_id | value | unit | status |
|---|---|---|---|---|---:|---|---|
| msg-001 | 2024-06-01 08:00:00 | SITE_EG_01 | TEMPERATURE | TEMP_1 | 37.8 | C | OK |
| msg-001 | 2024-06-01 08:00:00 | SITE_EG_01 | TEMPERATURE | TEMP_2 | 38.2 | C | HIGH |
| msg-001 | 2024-06-01 08:00:00 | SITE_EG_01 | HUMIDITY | HUM_1 | 62.4 | percent | OK |
| msg-003 | 2024-06-01 08:10:00 | SITE_EG_02 | TEMPERATURE | TEMP_1 | 44.6 | C | HIGH |
| msg-003 | 2024-06-01 08:10:00 | SITE_EG_02 | HUMIDITY | HUM_1 | 81.2 | percent | HIGH |
| msg-004 | 2024-06-01 08:15:00 | SITE_EG_03 | TEMPERATURE | TEMP_1 | 31.5 | C | OK |
| msg-005 | 2024-06-01 08:20:00 | SITE_EG_04 | HUMIDITY | HUM_1 | 55.8 | percent | OK |
| msg-007 | 2024-06-01 08:30:00 | SITE_EG_06 | TEMPERATURE | TEMP_2 | 41.1 | C | HIGH |
| msg-008 | 2024-06-01 08:35:00 | SITE_EG_07 | HUMIDITY | HUM_1 | 48.3 | percent | OK |
| msg-010 | 2024-06-01 08:45:00 | SITE_EG_09 | TEMPERATURE | TEMP_1 | 46.7 | C | CRITICAL |

## 3. `cells`

| message_id | snapshot_time | site_id | cell_id | sector_id | technology | cell_status | connected_users | dl_mbps | sinr_db | call_drop_pct |
|---|---|---|---|---|---|---|---:|---:|---:|---:|
| msg-001 | 2024-06-01 08:00:00 | SITE_EG_01 | CELL_LTE_1 | SEC_1 | LTE | UP | 142 | 87.3 | 14.2 | 0.12 |
| msg-001 | 2024-06-01 08:00:00 | SITE_EG_01 | CELL_LTE_2 | SEC_2 | LTE | UP | 110 | 64.8 | 12.8 | 0.08 |
| msg-001 | 2024-06-01 08:00:00 | SITE_EG_01 | CELL_NR_1 | SEC_3 | NR | UP | 45 | 410.5 | 18.6 | 0.03 |
| msg-003 | 2024-06-01 08:10:00 | SITE_EG_02 | CELL_LTE_1 | SEC_1 | LTE | DOWN | 0 | 0.0 | null | null |
| msg-005 | 2024-06-01 08:20:00 | SITE_EG_04 | CELL_NR_1 | SEC_1 | NR | UP | 70 | 520.2 | 20.1 | 0.02 |
| msg-006 | 2024-06-01 08:25:00 | SITE_EG_05 | CELL_LTE_1 | SEC_1 | LTE | UP | 98 | 72.4 | 13.1 | 0.10 |
| msg-007 | 2024-06-01 08:30:00 | SITE_EG_06 | CELL_LTE_2 | SEC_2 | LTE | UP | 156 | 93.8 | 11.9 | 0.21 |
| msg-008 | 2024-06-01 08:35:00 | SITE_EG_07 | CELL_NR_1 | SEC_1 | NR | UP | 88 | 610.7 | 22.4 | 0.01 |
| msg-009 | 2024-06-01 08:40:00 | SITE_EG_08 | CELL_NR_2 | SEC_2 | NR | DEGRADED | 61 | 280.4 | 9.8 | 0.18 |
| msg-010 | 2024-06-01 08:45:00 | SITE_EG_09 | CELL_LTE_1 | SEC_1 | LTE | DOWN | 0 | 0.0 | null | null |

## 4. `antennas`

| message_id | snapshot_time | site_id | antenna_id | sector_id | status | op_state | mimo_layers | azimuth_degree | tilt_degree | rssi_dbm | snr_db |
|---|---|---|---|---|---|---|---:|---:|---:|---:|---:|
| msg-001 | 2024-06-01 08:00:00 | SITE_EG_01 | ANT_1 | SEC_1 | UP | HEALTHY | 4 | 0 | 3.0 | -65.2 | 18.4 |
| msg-001 | 2024-06-01 08:00:00 | SITE_EG_01 | ANT_2 | SEC_2 | UP | HEALTHY | 4 | 120 | 3.5 | -70.1 | 15.2 |
| msg-001 | 2024-06-01 08:00:00 | SITE_EG_01 | ANT_3 | SEC_3 | UP | HEALTHY | 8 | 240 | 2.5 | -68.5 | 17.1 |
| msg-003 | 2024-06-01 08:10:00 | SITE_EG_02 | ANT_1 | SEC_1 | DOWN | FAILED | 4 | 0 | 4.0 | null | null |
| msg-005 | 2024-06-01 08:20:00 | SITE_EG_04 | ANT_1 | SEC_1 | UP | DEGRADED | 8 | 30 | 2.0 | -75.4 | 10.8 |
| msg-006 | 2024-06-01 08:25:00 | SITE_EG_05 | ANT_1 | SEC_1 | UP | HEALTHY | 4 | 60 | 3.2 | -67.0 | 16.3 |
| msg-007 | 2024-06-01 08:30:00 | SITE_EG_06 | ANT_2 | SEC_2 | UP | HEALTHY | 4 | 180 | 4.1 | -71.2 | 14.7 |
| msg-008 | 2024-06-01 08:35:00 | SITE_EG_07 | ANT_1 | SEC_1 | UP | HEALTHY | 8 | 90 | 2.8 | -62.9 | 21.5 |
| msg-009 | 2024-06-01 08:40:00 | SITE_EG_08 | ANT_2 | SEC_2 | UP | DEGRADED | 8 | 210 | 3.6 | -78.1 | 9.9 |
| msg-010 | 2024-06-01 08:45:00 | SITE_EG_09 | ANT_1 | SEC_1 | DOWN | FAILED | 4 | 0 | 4.5 | null | null |

## 5. `radio_units`

| message_id | snapshot_time | site_id | ru_id | sector_id | status | op_state | tx_power_watts | rx_signal_dbm | temperature_c | voltage_volt | vswr |
|---|---|---|---|---|---|---|---:|---:|---:|---:|---:|
| msg-001 | 2024-06-01 08:00:00 | SITE_EG_01 | RU_1 | SEC_1 | UP | HEALTHY | 40.0 | -66.1 | 32.4 | 48.1 | 1.35 |
| msg-001 | 2024-06-01 08:00:00 | SITE_EG_01 | RU_2 | SEC_2 | UP | HEALTHY | 38.5 | -69.8 | 33.0 | 48.0 | 1.28 |
| msg-001 | 2024-06-01 08:00:00 | SITE_EG_01 | RU_3 | SEC_3 | UP | HEALTHY | 41.2 | -67.3 | 34.1 | 48.2 | 1.41 |
| msg-003 | 2024-06-01 08:10:00 | SITE_EG_02 | RU_1 | SEC_1 | UP | DEGRADED | 35.0 | -82.0 | 45.8 | 47.5 | 2.10 |
| msg-005 | 2024-06-01 08:20:00 | SITE_EG_04 | RU_1 | SEC_1 | UP | HEALTHY | 43.0 | -63.7 | 31.2 | 48.3 | 1.22 |
| msg-006 | 2024-06-01 08:25:00 | SITE_EG_05 | RU_1 | SEC_1 | UP | HEALTHY | 39.5 | -68.8 | 33.9 | 48.0 | 1.31 |
| msg-007 | 2024-06-01 08:30:00 | SITE_EG_06 | RU_2 | SEC_2 | UP | DEGRADED | 36.2 | -76.3 | 42.0 | 47.8 | 1.84 |
| msg-008 | 2024-06-01 08:35:00 | SITE_EG_07 | RU_1 | SEC_1 | UP | HEALTHY | 44.5 | -61.2 | 30.8 | 48.4 | 1.18 |
| msg-009 | 2024-06-01 08:40:00 | SITE_EG_08 | RU_2 | SEC_2 | UP | DEGRADED | 34.0 | -80.5 | 43.6 | 47.6 | 2.02 |
| msg-010 | 2024-06-01 08:45:00 | SITE_EG_09 | RU_1 | SEC_1 | DOWN | FAILED | 0.0 | null | 47.9 | 0.0 | null |

## 6. `baseband_units`

| message_id | snapshot_time | site_id | bbu_id | status | op_state | active_users | cpu_pct | memory_pct | disk_pct | control_latency_ms | user_latency_ms |
|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|
| msg-001 | 2024-06-01 08:00:00 | SITE_EG_01 | BBU_MAIN | UP | HEALTHY | 211 | 72.4 | 64.1 | 55.3 | 8.2 | 12.5 |
| msg-002 | 2024-06-01 08:05:00 | SITE_EG_01 | BBU_MAIN | UP | HEALTHY | 198 | 68.9 | 61.7 | 55.4 | 7.9 | 11.8 |
| msg-003 | 2024-06-01 08:10:00 | SITE_EG_02 | BBU_MAIN | UP | DEGRADED | 95 | 91.5 | 82.4 | 70.1 | 18.6 | 30.2 |
| msg-004 | 2024-06-01 08:15:00 | SITE_EG_03 | BBU_MAIN | UP | HEALTHY | 134 | 57.8 | 49.2 | 42.0 | 6.4 | 10.1 |
| msg-005 | 2024-06-01 08:20:00 | SITE_EG_04 | BBU_MAIN | UP | HEALTHY | 70 | 45.3 | 40.6 | 38.9 | 5.8 | 8.4 |
| msg-006 | 2024-06-01 08:25:00 | SITE_EG_05 | BBU_MAIN | UP | HEALTHY | 98 | 61.0 | 52.8 | 44.1 | 7.1 | 10.9 |
| msg-007 | 2024-06-01 08:30:00 | SITE_EG_06 | BBU_MAIN | UP | DEGRADED | 156 | 88.2 | 79.5 | 66.4 | 15.2 | 24.8 |
| msg-008 | 2024-06-01 08:35:00 | SITE_EG_07 | BBU_MAIN | UP | HEALTHY | 88 | 49.9 | 41.0 | 35.2 | 5.9 | 8.8 |
| msg-009 | 2024-06-01 08:40:00 | SITE_EG_08 | BBU_MAIN | UP | HEALTHY | 61 | 53.7 | 46.6 | 39.8 | 6.8 | 9.7 |
| msg-010 | 2024-06-01 08:45:00 | SITE_EG_09 | BBU_MAIN | UP | CRITICAL | 40 | 96.3 | 90.1 | 78.5 | 28.4 | 41.9 |

## 7. `batteries`

| message_id | snapshot_time | site_id | battery_id | status | op_state | charge_pct | temperature_c |
|---|---|---|---|---|---|---:|---:|
| msg-001 | 2024-06-01 08:00:00 | SITE_EG_01 | BAT_1 | UP | HEALTHY | 94.2 | 28.1 |
| msg-001 | 2024-06-01 08:00:00 | SITE_EG_01 | BAT_2 | UP | HEALTHY | 91.7 | 27.4 |
| msg-003 | 2024-06-01 08:10:00 | SITE_EG_02 | BAT_1 | UP | DEGRADED | 42.8 | 36.9 |
| msg-003 | 2024-06-01 08:10:00 | SITE_EG_02 | BAT_2 | UP | DEGRADED | 39.5 | 37.2 |
| msg-005 | 2024-06-01 08:20:00 | SITE_EG_04 | BAT_1 | UP | HEALTHY | 96.0 | 26.8 |
| msg-006 | 2024-06-01 08:25:00 | SITE_EG_05 | BAT_1 | UP | HEALTHY | 88.4 | 29.1 |
| msg-007 | 2024-06-01 08:30:00 | SITE_EG_06 | BAT_1 | UP | DEGRADED | 55.2 | 34.6 |
| msg-008 | 2024-06-01 08:35:00 | SITE_EG_07 | BAT_1 | UP | HEALTHY | 90.7 | 27.9 |
| msg-009 | 2024-06-01 08:40:00 | SITE_EG_08 | BAT_1 | UP | HEALTHY | 84.3 | 30.2 |
| msg-010 | 2024-06-01 08:45:00 | SITE_EG_09 | BAT_1 | DOWN | FAILED | 8.1 | 45.0 |

## 8. `rectifiers`

| message_id | snapshot_time | site_id | rectifier_id | status | op_state | current_ampere | output_voltage_volt |
|---|---|---|---|---|---|---:|---:|
| msg-001 | 2024-06-01 08:00:00 | SITE_EG_01 | REC_1 | UP | HEALTHY | 45.3 | 48.2 |
| msg-001 | 2024-06-01 08:00:00 | SITE_EG_01 | REC_2 | UP | HEALTHY | 42.8 | 48.1 |
| msg-003 | 2024-06-01 08:10:00 | SITE_EG_02 | REC_1 | DOWN | FAILED | 0.0 | 0.0 |
| msg-003 | 2024-06-01 08:10:00 | SITE_EG_02 | REC_2 | UP | DEGRADED | 31.2 | 46.9 |
| msg-005 | 2024-06-01 08:20:00 | SITE_EG_04 | REC_1 | UP | HEALTHY | 40.6 | 48.0 |
| msg-006 | 2024-06-01 08:25:00 | SITE_EG_05 | REC_1 | UP | HEALTHY | 38.9 | 48.4 |
| msg-007 | 2024-06-01 08:30:00 | SITE_EG_06 | REC_1 | UP | WARNING | 34.0 | 47.2 |
| msg-008 | 2024-06-01 08:35:00 | SITE_EG_07 | REC_1 | UP | HEALTHY | 44.1 | 48.3 |
| msg-009 | 2024-06-01 08:40:00 | SITE_EG_08 | REC_1 | UP | HEALTHY | 39.7 | 48.1 |
| msg-010 | 2024-06-01 08:45:00 | SITE_EG_09 | REC_1 | DOWN | FAILED | 0.0 | 0.0 |

## 9. `transport_links`

| message_id | snapshot_time | site_id | link_id | link_type | status | op_state | throughput_mbps | utilization_percent | latency_ms | jitter_ms | packet_loss_percent |
|---|---|---|---|---|---|---|---:|---:|---:|---:|---:|
| msg-001 | 2024-06-01 08:00:00 | SITE_EG_01 | BH_1 | FIBER | UP | HEALTHY | 4807.04 | 23.31 | 4.71 | 1.07 | 0.03 |
| msg-001 | 2024-06-01 08:00:00 | SITE_EG_01 | BH_2 | MICROWAVE | UP | HEALTHY | 8176.97 | 24.20 | 6.16 | 0.86 | 0.02 |
| msg-002 | 2024-06-01 08:05:00 | SITE_EG_02 | BH_1 | FIBER | UP | HEALTHY | 5412.87 | 23.03 | 4.80 | 1.36 | 0.02 |
| msg-002 | 2024-06-01 08:05:00 | SITE_EG_02 | BH_2 | MICROWAVE | UP | HEALTHY | 8626.76 | 5.24 | 2.82 | 0.15 | 0.01 |
| msg-003 | 2024-06-01 08:10:00 | SITE_EG_03 | BH_1 | FIBER | UP | HEALTHY | 3648.00 | 17.09 | 4.17 | 0.43 | 0.02 |
| msg-003 | 2024-06-01 08:10:00 | SITE_EG_03 | BH_2 | MICROWAVE | UP | HEALTHY | 2243.94 | 5.40 | 2.14 | 0.56 | 0.01 |
| msg-004 | 2024-06-01 08:15:00 | SITE_EG_04 | BH_1 | FIBER | UP | DEGRADED | 216.39 | 47.76 | 8.59 | 2.52 | 0.05 |
| msg-004 | 2024-06-01 08:15:00 | SITE_EG_04 | BH_2 | MICROWAVE | DOWN | FAILED | null | null | null | null | null |
| msg-005 | 2024-06-01 08:20:00 | SITE_EG_05 | BH_1 | FIBER | UP | HEALTHY | 4023.33 | 11.66 | 3.55 | 0.42 | 0.01 |
| msg-005 | 2024-06-01 08:20:00 | SITE_EG_05 | BH_2 | MICROWAVE | UP | HEALTHY | 4310.79 | 8.84 | 3.52 | 0.22 | 0.01 |

## 10. `alerts`

| message_id | snapshot_time | site_id | alert_id | severity | category | component_type | component_id | code | alert_value_num | alert_value_str |
|---|---|---|---|---|---|---|---|---|---:|---|
| msg-001 | 2024-06-01 08:00:00 | SITE_EG_01 | ALT_001 | WARNING | performance | CELL | CELL_LTE_1 | HIGH_PRB | 68.4 | null |
| msg-001 | 2024-06-01 08:00:00 | SITE_EG_01 | ALT_002 | WARNING | environment | SENSOR | TEMP_2 | HIGH_TEMP | 38.2 | null |
| msg-003 | 2024-06-01 08:10:00 | SITE_EG_02 | ALT_003 | CRITICAL | power | RECTIFIER | REC_1 | COMPONENT_DOWN | null | DOWN |
| msg-003 | 2024-06-01 08:10:00 | SITE_EG_02 | ALT_004 | CRITICAL | radio | CELL | CELL_LTE_1 | CELL_DOWN | null | DOWN |
| msg-005 | 2024-06-01 08:20:00 | SITE_EG_04 | ALT_005 | WARNING | radio | ANTENNA | ANT_1 | LOW_SNR | 10.8 | null |
| msg-007 | 2024-06-01 08:30:00 | SITE_EG_06 | ALT_006 | WARNING | radio | RADIO_UNIT | RU_2 | HIGH_VSWR | 1.84 | null |
| msg-007 | 2024-06-01 08:30:00 | SITE_EG_06 | ALT_007 | WARNING | compute | BBU | BBU_MAIN | HIGH_CPU | 88.2 | null |
| msg-009 | 2024-06-01 08:40:00 | SITE_EG_08 | ALT_008 | WARNING | radio | RADIO_UNIT | RU_2 | HIGH_VSWR | 2.02 | null |
| msg-010 | 2024-06-01 08:45:00 | SITE_EG_09 | ALT_009 | CRITICAL | power | BATTERY | BAT_1 | BATTERY_FAILED | null | FAILED |
| msg-010 | 2024-06-01 08:45:00 | SITE_EG_09 | ALT_010 | CRITICAL | environment | SENSOR | TEMP_1 | CRITICAL_TEMP | 46.7 | null |

## Final Opinion

Use the **10-table design**.

It keeps the schema normalized, avoids very sparse merged component tables, includes all major arrays from the real RAN JSON, and still stays simple enough for dashboards and Athena/Spark analysis.
