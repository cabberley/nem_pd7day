# NEM PD7DAY Price Forecast — Home Assistant Integration

[![HACS Custom](https://img.shields.io/badge/HACS-Custom-orange.svg)](https://github.com/hacs/integration)
[![HA Version](https://img.shields.io/badge/Home%20Assistant-%3E%3D2024.1-blue.svg)](https://www.home-assistant.io/)
[![Version](https://img.shields.io/github/v/release/purcell-lab/nem_pd7day)](https://github.com/purcell-lab/nem_pd7day/releases)

A Home Assistant custom integration that fetches AEMO's **7-day ahead electricity price forecasts** (PD7DAY) for the National Electricity Market (NEM) and exposes them as HA sensors with machine-learning calibration.

AEMO publishes PD7DAY three times per day (07:30, 13:00, 18:00 AEST). This integration fetches those updates on the same schedule and applies an on-device calibration layer — using your local history of forecast vs actual prices — to produce calibrated estimates with P10/P50/P90 confidence bands.

---

## Features

- **7-day price forecast** — calibrated $/kWh for QLD1 (or any NEM region)
- **Confidence bands** — P10, P50, P90 quantile regression (IRLS) per forecast period
- **OLS calibration** — linear bias correction fitted on actual Amber vs PD7DAY pairs
- **Gas generation forecast** — daily TJ forecast from MARKET_SUMMARY
- **Interconnector flows** — NSW1-QLD1 and N-Q-MNSP1 MW forecasts
- **Market intervention flag** — binary sensor from CASESOLUTION data
- **Calibration diagnostic** — observation count, active bucket count, fit quality
- **No polling** — fetches only at AEMO publish times (3 requests/day)
- **Pure Python** — zero external dependencies beyond Home Assistant

---

## Requirements

- Home Assistant 2024.1 or later
- An [Amber Electric](https://www.amber.com.au/) account with the [Amber integration](https://www.home-assistant.io/integrations/ambee/) configured (required for calibration; the integration works in passthrough mode without it)
- Network access to `www.nemweb.com.au`

---

## Installation

### Via HACS (recommended)

1. Open HACS in your HA instance
2. Go to **Integrations → Custom repositories**
3. Add `https://github.com/purcell-lab/nem_pd7day` with category **Integration**
4. Search for **NEM PD7DAY** and install
5. Restart Home Assistant
6. Go to **Settings → Devices & Services → Add Integration** and search for **NEM PD7DAY**

### Manual

1. Download the latest release zip from the [Releases page](https://github.com/purcell-lab/nem_pd7day/releases)
2. Extract `custom_components/nem_pd7day/` into your HA config directory
3. Restart Home Assistant
4. Go to **Settings → Devices & Services → Add Integration** and search for **NEM PD7DAY**

---

## Configuration

The integration is configured via the UI config flow. You will be prompted for:

| Field | Default | Description |
|---|---|---|
| Region | `QLD1` | NEM region code (`QLD1`, `NSW1`, `VIC1`, `SA1`, `TAS1`) |
| Interconnectors | `NSW1-QLD1, N-Q-MNSP1` | Comma-separated interconnector IDs to monitor |

No `configuration.yaml` entries are required.

### Recorder exclusion (recommended)

The forecast sensors carry large attribute payloads (7 days × 48 intervals). Add the following to `configuration.yaml` to prevent recorder warnings and database bloat:

```yaml
recorder:
  exclude:
    entity_globs:
      - sensor.pd7day_ic_*
    entities:
      - sensor.qld1_pd7day_forecast
      - sensor.qld1_pd7day_forecast_day_2_plus
      - sensor.qld1_pd7day_forecast_after_amber
      - sensor.qld1_pd7day_buy_cost_after_amber
```

---

## Sensors

### `sensor.qld1_pd7day_forecast`

The primary price forecast sensor.

| Attribute | Description |
|---|---|
| `state` | Calibrated price for the current interval ($/kWh) |
| `region` | NEM region code |
| `forecast_generated_at` | ISO-8601 timestamp of the AEMO source file |
| `forecast` | List of all forecast periods (see below) |
| `next_value` | Calibrated price for the next interval |
| `min_24h_value` | Minimum calibrated price in the next 24 hours |
| `max_24h_value` | Maximum calibrated price in the next 24 hours |
| `cheapest_2h_window` | Best contiguous 2-hour window over 7 days |

Each entry in `forecast` contains:

```yaml
nemtime: "2026-04-15T17:30:00+10:00"   # interval END (AEMO convention)
time:    "2026-04-15T17:00:00+10:00"   # interval START
raw_value: 0.084                        # raw AEMO forecast ($/kWh)
calibrated: 0.142                       # OLS-calibrated value
p10: 0.091                             # 10th percentile (optimistic)
p50: 0.138                             # 50th percentile (median)
p90: 0.231                             # 90th percentile (conservative)
mae: 0.038                             # mean absolute error of OLS fit
calibrated_source: ols                 # "ols" or "passthrough"
n_obs: 147                             # observations used for this bucket
horizon_hours: 36.5                    # hours ahead
value: 0.142                           # alias for calibrated (template compat)
```

> **Timestamp convention**: `nemtime` is the interval END timestamp as published by AEMO. `time` is the interval START (nemtime − 30 minutes). This matches the AEMO dispatch interval convention.

---

### `sensor.nem_pd7day_gas_forecast`

Gas-fired generation forecast from MARKET_SUMMARY.

| Attribute | Description |
|---|---|
| `state` | Gas generation for the current period (TJ/day) |
| `forecast` | List of daily gas forecast periods |

---

### `sensor.pd7day_ic_nsw1_qld1` / `sensor.pd7day_ic_n_q_mnsp1`

Interconnector flow forecasts.

| Attribute | Description |
|---|---|
| `state` | Current period MW flow (positive = export from QLD) |
| `interconnector_id` | Interconnector identifier |
| `forecast` | List of forecast periods with `nemtime`, `time`, `mw` |

---

### `binary_sensor.nem_pd7day_intervention`

`ON` when AEMO has flagged a market intervention in the CASESOLUTION data. Under normal market conditions this is `OFF`.

---

### `sensor.nem_pd7day_calibration`

Calibration system diagnostic sensor.

| Attribute | Description |
|---|---|
| `state` | Total observations logged |
| `active_buckets` | Number of calibration buckets with ≥ 10 observations |
| `total_buckets` | 24 (6 horizons × 4 time-of-day bands) |
| `fitted_at` | ISO-8601 timestamp of last model refit |
| `observation_count` | Same as state |

---

## Calibration System

The calibration system corrects the known bias in AEMO's PD7DAY forecasts using your local history of forecast vs actual wholesale prices.

### How it works

1. **Forecast ingestion** — each fetch logs the forecast price for every future interval into persistent storage keyed by interval start time
2. **Actual logging** — when Amber's feed-in price sensor updates, the actual wholesale RRP is logged against the current interval
3. **Matching** — when both forecast and actual exist for an interval, an observation pair is created
4. **Bucketing** — observations are grouped into 24 buckets by horizon and time-of-day:

| Horizon buckets | Time-of-day buckets |
|---|---|
| `h00_06` — 0 to 6 hours ahead | `solar` — 10:00–16:00 |
| `h06_12` — 6 to 12 hours | `peak` — 16:00–20:00 |
| `h12_24` — 12 to 24 hours | `shoulder` — 20:00–22:00 |
| `h24_48` — 24 to 48 hours | `offpeak` — all other hours |
| `h48_96` — 48 to 96 hours | |
| `h96plus` — beyond 96 hours | |

5. **Model fitting** — once a bucket has ≥ 10 observations, two models are fitted:
   - **OLS** (ordinary least squares): `calibrated = a + b × raw` — corrects linear bias
   - **IRLS quantile regression** (pinball loss): separate fits for P10, P50, P90

6. **Application** — at forecast time, each period's bucket is looked up. If active, OLS and quantile values are returned. Otherwise the raw value passes through unchanged.

### Warm-up period

With 3 fetches per day, expect:

| Day | Buckets active | Coverage |
|---|---|---|
| 1–3 | 0 | All passthrough |
| 4–5 | h00_06, h06_12, h12_24 | Near-term calibrated |
| 6–8 | h24_48 | 2-day horizon calibrated |
| 10–14 | h48_96, h96plus | Full 7-day calibration |

### Persistent storage

Observations and fitted coefficients are stored in HA's `.storage` directory:

- `/config/.storage/nem_pd7day.observation_log`
- `/config/.storage/nem_pd7day.calibration_coefficients`

To reset calibration (e.g. after changing regions):

```bash
rm /config/.storage/nem_pd7day.observation_log
rm /config/.storage/nem_pd7day.calibration_coefficients
```

Then reload or restart the integration.

---

## Fetch Schedule

| NEM time (AEST) | UTC | Notes |
|---|---|---|
| 07:30 | 21:30 (previous day) | Morning AEMO publish |
| 13:00 | 03:00 | Midday AEMO publish |
| 18:00 | 08:00 | Evening AEMO publish |

The integration uses `async_track_point_in_utc_time` which fires reliably at exact UTC datetimes and self-reschedules 24 hours after each fire. It works correctly regardless of the HA host timezone.

---

## Template Sensor Examples

The `value` key in each forecast period is an alias for `calibrated` (or `raw_value` in passthrough), maintained for backward compatibility with template sensors.

### Next 24-hour minimum price

```yaml
template:
  - sensor:
      - name: "PD7DAY Min Price 24h"
        unit_of_measurement: "$/kWh"
        state: >
          {{ state_attr('sensor.qld1_pd7day_forecast', 'min_24h_value') | round(4) }}
```

### Cheapest 2-hour window start time

```yaml
template:
  - sensor:
      - name: "PD7DAY Cheapest Window Start"
        state: >
          {{ state_attr('sensor.qld1_pd7day_forecast', 'cheapest_2h_window')['time'] }}
```

### Forecast after current Amber price

```yaml
template:
  - sensor:
      - name: "QLD1 PD7DAY Forecast After Amber"
        unit_of_measurement: "$/kWh"
        state: >
          {% set forecast = state_attr('sensor.qld1_pd7day_forecast', 'forecast') %}
          {% set amber = states('sensor.amber_express_amber_feed_in_price') | float(0) %}
          {% if forecast %}
            {{ (forecast | map(attribute='value') | list)[1] | round(4) }}
          {% else %}
            unavailable
          {% endif %}
```

---

## NEM Time Convention

All timestamps in this integration use **AEST (UTC+10:00)** with no daylight saving adjustment, matching AEMO's published data. Timestamps are always ISO-8601 with explicit `+10:00` suffix, e.g. `2026-04-14T07:30:00+10:00`.

The `nemtime` field in forecast periods is the **interval end** timestamp (AEMO convention). The `time` field is the **interval start** (`nemtime − 30 minutes`).

---

## Data Source

Price forecast data is sourced from [AEMO NEMWeb](https://www.nemweb.com.au/REPORTS/CURRENT/PD7Day/) — the Australian Energy Market Operator's public data portal. The PD7DAY dataset is updated three times per day and contains 7-day ahead dispatch price forecasts for all NEM regions.

---

## Troubleshooting

### Integration fails to load

Check the HA log for errors from `custom_components.nem_pd7day`. The most common cause is a network issue reaching `nemweb.com.au`.

### Sensors show `unavailable`

The first fetch runs at integration load. Check **Settings → System → Logs** filtered to `nem_pd7day` for fetch errors.

### p10/p50/p90 values are `null`

Normal for the first 3–5 days. Calibration requires at least 10 observations per bucket. Check `sensor.nem_pd7day_calibration` state for current observation count.

### Recorder warnings about attribute size

Add the recorder exclusions shown in the [Configuration](#configuration) section.

---

## Version History

| Version | Changes |
|---|---|
| 1.5.0 | AEMO interval convention: `nemtime` (end) + `time` (start) on all forecast periods |
| 1.4.0 | Timezone overhaul: all timestamps ISO-8601 +10:00, UTC-safe scheduling |
| 1.3.0 | Replaced polling with `async_track_point_in_utc_time` at AEMO publish times |
| 1.2.0 | OLS + IRLS quantile calibration engine, Amber listener, calibration diagnostic sensor |
| 1.1.0 | CASESOLUTION (binary sensor), MARKET_SUMMARY (gas), INTERCONNECTORSOLUTION sensors |
| 1.0.0 | Initial release: PRICESOLUTION forecast sensor |

---

## License

MIT License — see [LICENSE](LICENSE) for details.
