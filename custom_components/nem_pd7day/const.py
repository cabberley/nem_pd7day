"""Constants for the NEM PD7DAY integration."""

DOMAIN = "nem_pd7day"

# NEMWeb base URL for PD7DAY reports
NEMWEB_BASE_URL = "https://www.nemweb.com.au/REPORTS/CURRENT/PD7Day/"

# Supported NEM regions
REGIONS = ["QLD1", "NSW1", "VIC1", "SA1", "TAS1"]

# Config entry keys
CONF_REGIONS = "regions"

# Defaults
DEFAULT_REGIONS = ["QLD1"]

# AEMO PD7DAY publish times (NEM local, hour, minute)
# Fetches are scheduled 25-55 min after each publish to allow NEMWeb to settle.
FETCH_TIMES_NEM = [(7, 30), (13, 0), (18, 0)]

# Coordinator / store keys
COORDINATOR_KEY = "coordinator"
STORE_KEY = "store"

# ── PRICESOLUTION sensor attributes ──────────────────────────────────────────
ATTR_REGION = "region"
ATTR_FORECAST_GENERATED_AT = "forecast_generated_at"
ATTR_INTERVAL_MINUTES = "interval_minutes"
ATTR_NEXT_VALUE = "next_value"
ATTR_MIN_24H = "min_24h_value"
ATTR_MAX_24H = "max_24h_value"
ATTR_CHEAPEST_2H = "cheapest_2h_window"
ATTR_FORECAST = "forecast"
ATTR_SOURCE_FILE = "source_file"

# ── CASESOLUTION binary sensor attributes ─────────────────────────────────────
ATTR_RUN_DATETIME = "run_datetime"
ATTR_LAST_CHANGED = "last_changed"

# ── MARKET_SUMMARY sensor attributes ─────────────────────────────────────────
ATTR_CURRENT_TJ = "current_tj"
ATTR_MAX_7D_TJ = "max_7d_tj"
ATTR_GAS_FORECAST = "forecast"

# ── INTERCONNECTORSOLUTION sensor attributes ──────────────────────────────────
ATTR_INTERCONNECTOR_ID = "interconnector_id"
ATTR_MWFLOW = "mwflow"
ATTR_METEREDMWFLOW = "meteredmwflow"
ATTR_MWLOSSES = "mwlosses"
ATTR_MARGINALVALUE = "marginalvalue"
ATTR_VIOLATIONDEGREE = "violationdegree"
ATTR_EXPORTLIMIT = "exportlimit"
ATTR_IMPORTLIMIT = "importlimit"
ATTR_IS_CONSTRAINED = "is_constrained"
ATTR_MAX_VIOLATION_7D = "max_violation_7d"
ATTR_IC_FORECAST = "forecast"

# ── Calibration sensor attributes ─────────────────────────────────────────────
ATTR_CAL_STATUS = "status"
ATTR_CAL_FITTED_AT = "fitted_at"
ATTR_CAL_OBS_COUNT = "observation_count"
ATTR_CAL_ACTIVE_BUCKETS = "active_buckets"
ATTR_CAL_TOTAL_BUCKETS = "total_buckets"
ATTR_CAL_SUMMARY = "summary"

# ── Calibrated forecast attributes ────────────────────────────────────────────
ATTR_CAL_CALIBRATED = "calibrated"
ATTR_CAL_P10 = "p10"
ATTR_CAL_P50 = "p50"
ATTR_CAL_P90 = "p90"
ATTR_CAL_MAE = "mae"
ATTR_CAL_SOURCE = "calibrated_source"
ATTR_CAL_N_OBS = "n_obs"
