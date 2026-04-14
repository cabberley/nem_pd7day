"""
NEM PD7DAY Calibration Store
==============================
Manages two persistent JSON files in HA's .storage directory:

  nem_pd7day.observation_log
    Rolling window of paired (forecast, actual) observations.
    Written every time an actual RRP is received from Amber.
    Pruned to MAX_TOTAL_OBS entries (oldest dropped first).

  nem_pd7day.calibration_coefficients
    Serialised CalibrationResult produced by CalibrationEngine.fit().
    Written every time a refit completes (default: every 24 hours).

Timezone policy
---------------
All stored datetime strings are ISO-8601 with explicit +10:00 offset
(NEM time).  Horizon calculations always operate on tz-aware datetimes
so they are correct regardless of the HA system timezone.
"""
from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING

from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import Store

from .calibration_engine import (
    CalibrationEngine,
    CalibrationResult,
    Observation,
)
from .nem_time import now_nem, parse_iso, to_nem_iso

if TYPE_CHECKING:
    from .pd7day_client import PD7DayData, InterconnectorData, CaseSolutionData

_LOGGER = logging.getLogger(__name__)

OBS_STORAGE_KEY = "nem_pd7day.observation_log"
COEFF_STORAGE_KEY = "nem_pd7day.calibration_coefficients"
STORAGE_VERSION = 1

MAX_TOTAL_OBS = 20_000
MAX_FORECAST_AGE_DAYS = 14
MAX_HORIZON_HOURS = 168   # 7 days


class CalibrationStore:
    """
    Coordinates observation logging, coefficient persistence, and
    forecast history caching for the calibration pipeline.
    """

    def __init__(self, hass: HomeAssistant) -> None:
        self._hass = hass
        self._obs_store = Store(hass, STORAGE_VERSION, OBS_STORAGE_KEY)
        self._coeff_store = Store(hass, STORAGE_VERSION, COEFF_STORAGE_KEY)
        self._engine = CalibrationEngine()

        self._observations: list[dict] = []
        self._calibration: CalibrationResult | None = None

        # Forecast history: interval_time_iso → list of forecast entries
        # Keys and run_at values are ISO-8601 +10:00 strings.
        self._forecast_history: dict[str, list[dict]] = {}

    # ── Startup ───────────────────────────────────────────────────────────────

    async def async_load(self) -> None:
        obs_data = await self._obs_store.async_load() or {}
        self._observations = obs_data.get("observations", [])
        _LOGGER.info(
            "PD7DAY calibration: loaded %d observations from storage",
            len(self._observations),
        )

        coeff_data = await self._coeff_store.async_load()
        if coeff_data:
            try:
                self._calibration = self._engine.from_storage(coeff_data)
                _LOGGER.info(
                    "PD7DAY calibration: restored coefficients fitted at %s (%d obs)",
                    self._calibration.fitted_at,
                    self._calibration.total_observations,
                )
            except Exception as exc:
                _LOGGER.warning(
                    "PD7DAY calibration: could not restore coefficients: %s", exc
                )

    # ── Forecast history management ───────────────────────────────────────────

    def ingest_forecast(
        self,
        region: str,
        price_data: "PD7DayData",
        interconnectors: dict[str, "InterconnectorData"],
        case: "CaseSolutionData | None",
    ) -> None:
        """
        Called by the coordinator on each successful fetch.
        All interval_time keys and run_at values are ISO-8601 +10:00 strings.
        """
        run_at_str = price_data.forecast_generated_at or to_nem_iso(now_nem())
        is_intervention = case.intervention if case else False

        qni = interconnectors.get("NSW1-QLD1")
        qni_mwflow = qni.current_mwflow if qni else None
        qni_violation = qni.current_violationdegree if qni else None

        for period in price_data.forecast:
            entry = {
                "run_at": run_at_str,
                "forecast_price": period.value,
                "gas_tj": None,
                "qni_mwflow": qni_mwflow,
                "qni_violation": qni_violation,
                "is_intervention": is_intervention,
                "region": region,
            }
            if period.time not in self._forecast_history:
                self._forecast_history[period.time] = []
            self._forecast_history[period.time].append(entry)

        # Prune old history — compare ISO strings directly (fixed offset sorts correctly)
        cutoff = to_nem_iso(
            now_nem() - timedelta(days=MAX_FORECAST_AGE_DAYS)
        )
        self._forecast_history = {
            k: v for k, v in self._forecast_history.items() if k >= cutoff
        }

    # ── Observation logging ───────────────────────────────────────────────────

    async def async_record_actual(
        self,
        interval_time: str,   # ISO-8601 +10:00 NEM time
        actual_rrp: float,
    ) -> int:
        """
        Match the actual RRP for an interval against all PD7DAY forecasts
        that covered it.  Horizon is computed from tz-aware datetimes so it
        is accurate regardless of system timezone.
        """
        forecasts = self._forecast_history.get(interval_time, [])
        if not forecasts:
            _LOGGER.debug(
                "No forecast history for interval %s — skipping", interval_time
            )
            return 0

        interval_dt = parse_iso(interval_time)
        new_count = 0

        for fc in forecasts:
            try:
                run_dt = parse_iso(fc["run_at"])
            except (ValueError, KeyError):
                continue

            # Both datetimes are tz-aware (UTC+10) — subtraction is unambiguous
            horizon_h = (interval_dt - run_dt).total_seconds() / 3600
            if horizon_h < 0 or horizon_h > MAX_HORIZON_HOURS:
                continue

            obs = {
                "interval_time": interval_time,
                "horizon_hours": round(horizon_h, 2),
                "pd7day_forecast": fc["forecast_price"],
                "actual_rrp": actual_rrp,
                "forecast_run_at": fc["run_at"],
                "hour_of_day": interval_dt.hour,   # NEM local hour (UTC+10)
                "day_of_week": interval_dt.weekday(),
                "month": interval_dt.month,
                "gas_forecast_tj": fc.get("gas_tj"),
                "qni_mwflow": fc.get("qni_mwflow"),
                "qni_violation_degree": fc.get("qni_violation"),
                "is_intervention": fc.get("is_intervention", False),
            }
            self._observations.append(obs)
            new_count += 1

        if new_count:
            if len(self._observations) > MAX_TOTAL_OBS:
                self._observations = self._observations[-MAX_TOTAL_OBS:]
            await self._save_observations()
            _LOGGER.debug(
                "Logged %d observations for interval %s (total=%d)",
                new_count, interval_time, len(self._observations),
            )

        return new_count

    async def _save_observations(self) -> None:
        await self._obs_store.async_save({"observations": self._observations})

    # ── Calibration fitting ───────────────────────────────────────────────────

    async def async_refit(self) -> CalibrationResult:
        obs_list = [
            Observation(
                interval_time=o["interval_time"],
                horizon_hours=o["horizon_hours"],
                pd7day_forecast=o["pd7day_forecast"],
                actual_rrp=o["actual_rrp"],
                forecast_run_at=o["forecast_run_at"],
                hour_of_day=o["hour_of_day"],
                day_of_week=o["day_of_week"],
                month=o["month"],
                gas_forecast_tj=o.get("gas_forecast_tj"),
                qni_mwflow=o.get("qni_mwflow"),
                qni_violation_degree=o.get("qni_violation_degree"),
                is_intervention=o.get("is_intervention", False),
            )
            for o in self._observations
        ]

        result = await self._hass.async_add_executor_job(
            self._engine.fit, obs_list
        )
        self._calibration = result
        await self._coeff_store.async_save(self._engine.to_storage(result))
        return result

    # ── Public accessors ──────────────────────────────────────────────────────

    @property
    def calibration(self) -> CalibrationResult | None:
        return self._calibration

    @property
    def observation_count(self) -> int:
        return len(self._observations)

    @property
    def active_bucket_count(self) -> int:
        if not self._calibration:
            return 0
        return sum(
            1 for m in self._calibration.models.values()
            if not m.ols.is_default
        )

    def apply_to_price(
        self,
        raw_price: float,
        horizon_hours: float,
        hour_of_day: int,
    ) -> dict:
        if self._calibration is None:
            return {
                "calibrated": round(raw_price, 6),
                "p10": None,
                "p50": None,
                "p90": None,
                "mae": None,
                "calibrated_source": "passthrough",
                "n_obs": 0,
            }
        return self._calibration.apply(raw_price, horizon_hours, hour_of_day)

    def summary_attributes(self) -> dict:
        if not self._calibration:
            return {
                "status": "no_calibration",
                "observation_count": self.observation_count,
                "active_buckets": 0,
            }
        return {
            "status": "active",
            "fitted_at": self._calibration.fitted_at,
            "observation_count": self.observation_count,
            "active_buckets": self.active_bucket_count,
            "total_buckets": 24,
            "summary": self._calibration.summary(),
        }
