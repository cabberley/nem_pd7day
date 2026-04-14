"""NEM PD7DAY Price Forecast — Home Assistant integration."""
from __future__ import annotations

import logging
from datetime import timedelta

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform, STATE_UNAVAILABLE, STATE_UNKNOWN
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.event import (
    async_track_state_change_event,
    async_track_time_interval,
    async_track_point_in_utc_time,
)
from homeassistant.util import dt as dt_util

from .calibration_store import CalibrationStore
from .const import (
    CONF_REGIONS,
    COORDINATOR_KEY,
    DOMAIN,
    STORE_KEY,
)
from .coordinator import PD7DayCoordinator
from .nem_time import current_nem_interval, fetch_times_as_utc, now_nem

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [Platform.SENSOR, Platform.BINARY_SENSOR]

# Amber entity that carries the actual wholesale RRP per interval.
AMBER_ACTUAL_ENTITY = "sensor.amber_express_amber_feed_in_price"

# How often to refit calibration models
REFIT_INTERVAL = timedelta(hours=24)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up NEM PD7DAY from a config entry."""
    hass.data.setdefault(DOMAIN, {})

    regions: list[str] = entry.data[CONF_REGIONS]

    # ── Calibration store ────────────────────────────────────────────────────
    store = CalibrationStore(hass)
    await store.async_load()

    # ── Coordinator (no automatic polling) ───────────────────────────────────
    coordinator = PD7DayCoordinator(hass, regions, store)
    await coordinator.async_config_entry_first_refresh()

    hass.data[DOMAIN][entry.entry_id] = {
        COORDINATOR_KEY: coordinator,
        STORE_KEY: store,
    }

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    # ── Shared calibration refit coroutine ──────────────────────────────────
    #
    # Defined here so it can be called both from the scheduled fetch path
    # (_fetch_then_refit) and the periodic 24-hour timer (_refit callback).

    async def _do_refit() -> None:
        """Refit calibration models and refresh sensors."""
        if store.observation_count < 10:
            _LOGGER.debug(
                "Skipping calibration refit — only %d observations (need \u2265 10)",
                store.observation_count,
            )
            return
        _LOGGER.info(
            "PD7DAY calibration refit starting (%d observations)",
            store.observation_count,
        )
        await store.async_refit()
        _LOGGER.info(
            "PD7DAY calibration refit complete \u2014 %d active buckets",
            store.active_bucket_count,
        )
        await coordinator.async_refresh()

    # ── Scheduled fetches at AEMO publish times ──────────────────────────────
    #
    # We use async_track_point_in_utc_time which fires ONCE at a specific UTC
    # datetime — immune to missed-second issues with async_track_time_change.
    # After each fire we immediately reschedule the next occurrence 24h later.
    #
    # NEM fetch times: 07:30, 13:00, 18:00 AEST → 21:30, 03:00, 08:00 UTC.

    def _next_utc_fire(hour: int, minute: int) -> "datetime":
        """Return the next UTC datetime for the given UTC hour:minute."""
        from datetime import datetime, timezone as _tz
        now_utc = datetime.now(_tz.utc)
        candidate = now_utc.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if candidate <= now_utc:
            candidate += timedelta(days=1)
        return candidate

    def _schedule_fetch(hour: int, minute: int) -> None:
        """Schedule (or re-schedule) a single fetch point 24 h apart."""
        fire_at = _next_utc_fire(hour, minute)

        @callback
        def _on_fire(_now=None):
            _LOGGER.info(
                "PD7DAY scheduled fetch triggered — NEM time: %s",
                now_nem().strftime("%Y-%m-%dT%H:%M:%S+10:00"),
            )
            async def _fetch_then_refit():
                await coordinator.async_refresh()
                # Refit immediately after each fetch — new obs may activate buckets
                await _do_refit()  # hoisted above; accessible in this closure
            hass.async_create_task(_fetch_then_refit())
            # Reschedule for tomorrow
            _schedule_fetch(hour, minute)

        cancel = async_track_point_in_utc_time(hass, _on_fire, fire_at)
        entry.async_on_unload(cancel)
        _LOGGER.debug(
            "PD7DAY next fetch at %s UTC (NEM %02d:%02d)",
            fire_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            (hour + 10) % 24,
            minute,
        )

    utc_times = fetch_times_as_utc()  # ["21:30:00", "03:00:00", "08:00:00"]
    for utc_time_str in utc_times:
        t = dt_util.parse_time(utc_time_str)
        _schedule_fetch(t.hour, t.minute)

    _LOGGER.info(
        "PD7DAY scheduled fetches registered at %s NEM time (%s UTC)",
        "07:30, 13:00, 18:00",
        ", ".join(utc_times),
    )

    # ── Amber actual-price listener ──────────────────────────────────────────
    @callback
    def _on_amber_state_change(event):
        """
        Fires each time Amber's feed-in price sensor updates.

        Computes the current 30-minute dispatch interval boundary in NEM time
        (UTC+10, fixed) regardless of HA system timezone, then logs the actual
        wholesale RRP so the calibration pipeline can match it to PD7DAY
        forecasts for that interval.
        """
        new_state = event.data.get("new_state")
        if new_state is None or new_state.state in (
            STATE_UNAVAILABLE, STATE_UNKNOWN, "unknown", "unavailable", ""
        ):
            return

        try:
            actual_rrp = float(new_state.state)
        except (ValueError, TypeError):
            return

        # Compute current interval start in NEM time — always UTC+10, no DST
        interval_iso = current_nem_interval()

        hass.async_create_task(
            store.async_record_actual(interval_iso, actual_rrp)
        )

    entry.async_on_unload(
        async_track_state_change_event(
            hass,
            [AMBER_ACTUAL_ENTITY],
            _on_amber_state_change,
        )
    )

    # ── Periodic calibration refit (daily) ───────────────────────────────────
    @callback
    def _refit(_now=None):
        """Refit calibration models and refresh sensors (24-hour timer)."""
        hass.async_create_task(_do_refit())

    if store.observation_count >= 10 and store.calibration is None:
        _refit()

    entry.async_on_unload(
        async_track_time_interval(hass, _refit, REFIT_INTERVAL)
    )

    entry.async_on_unload(entry.add_update_listener(_async_update_listener))

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    if unload_ok := await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        hass.data[DOMAIN].pop(entry.entry_id)
    return unload_ok


async def _async_update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """React to options changes by reloading the entry."""
    await hass.config_entries.async_reload(entry.entry_id)
