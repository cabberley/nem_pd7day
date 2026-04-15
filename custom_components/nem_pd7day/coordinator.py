"""DataUpdateCoordinator for NEM PD7DAY."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import aiohttp
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

try:
    from .const import DOMAIN, QLD1_INTERCONNECTORS, interconnectors_for_regions
except ImportError:  # pragma: no cover - support direct spec loading in tests
    import importlib.util
    import os
    import sys

    _const_name = "custom_components.nem_pd7day.const"
    if _const_name in sys.modules:
        _const = sys.modules[_const_name]
    else:
        _const_path = os.path.join(os.path.dirname(__file__), "const.py")
        _spec = importlib.util.spec_from_file_location(_const_name, _const_path)
        if _spec is None or _spec.loader is None:
            raise
        _const = importlib.util.module_from_spec(_spec)
        sys.modules[_const_name] = _const
        _spec.loader.exec_module(_const)

    DOMAIN = _const.DOMAIN
    QLD1_INTERCONNECTORS = _const.QLD1_INTERCONNECTORS
    interconnectors_for_regions = _const.interconnectors_for_regions
from .pd7day_client import PD7DayClient, PD7DayResult

if TYPE_CHECKING:
    from .calibration_store import CalibrationStore

_LOGGER = logging.getLogger(__name__)


class PD7DayCoordinator(DataUpdateCoordinator[PD7DayResult]):
    """
    Coordinator for NEM PD7DAY data.

    update_interval is set to None — polling is entirely disabled.
    Refreshes are triggered explicitly by async_track_time callbacks in
    __init__.py at the three AEMO publish times (07:30, 13:00, 18:00 NEM
    local time) plus once at startup via async_config_entry_first_refresh().

    This means the integration makes exactly 3 network requests per day
    instead of 48 (one every 30 minutes).
    """

    def __init__(
        self,
        hass: HomeAssistant,
        regions: list[str],
        store: "CalibrationStore | None" = None,
        interconnector_ids: set[str] | None = None,
    ) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=None,   # no automatic polling — time-triggered only
        )
        self._regions = regions
        derived_ids = interconnectors_for_regions(regions)
        self._interconnector_ids = interconnector_ids or derived_ids or QLD1_INTERCONNECTORS
        self._store = store
        self._session: aiohttp.ClientSession | None = None

    def _get_client(self) -> PD7DayClient:
        if self._session is None or self._session.closed:
            self._session = async_get_clientsession(self.hass)
        return PD7DayClient(
            self._session,
            interconnector_ids=self._interconnector_ids,
        )

    async def _async_update_data(self) -> PD7DayResult:
        client = self._get_client()
        try:
            result = await client.fetch_all(self._regions)
        except Exception as exc:  # noqa: BLE001
            raise UpdateFailed(f"PD7DAY fetch failed: {exc}") from exc

        _LOGGER.debug(
            "PD7DAY updated: source=%s intervention=%s regions=%s interconnectors=%s",
            result.source_file,
            result.case.intervention if result.case else "unknown",
            list(result.prices.keys()),
            list(result.interconnectors.keys()),
        )

        # Feed forecast history into calibration store
        if self._store is not None:
            for region, price_data in result.prices.items():
                self._store.ingest_forecast(
                    region=region,
                    price_data=price_data,
                    interconnectors=result.interconnectors,
                    case=result.case,
                )

        return result
