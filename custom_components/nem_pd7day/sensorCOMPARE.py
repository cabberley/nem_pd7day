"""Sensor platform for NEM PD7DAY."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from homeassistant.components.sensor import SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import CONF_REGIONS, DEFAULT_REGIONS, DOMAIN
from .coordinator import NEMPD7DayCoordinator


@dataclass(frozen=True)
class RegionMetric:
    """Description of a metric sensor bound to a region."""

    key: str
    suffix: str
    name: str
    entity_category: EntityCategory | None = None


REGION_METRICS: tuple[RegionMetric, ...] = (
    RegionMetric("current_forecast_price", "current", "Current Forecast Price"),
    RegionMetric("next_forecast_price", "next", "Next Forecast Price"),
    RegionMetric("min_24h", "min_24h", "Min 24h Forecast Price", EntityCategory.DIAGNOSTIC),
    RegionMetric("max_24h", "max_24h", "Max 24h Forecast Price", EntityCategory.DIAGNOSTIC),
    RegionMetric("cheapest_2h_avg", "cheapest_2h_avg", "Cheapest 2h Avg Forecast", EntityCategory.DIAGNOSTIC),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up NEM PD7DAY sensors from a config entry."""
    coordinator: NEMPD7DayCoordinator = hass.data[DOMAIN][entry.entry_id]
    selected_regions = entry.options.get(
        CONF_REGIONS,
        entry.data.get(CONF_REGIONS, DEFAULT_REGIONS),
    )

    entities: list[NEMPD7DayRegionSensor] = []
    for region in selected_regions:
        for metric in REGION_METRICS:
            entities.append(NEMPD7DayRegionSensor(coordinator, entry, region, metric))

    async_add_entities(entities)


class NEMPD7DayRegionSensor(CoordinatorEntity[NEMPD7DayCoordinator], SensorEntity):
    """NEM PD7DAY region metric sensor."""

    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: NEMPD7DayCoordinator,
        entry: ConfigEntry,
        region: str,
        metric: RegionMetric,
    ) -> None:
        super().__init__(coordinator)
        self._entry = entry
        self._region = region
        self._metric = metric
        self._attr_unique_id = f"{entry.entry_id}_{region.lower()}_{metric.suffix}"
        self._attr_name = f"{region} {metric.name}"
        self._attr_native_unit_of_measurement = "$/kWh"
        self._attr_entity_category = metric.entity_category
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, f"{self._entry.entry_id}_{self._region}")},
            name=f"NEM PD7DAY {self._region}",
            manufacturer="AEMO NEMWeb",
            model="PD7DAY",
        )
        self._apply_payload_state()

    def _apply_payload_state(self) -> None:
        """Map latest coordinator payload into entity attributes."""
        payload = self._region_payload
        if payload is None:
            self._attr_available = False
            self._attr_native_value = None
            self._attr_extra_state_attributes = {}
            return

        self._attr_available = True

        if self._metric.key == "cheapest_2h_avg":
            window = payload.get("cheapest_2h_window")
            self._attr_native_value = window.get("avg_price_kwh") if window else None
        else:
            self._attr_native_value = payload.get(self._metric.key)

        attrs: dict[str, Any] = {
            "region": self._region,
            "source_file": self.coordinator.data.get("source_file"),
            "source_file_name": self.coordinator.data.get("source_file_name"),
            "forecast_generated_at": payload.get("forecast_generated_at"),
            "interval_minutes": payload.get("interval_minutes"),
        }

        if self._metric.key == "current_forecast_price":
            attrs["next_forecast_price"] = payload.get("next_forecast_price")
            attrs["min_24h"] = payload.get("min_24h")
            attrs["max_24h"] = payload.get("max_24h")
            attrs["cheapest_2h_window"] = payload.get("cheapest_2h_window")
            attrs["prices"] = payload.get("prices")

        if self._metric.key == "cheapest_2h_avg":
            attrs["cheapest_2h_window"] = payload.get("cheapest_2h_window")

        self._attr_extra_state_attributes = attrs

    def _handle_coordinator_update(self) -> None:
        """Handle updated data from the coordinator."""
        self._apply_payload_state()
        super()._handle_coordinator_update()

    @property
    def _region_payload(self) -> dict | None:
        data = self.coordinator.data or {}
        region_payloads = data.get("regions", {})
        return region_payloads.get(self._region)
