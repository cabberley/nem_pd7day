"""NEM PD7DAY binary sensor platform — market intervention flag."""
from __future__ import annotations

import logging
from typing import Any

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import (
    ATTR_LAST_CHANGED,
    ATTR_RUN_DATETIME,
    ATTR_SOURCE_FILE,
    COORDINATOR_KEY,
    DOMAIN,
)
from .coordinator import PD7DayCoordinator

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    coordinator: PD7DayCoordinator = hass.data[DOMAIN][entry.entry_id][COORDINATOR_KEY]
    async_add_entities([PD7DayInterventionSensor(coordinator)], update_before_add=True)


class PD7DayInterventionSensor(CoordinatorEntity[PD7DayCoordinator], BinarySensorEntity):
    """
    Market intervention flag from CASESOLUTION.

    State : ON  = intervention pricing is in effect
            OFF = normal market pricing

    When ON, the AEMO has issued a direction to one or more generators and
    the Regional Reference Price (RRP) no longer reflects normal supply/demand
    dispatch. Price forecast sensors will still show values but they should be
    treated as unreliable for optimisation decisions (EV charging schedules,
    battery dispatch targets, EMHASS planning).

    Recommended automation: when this sensor is ON, suppress any automation
    that acts on pd7day price forecasts until it returns OFF.
    """

    _attr_device_class = BinarySensorDeviceClass.PROBLEM
    _attr_has_entity_name = True
    _attr_should_poll = False

    def __init__(self, coordinator: PD7DayCoordinator) -> None:
        super().__init__(coordinator)
        self._attr_unique_id = "nem_pd7day_intervention"
        self._attr_name = "NEM PD7DAY Market Intervention"
        self.entity_id = "binary_sensor.nem_pd7day_intervention"

    @property
    def _data(self):
        if not self.coordinator.data:
            return None
        return self.coordinator.data.case

    @property
    def available(self) -> bool:
        return self.coordinator.last_update_success and self._data is not None

    @property
    def is_on(self) -> bool | None:
        d = self._data
        return d.intervention if d else None

    @property
    def icon(self) -> str:
        return "mdi:alert-circle" if self.is_on else "mdi:check-circle"

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        d = self._data
        if d is None:
            return {}
        return {
            ATTR_RUN_DATETIME: d.run_datetime,
            ATTR_LAST_CHANGED: d.last_changed,
            ATTR_SOURCE_FILE: (
                self.coordinator.data.source_file if self.coordinator.data else None
            ),
        }
