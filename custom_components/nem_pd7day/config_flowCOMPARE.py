"""Config flow for NEM PD7DAY."""

from __future__ import annotations

from typing import Any

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.helpers import config_validation as cv

from .const import CONF_REGIONS, DEFAULT_REGIONS, DOMAIN, REGION_OPTIONS


class NEMPD7DayConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for NEM PD7DAY."""

    VERSION = 1

    async def async_step_user(self, user_input: dict[str, Any] | None = None):
        """Handle the initial step."""
        errors: dict[str, str] = {}

        if user_input is not None:
            regions = user_input.get(CONF_REGIONS, [])
            if not regions:
                errors[CONF_REGIONS] = "select_at_least_one_region"
            else:
                await self.async_set_unique_id("nem_pd7day")
                self._abort_if_unique_id_configured()
                return self.async_create_entry(
                    title="NEM PD7DAY",
                    data={CONF_REGIONS: regions},
                )

        data_schema = vol.Schema(
            {
                vol.Required(CONF_REGIONS, default=DEFAULT_REGIONS): cv.multi_select(
                    {region: region for region in REGION_OPTIONS}
                )
            }
        )

        return self.async_show_form(
            step_id="user",
            data_schema=data_schema,
            errors=errors,
        )

    @staticmethod
    def async_get_options_flow(config_entry):
        """Get the options flow for this handler."""
        return NEMPD7DayOptionsFlow(config_entry)


class NEMPD7DayOptionsFlow(config_entries.OptionsFlow):
    """Handle NEM PD7DAY options flow."""

    def __init__(self, config_entry) -> None:
        self._config_entry = config_entry

    async def async_step_init(self, user_input: dict[str, Any] | None = None):
        """Manage options."""
        errors: dict[str, str] = {}

        if user_input is not None:
            regions = user_input.get(CONF_REGIONS, [])
            if not regions:
                errors[CONF_REGIONS] = "select_at_least_one_region"
            else:
                return self.async_create_entry(data={CONF_REGIONS: regions})

        existing = self._config_entry.options.get(
            CONF_REGIONS,
            self._config_entry.data.get(CONF_REGIONS, DEFAULT_REGIONS),
        )

        data_schema = vol.Schema(
            {
                vol.Required(CONF_REGIONS, default=existing): cv.multi_select(
                    {region: region for region in REGION_OPTIONS}
                )
            }
        )

        return self.async_show_form(
            step_id="init",
            data_schema=data_schema,
            errors=errors,
        )
