from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any
from dataclasses import field

@dataclass
class ThermostatData:
    zone_id: int
    zone_name: str = ""
    is_timeclock: bool = False

    online: bool = False
    current_temperature: float | None = None
    target_temperature: float | None = None

    enabled: bool = False
    demand: bool = False

    floor_current_temperature: float | None = None
    floor_limit_temperature: float | None = None
    floor_limit_active: bool = False

    last_neohub_data: dict[str, Any] =  field(default_factory=dict)
    last_neohub_engineers: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return the thermostat data as a plain dictionary."""
        return {
            "zone_id": self.zone_id,
            "zone_name": self.zone_name,
            "is_timeclock": self.is_timeclock,
            "online": self.online,
            "current_temperature": self.current_temperature,
            "target_temperature": self.target_temperature,
            "enabled": self.enabled,
            "demand": self.demand,
            "floor_current_temperature": self.floor_current_temperature,
            "floor_limit_temperature": self.floor_limit_temperature,
            "floor_limit_active": self.floor_limit_active,
        }

    def to_dict_all(self) -> dict[str, Any]:
        """Return the thermostat data as a plain dictionary."""
        return asdict(self)

    @classmethod
    def dump_heading(_cls) -> str:
        return "| NAME            | SETP | TEMP | FLOOR | LIMIT |  ON  | HEAT |"

    @classmethod
    def dump_separator(_cls) -> str:
        return "+-----------------+------+------+-------+-------+------+------+"

    def dump_line(self) -> str:
        if self.online:
            return (
                f"| {self.zone_name:<15} " + 
                f"| {_fmt(self.target_temperature, 4, 1)} " + 
                f"| {_fmt(self.current_temperature, 4, 1)} " + 
                f"| {_fmt(self.floor_current_temperature, 5, 1)} " + 
                f"| {_fmt(self.floor_limit_temperature, 5, 1)} " + 
                f"| { ' ON ' if self.enabled else '    ' } " +
                f"| { 'HEAT' if self.demand else '    ' } " +
                "|"
            )
        else:
            return (
                f"| {self.zone_name:<15} " + 
                "+|              -- OFFLINE --               |"
            )

    @classmethod
    def create(
        cls,
        id: int, name: str,
    ) -> "ThermostatData":
        """Create a new ThermostatData object from the data provided in a response to an 'INFO' request """
        
        thermostat = cls(zone_id=id, zone_name=name)
        return thermostat

    def update_from_heatmiser_info(
        self,
        neohub_data: dict[str, Any],
    ) -> None:
        """Update a ThermostatData object from the data provided in a response to an 'INFO' request """
        self.online = not bool(neohub_data.get("OFFLINE", False))

        if not self.is_timeclock:
            self.current_temperature       = self._temperature(neohub_data.get("CURRENT_TEMPERATURE"))
            self.target_temperature        = self._temperature(neohub_data.get("CURRENT_SET_TEMPERATURE"))
            self.enabled                   = not bool(neohub_data.get("STANDBY", False))
            self.demand                    = bool(neohub_data.get("HEATING", False))
            self.floor_current_temperature = self._temperature(neohub_data.get("CURRENT_FLOOR_TEMPERATURE"))
            self.floor_limit_active        = bool(neohub_data.get("FLOOR_LIMIT", False))
        else:
            self.current_temperature       = None
            self.target_temperature        = None
            self.enabled                   = not bool(neohub_data.get("STANDBY", False))
            self.demand                    = bool(neohub_data.get("HEATING", False))
            self.floor_limit_temperature   = None
            self.floor_current_temperature = None
            self.floor_limit_active        = False


    def update_from_heatmiser_live_data(
        self,
        neohub_data: dict[str, Any],
    ) -> None:
        """Update a ThermostatData object from the data provided in a response to an 'GET_DATA' request """

        # Fundamentals: update name and whether this is a time clock or not
        self.last_neohub_data = neohub_data

        if "ZONE_NAME" in neohub_data:
            self.zone_name = str(neohub_data["ZONE_NAME"])

        self.is_timeclock = bool(neohub_data.get("TIMECLOCK", False))
        self.online = not bool(neohub_data.get("OFFLINE", False))

        if not self.is_timeclock:
            self.current_temperature       = self._temperature(neohub_data.get("ACTUAL_TEMP"))
            self.target_temperature        = self._temperature(neohub_data.get("SET_TEMP"))
            self.enabled                   = not bool(neohub_data.get("STANDBY", False))
            self.demand                    = bool(neohub_data.get("HEAT_ON", False))
            self.floor_current_temperature = self._temperature(neohub_data.get("CURRENT_FLOOR_TEMPERATURE"))
            self.floor_limit_active        = bool(neohub_data.get("FLOOR_LIMIT", False))
        else:
            self.current_temperature       = None
            self.target_temperature        = None
            self.enabled                   = not bool(neohub_data.get("STANDBY", False))
            self.demand                    = bool(neohub_data.get("TIMER_ON", False))
            self.floor_limit_temperature   = None
            self.floor_current_temperature = None
            self.floor_limit_active        = False

    def add_heatmiser_engineers_data(
        self,
        neohub_data: dict[str, Any],
    ) -> None:
        """Update a ThermostatData object from the data provided in a response to an 'GET_ENGINEERS' request """
        if self.is_timeclock:
            return
        self.floor_limit_temperature = self._temperature(neohub_data.get("FLOOR_LIMIT"))

    @staticmethod
    def _temperature(value: Any) -> float | None:
        try:
            value = float(value)
        except (TypeError, ValueError):
            return None

        if value > 127.0:
            return None

        return round(value, 1)

def _fmt(value, width=4, decimals=1):
    return f"{value:{width}.{decimals}f}" if value is not None else "-" * width
    
