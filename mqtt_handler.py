import json
import math
import threading
import time
from dataclasses import dataclass
from typing import Callable

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from heatmiser_thermostat import ThermostatData
from const import MQTT_ROOT, MQTT_RETAINED_EXPIRY_SECONDS, MQTT_REFRESH_FRACTION, LOGGER, MQTT_DISCOVERY_PREFIX


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MQTT_QOS = 1


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

# connection management
_mqtt_client: mqtt.Client | None = None
_mqtt_connected = threading.Event()
_mqtt_lock = threading.RLock()

# Last successful publication time and data for each retained topic.
_zone_ids_published: set[int] = set()
_last_published: dict[str, float] = {}
_last_published_payload: dict[str, str] = {}

# Flag to indicate that everything has to be published, e.g., on first start
# and every MQTT reconnect 
_force_republish = True


# ---------------------------------------------------------------------------
# Hooks for write operations
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class MqttCommandHandlers:
    thermostat_on:                 Callable[[int, bool],  None] | None = None
    thermostat_target_temperature: Callable[[int, float], None] | None = None
    floor_limit_temperature:       Callable[[int, float], None] | None = None
    timeclock_on:                  Callable[[int, bool],  None] | None = None


mqtt_handlers = MqttCommandHandlers()


# ---------------------------------------------------------------------------
# MQTT setup
# ---------------------------------------------------------------------------

def mqtt_start(
    host: str,
    port: int = 1883,
    username: str | None = None,
    password: str | None = None,
) -> mqtt.Client:
    global _mqtt_client

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        protocol=mqtt.MQTTv5,
    )

    if username is not None:
        client.username_pw_set(username, password)

    client.will_set(
        f"{MQTT_ROOT}/availability",
        payload="offline",
        qos=MQTT_QOS,
        retain=True,
    )

    client.on_connect = _on_connect
    client.on_disconnect = _on_disconnect
    client.on_message = _on_message

    # Paho automatically reconnects after an established connection is lost.
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    # connect() can raise OSError if the initial TCP connection fails.
    result = client.connect(host, port, keepalive=60)
    if result != mqtt.MQTT_ERR_SUCCESS:
        raise RuntimeError(f"MQTT connect failed: {result}")

    _mqtt_client = client
    client.loop_start()

    return client


def _on_connect(client, userdata, flags, reason_code, properties) -> None:
    global _force_republish

    if reason_code.is_failure:
        LOGGER.error("MQTT connection failed: %s", reason_code)
        return

    # The broker may have restarted/lost its retained database while we
    # were disconnected, so republish everything on the next poll.
    with _mqtt_lock:
        _force_republish = True

    # Commands must never be published retained by the sender.
    client.subscribe(
        [
            (f"{MQTT_ROOT}/+/thermostat/set/on", MQTT_QOS),
            (f"{MQTT_ROOT}/+/thermostat/set/target_temperature", MQTT_QOS),
            (f"{MQTT_ROOT}/+/floor/set/limit_temperature", MQTT_QOS),
            (f"{MQTT_ROOT}/+/timeclock/set/on", MQTT_QOS),
        ]
    )

    # Establish bridge availability.
    client.publish(
        f"{MQTT_ROOT}/availability",
        payload="online",
        qos=MQTT_QOS,
        retain=True,
    )

    with _mqtt_lock:
        _mqtt_connected.set()


def _on_disconnect(_client, _userdata, _disconnect_flags, _reason_code, _properties):
    _mqtt_connected.clear()


def mqtt_stop() -> None:
    """
    Clean shutdown.

    A clean MQTT disconnect does not trigger the Last Will, so explicitly
    publish offline before disconnecting.
    """
    global _mqtt_client

    client = _mqtt_client
    if client is None:
        return

    if _mqtt_connected.is_set():
        info = client.publish(
            f"{MQTT_ROOT}/availability",
            payload="offline",
            qos=MQTT_QOS,
            retain=True,
        )
        info.wait_for_publish(timeout=2.0)

    client.disconnect()
    client.loop_stop()

    _mqtt_connected.clear()
    _mqtt_client = None


# ---------------------------------------------------------------------------
# State topic generation for a zone
# ---------------------------------------------------------------------------

def _zone_payloads(zone: ThermostatData) -> dict[str, str]:
    """Generate the retained topics which currently exist for the given zone."""

    base = f"{MQTT_ROOT}/{zone.zone_id}"

    result: dict[str, str] = {
        f"{base}/online": "online" if zone.online else "offline",
        f"{base}/is_timeclock": _bool_payload(zone.is_timeclock),
    }

    if zone.zone_name:
        result[f"{base}/name"] = zone.zone_name

    if zone.is_timeclock:
        result[f"{base}/timeclock/on"] = _bool_payload(zone.on)
        result[f"{base}/timeclock/demand"] = _bool_payload(zone.demand)

    else:
        result[f"{base}/thermostat/current_temperature"] = _float_payload(zone.current_temperature)
        result[f"{base}/thermostat/target_temperature"] = _float_payload(zone.target_temperature)
        result[f"{base}/thermostat/on"]             = _bool_payload(zone.on)
        result[f"{base}/thermostat/demand"]         = _bool_payload(zone.demand)
        result[f"{base}/floor/current_temperature"] = _float_payload(zone.floor_current_temperature)
        result[f"{base}/floor/limit_temperature"]   = _float_payload(zone.floor_limit_temperature)
        result[f"{base}/floor/limit_active"]        = _bool_payload(zone.floor_limit_active)

    return result


def _published_topics_for_zone(zone_id: int) -> set[str]:
    prefix = f"{MQTT_ROOT}/{zone_id}/"
    return {
        topic
        for topic in _last_published_payload
        if topic.startswith(prefix)
    }

def _bool_payload(value: bool) -> str:
    return "true" if value else "false"

def _float_payload(value: float | None) -> str:
    if value is None:
        return "null"
    return f"{value:.1f}"


# ---------------------------------------------------------------------------
# Publishing topics
# ---------------------------------------------------------------------------

def _publish_retained_topic(topic: str, payload: str | None, always: bool) -> bool:
    """
    Publish the given topic with the given payload. 
    payload=None deletes the retained topic.
    If the payload is unchanged since the topic was last updated, it is NOT 
    republished unless 'always' is true.
    """
    client = _mqtt_client

    if client is None or not _mqtt_connected.is_set():
        return False

    now = time.monotonic()
    if payload is None:
        # Zero-length retained publication deletes retained state.
        if not(always or topic in _last_published_payload):
            return True
        info = client.publish(
            topic,
            payload=b"",
            qos=MQTT_QOS,
            retain=True,
        )
    else:
        changed = (
            topic not in _last_published_payload
            or _last_published_payload[topic] != payload
        )
        refresh = _refresh_due(topic, now)
        if not (always or changed or refresh):
            return True
        info = client.publish(
            topic,
            payload=payload,
            qos=MQTT_QOS,
            retain=True,
            properties=_publish_properties(),
        )
        LOGGER.debug(f"Published {topic} = {payload}")

    if info.rc != mqtt.MQTT_ERR_SUCCESS:
        LOGGER.warning(
            "Failed to queue MQTT publication %s: %s",
            topic,
            info.rc,
        )
        return False

    if payload is None:
        _last_published.pop(topic, None)
        _last_published_payload.pop(topic, None)
    else:
        _last_published[topic] = now
        _last_published_payload[topic] = payload

    return True

def _publish_properties() -> Properties | None:
    """Create the properties to ensure mesage expiry"""
    if MQTT_RETAINED_EXPIRY_SECONDS is None:
        return None

    properties = Properties(PacketTypes.PUBLISH)
    properties.MessageExpiryInterval = MQTT_RETAINED_EXPIRY_SECONDS
    return properties


def mqtt_publish(zones: dict[int, ThermostatData]):
    """
    Publish the latest Heatmiser state.

    First call:
        publishes the complete state.

    Subsequent calls:
        publishes changed values only, except for the occasional refresh
        required when MQTT_RETAINED_EXPIRY_SECONDS is finite.

    zones must contain the complete CURRENT set of zones.
    """
    global _force_republish

    if not _mqtt_connected.is_set():
        # Do not update anything while disconnected. The next successful
        # poll after reconnect will therefore still be published.
        return

    with _mqtt_lock:
        force = _force_republish
        everything_ok = True

        # ---------------------------------------------------------------
        # Remove zones which no longer exist.
        # ---------------------------------------------------------------

        removed_zone_ids = _zone_ids_published - set(zones.keys())

        for zone_id in removed_zone_ids:
            zone_ok = True

            # Remove only topics which this process previously published.
            for topic in _published_topics_for_zone(zone_id):
                if not _publish_retained_topic(topic, None, True):
                    zone_ok = False

            # if that worked, we remove the zone from the set of the currently
            # published zones
            if zone_ok:
                _zone_ids_published.remove(zone_id)
            else:
                everything_ok = False

        # ---------------------------------------------------------------
        # Add/update current zones.
        # ---------------------------------------------------------------

        for zone_id, zone in zones.items():
            if zone.zone_id != zone_id:
                raise ValueError(
                    f"Zone dictionary key {zone_id} does not match "
                    f"ThermostatData.zone_id {zone.zone_id}"
                )

            topics = _zone_payloads(zone)
            zone_ok = True

            # Remove topics which used to exist for this zone but no longer do,
            # e.g. if a thermostat changes into a timeclock or vice versa.
            removed_topics = _published_topics_for_zone(zone_id) - set(topics.keys())
            for topic in removed_topics:
                if not _publish_retained_topic(topic, None, True):
                    zone_ok = False

            for topic, payload in topics.items():
                if not _publish_retained_topic(topic, payload, always = force):
                    zone_ok = False

            if zone_ok:
                _zone_ids_published.add(zone_id)
            else:
                everything_ok = False

        if everything_ok:
            _force_republish = False


def _refresh_due(topic: str, now: float) -> bool:
    if MQTT_RETAINED_EXPIRY_SECONDS is None:
        return False

    last = _last_published.get(topic)

    if last is None:
        return True

    refresh_after = (
        MQTT_RETAINED_EXPIRY_SECONDS * MQTT_REFRESH_FRACTION
    )

    return now - last >= refresh_after

# ---------------------------------------------------------------------------
# Discovery configuration for home assistant
# ---------------------------------------------------------------------------

def mqtt_publish_homeassistant_discovery(zones: dict[int, ThermostatData]) -> None:
    client = _mqtt_client

    if client is None or not _mqtt_connected.is_set():
        return

    for zone_id, zone in zones.items():

        # Timeclocks don't have thermostat/floor climate entities
        if zone.is_timeclock:
            continue

        base = f"{MQTT_ROOT}/{zone_id}"

        device = {
            "identifiers": [f"heatmiser_zone_{zone_id}"],
            "name": "Neostat " + zone.zone_name,
            "manufacturer": "Heatmiser",
        }

        availability = [
            {"topic": f"{MQTT_ROOT}/availability"},
            {"topic": f"{base}/online"},
        ]

        # ---------------------------------------------------------------
        # Thermostat
        # ---------------------------------------------------------------

        thermostat_config = {
            "name":                         zone.zone_name,
            "unique_id":                    f"heatmiser_{zone_id}_thermostat",
            "default_entity_id":            f"climate.heatmiser_{zone_id}_thermostat",

            "device":                       device,
            "availability":                 availability,
            "availability_mode":            "all",

            "modes":                        ["off", "heat"],
            "mode_state_topic":             f"{base}/thermostat/on",
            "mode_state_template":          "{{ 'heat' if value == 'true' else 'off' }}",
            "mode_command_topic":           f"{base}/thermostat/set/on",
            "mode_command_template":        "{{ 'true' if value == 'heat' else 'false' }}",

            "current_temperature_topic":    f"{base}/thermostat/current_temperature",
            "current_temperature_template": "{{ 'None' if value == 'null' else value }}",

            "temperature_state_topic":      f"{base}/thermostat/target_temperature",
            "temperature_state_template":   "{{ 'None' if value == 'null' else value }}",
            "temperature_command_topic":    f"{base}/thermostat/set/target_temperature",

            "action_topic":                 f"{base}/thermostat/demand",
            "action_template":              "{{ 'heating' if value == 'true' else 'idle' }}",

            "temperature_unit":             "C",
            "precision":                    0.1,
            "temp_step":                    0.5,
        }

        topic = (
            f"{MQTT_DISCOVERY_PREFIX}/climate/"
            f"heatmiser_{zone_id}_thermostat/config"
        )

        client.publish(
            topic,
            json.dumps(thermostat_config),
            qos=MQTT_QOS,
            retain=True,
        )

        # ---------------------------------------------------------------
        # Floor limiter
        # ---------------------------------------------------------------

        floor_config = {
            "name":                          zone.zone_name + " (Floor)",
            "unique_id":                     f"heatmiser_{zone_id}_floor",
            "default_entity_id":             f"climate.heatmiser_{zone_id}_floor",

            "device":                        device,
            "availability":                  availability,
            "availability_mode":             "all",

            "modes":                         ["off", "cool"],
            "mode_state_topic":              f"{base}/floor/limit_active",
            "mode_state_template":           "{{ 'cool' if value == 'true' else 'off' }}",

            "current_temperature_topic":     f"{base}/floor/current_temperature",
            "current_temperature_template":  "{{ 'None' if value == 'null' else value }}",

            "temperature_state_topic":       f"{base}/floor/limit_temperature",
            "temperature_state_template":    "{{ 'None' if value == 'null' else value }}",
            "temperature_command_topic":     f"{base}/floor/set/limit_temperature",

            "action_topic":                  f"{base}/floor/limit_active",
            "action_template":               "{{ 'cooling' if value == 'true' else 'idle' }}",

            "temperature_unit":              "C",
            "precision":                     0.1,
            "temp_step":                     0.5,

        }

        topic = (
            f"{MQTT_DISCOVERY_PREFIX}/climate/"
            f"heatmiser_{zone_id}_floor/config"
        )

        client.publish(
            topic,
            json.dumps(floor_config),
            qos=MQTT_QOS,
            retain=True,
        )


# ---------------------------------------------------------------------------
# Incoming command handling
# ---------------------------------------------------------------------------

_TRUE_VALUES = {"1", "true", "on", "heat"}
_FALSE_VALUES = {"0", "false", "off"}


def _parse_bool(payload: str) -> bool:
    value = payload.strip().lower()

    if value in _TRUE_VALUES:
        return True

    if value in _FALSE_VALUES:
        return False

    raise ValueError(f"Invalid boolean value: {payload!r}")


def _parse_float(payload: str) -> float:
    value = float(payload.strip())

    if not math.isfinite(value):
        raise ValueError("Temperature must be finite")

    return value


def _on_message(client, userdata, message) -> None:
    # Do not ever replay an accidentally-retained command.
    if message.retain:
        LOGGER.warning(
            "Ignoring retained MQTT command: %s",
            message.topic,
        )
        return

    parts = message.topic.split("/")

    # heatmiser/<zone_id>/<type>/set/<field>
    if (
        len(parts) != 5
        or parts[0] != MQTT_ROOT
        or parts[3] != "set"
    ):
        return

    try:
        zone_id = int(parts[1])
        device_type = parts[2]
        field = parts[4]
        payload = message.payload.decode("utf-8")

        if device_type == "thermostat":
            if field == "on":
                if mqtt_handlers.thermostat_on is None:
                    raise RuntimeError(
                        "thermostat_on handler is not configured"
                    )

                mqtt_handlers.thermostat_on(
                    zone_id,
                    _parse_bool(payload),
                )
                return

            if field == "target_temperature":
                if mqtt_handlers.thermostat_target_temperature is None:
                    raise RuntimeError(
                        "thermostat_target_temperature handler "
                        "is not configured"
                    )

                mqtt_handlers.thermostat_target_temperature(
                    zone_id,
                    _parse_float(payload),
                )
                return

        elif device_type == "floor":
            if field == "limit_temperature":
                if mqtt_handlers.floor_limit_temperature is None:
                    raise RuntimeError(
                        "floor_limit_temperature handler "
                        "is not configured"
                    )

                mqtt_handlers.floor_limit_temperature(
                    zone_id,
                    _parse_float(payload),
                )
                return

        elif device_type == "timeclock":
            if field == "on":
                if mqtt_handlers.timeclock_on is None:
                    raise RuntimeError(
                        "timeclock_on handler is not configured"
                    )

                mqtt_handlers.timeclock_on(
                    zone_id,
                    _parse_bool(payload),
                )
                return

        raise ValueError(
            f"Unsupported MQTT command: {message.topic}"
        )

    except (UnicodeDecodeError, ValueError, RuntimeError) as exc:
        LOGGER.warning(
            "Ignoring MQTT command %s: %s",
            message.topic,
            exc,
        )