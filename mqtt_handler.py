#--------------------------------------------------------------------------------------------------
# region Imports
#--------------------------------------------------------------------------------------------------

import asyncio
import json
import math
import threading
import time
from dataclasses import dataclass
from typing import Awaitable, Callable

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from paho.mqtt.reasoncodes import ReasonCode

from heatmiser_thermostat import ThermostatData
from const import (
    MQTT_CLIENT_NAME,
    MQTT_ROOT,
    MQTT_DISCOVERY_PREFIX,
    MQTT_RETAINED_EXPIRY_SECONDS,
    MQTT_REFRESH_FRACTION,
    MQTT_AVAILABILITY_TIMER_SECONDS,
    MQTT_KEEPALIVE_INTERVAL_SECONDS,
    LOGGER,
)

# endregion
#--------------------------------------------------------------------------------------------------

#--------------------------------------------------------------------------------------------------
# region Configuration
#--------------------------------------------------------------------------------------------------

MQTT_QOS = 1

# endregion
#--------------------------------------------------------------------------------------------------

#--------------------------------------------------------------------------------------------------
# region State
#--------------------------------------------------------------------------------------------------

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

# endregion
#--------------------------------------------------------------------------------------------------

#--------------------------------------------------------------------------------------------------
# region Hooks for write operations
#--------------------------------------------------------------------------------------------------

@dataclass(slots=True)
class MqttCommandHandlers:
    thermostat_set_enabled:     Callable[[int, bool],  Awaitable[None]] | None = None
    thermostat_set_temperature: Callable[[int, float], Awaitable[None]] | None = None
    thermostat_set_floor_limit: Callable[[int, float], Awaitable[None]] | None = None
    timeclock_set_enabled:      Callable[[int, bool],  Awaitable[None]] | None = None


mqtt_handlers = MqttCommandHandlers()

def mqtt_set_command_handlers(
    thermostat_set_enabled: Callable[[int, bool], None] | None = None,
    thermostat_set_temperature: Callable[[int, float], None] | None = None,
    thermostat_set_floor_limit: Callable[[int, float], None] | None = None,
    timeclock_set_enabled: Callable[[int, bool], None] | None = None,
) -> None:
    mqtt_handlers.thermostat_set_enabled     = thermostat_set_enabled
    mqtt_handlers.thermostat_set_temperature = thermostat_set_temperature
    mqtt_handlers.thermostat_set_floor_limit = thermostat_set_floor_limit
    mqtt_handlers.timeclock_set_enabled      = timeclock_set_enabled

# endregion
#--------------------------------------------------------------------------------------------------

#--------------------------------------------------------------------------------------------------
# region MQTT connection
#--------------------------------------------------------------------------------------------------

def mqtt_start(
    host: str,
    port: int = 1883,
    username: str | None = None,
    password: str | None = None,
) -> mqtt.Client:
    global _mqtt_client

    LOGGER.info(f"MQTT: Connecting to broker {host}:{port}")

    # Set up the client and credentials (if proviced)
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=MQTT_CLIENT_NAME,
        protocol=mqtt.MQTTv5,
    )

    if username is not None:
        client.username_pw_set(username, password)

    # Create a 'last will and testament' for the .../available
    # topic so it will report offline 'MQTT_AVAILABILITY_WILL_SECONDS'
    # after this bridge disconnects
    will_properties = Properties(PacketTypes.WILLMESSAGE)
    will_properties.WillDelayInterval = MQTT_AVAILABILITY_TIMER_SECONDS

    client.will_set(
        f"{MQTT_ROOT}/available",
        payload="false",
        qos=MQTT_QOS,
        retain=True,
        properties=will_properties,
    )

    client.on_connect = _on_connect
    client.on_disconnect = _on_disconnect
    client.on_message = _on_message
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    # connect() can raise an error if the initial TCP connection fails.
    connect_properties = Properties(PacketTypes.CONNECT)
    connect_properties.SessionExpiryInterval = 120

    result = client.connect(
        host,
        port,
        keepalive=60,
        clean_start=False,
        properties=connect_properties,
    )

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
            (f"{MQTT_ROOT}/+/thermostat/set/enabled", MQTT_QOS),
            (f"{MQTT_ROOT}/+/thermostat/set/target_temperature", MQTT_QOS),
            (f"{MQTT_ROOT}/+/floor/set/limit_temperature", MQTT_QOS),
            (f"{MQTT_ROOT}/+/timeclock/set/enabled", MQTT_QOS),
        ]
    )

    # Establish bridge availability.
    client.publish(
        f"{MQTT_ROOT}/available",
        payload="true",
        qos=MQTT_QOS,
        retain=True,
    )

    with _mqtt_lock:
        _mqtt_connected.set()

    LOGGER.info("MQTT: Connected, client started")


def _on_disconnect(_client, _userdata, disconnect_flags, reason_code, properties):
    LOGGER.info(
        "MQTT disconnected: reason=%s (%s), from_server=%s, properties=%s",
        reason_code,
        reason_code.value,
        disconnect_flags.is_disconnect_packet_from_server,
        properties,
    )
    _mqtt_connected.clear()


def mqtt_stop() -> None:
    """
    Clean shutdown.
    """
    global _mqtt_client

    client = _mqtt_client
    if client is None:
        return

    try:
        # Set disconnect reason so the 'will' for the availability topic
        # will be published after the configured delay
        reason = ReasonCode(
            PacketTypes.DISCONNECT,
            "Disconnect with will message",     # Magic string, do not change!
        )

        client.disconnect(reasoncode=reason)
        client.loop_stop()

        _mqtt_connected.clear()
        _mqtt_client = None

    except Exception as exc:
        LOGGER.error(
            "MQTT: Error while stopping: %s",
            exc,
        )

# endregion
#--------------------------------------------------------------------------------------------------

#--------------------------------------------------------------------------------------------------
# region Topic/payload generation for a single zone
#--------------------------------------------------------------------------------------------------

def _zone_payloads(zone: ThermostatData) -> dict[str, str]:
    """Generate the topics and their payloads for the given zone."""

    base = f"{MQTT_ROOT}/{zone.zone_id}"

    # Calculate new 'keepalive' value
    now = time.time()
    keepalive_cycle = math.floor((zone.keepalive - zone.keepalive_offset)/MQTT_KEEPALIVE_INTERVAL_SECONDS)
    next_keepalive = (keepalive_cycle + 1) * MQTT_KEEPALIVE_INTERVAL_SECONDS + zone.keepalive_offset
    if now >= next_keepalive:
        zone.keepalive = now

    result: dict[str, str] = {
        f"{base}/online": _bool_payload(zone.online),
        f"{base}/is_timeclock": _bool_payload(zone.is_timeclock),
        f"{base}/keepalive": str(int(zone.keepalive)),
    }
    if zone.zone_name:
        result[f"{base}/name"] = zone.zone_name

    if zone.is_timeclock:
        result[f"{base}/timeclock/enabled"] = _bool_payload(zone.enabled)
        result[f"{base}/timeclock/demand"] = _bool_payload(zone.demand)

    else:
        result[f"{base}/thermostat/current_temperature"] = _float_payload(zone.current_temperature)
        result[f"{base}/thermostat/target_temperature"] = _float_payload(zone.target_temperature)
        result[f"{base}/thermostat/enabled"]             = _bool_payload(zone.enabled)
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


# endregion
#--------------------------------------------------------------------------------------------------

#--------------------------------------------------------------------------------------------------
# region Publishing topics
#--------------------------------------------------------------------------------------------------

def mqtt_publish_zones(zones: dict[int, ThermostatData]) -> None:
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

        # First, publish discovery, but only if we are forced to republish
        # everything
        if force:
            if not mqtt_publish_homeassistant_discovery(zones):
                return

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

# endregion
#--------------------------------------------------------------------------------------------------

#--------------------------------------------------------------------------------------------------
# region Discovery configuration for home assistant
#--------------------------------------------------------------------------------------------------

def mqtt_publish_homeassistant_discovery(zones: dict[int, ThermostatData]) -> bool:
    client = _mqtt_client

    if client is None or not _mqtt_connected.is_set():
        return False

    all_ok = True

    for zone_id, zone in zones.items():

        # Timeclocks don't have thermostat/floor climate entities
        if zone.is_timeclock:
            continue

        base = f"{MQTT_ROOT}/{zone_id}"

        device = {
            "identifiers": [f"heatmiser_zone_{zone_id}"],
            "name": zone.zone_name,
            "manufacturer": "Heatmiser",
        }

        availability = [
            {
                "topic": f"{MQTT_ROOT}/available",
                "payload_available": "true",
                "payload_not_available": "false",
            },
            {
                "topic": f"{base}/online",
                "payload_available": "true",
                "payload_not_available": "false",
            },
        ]

        # ---------------------------------------------------------------
        # Thermostat
        # ---------------------------------------------------------------

        thermostat_config = {
            "name":                         None,
            "icon":                         "mdi:thermometer",
            "unique_id":                    f"heatmiser_{zone_id}_thermostat",
            "default_entity_id":            f"climate.heatmiser_{zone_id}_thermostat",

            "device":                       device,
            "availability":                 availability,
            "availability_mode":            "all",

            "modes":                        ["off", "heat"],
            "mode_state_topic":             f"{base}/thermostat/enabled",
            "mode_state_template":          "{{ 'heat' if value == 'true' else 'off' }}",
            "mode_command_topic":           f"{base}/thermostat/set/enabled",
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

        info = client.publish(
            topic,
            json.dumps(thermostat_config),
            qos=MQTT_QOS,
            retain=True,
        )
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            all_ok = False

        # ---------------------------------------------------------------
        # Floor limiter
        # ---------------------------------------------------------------

        floor_config = {
            "name":                          "(Floor)",
            "icon":                          "mdi:heating-coil",
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
            "temp_step":                     1.0,

        }

        topic = (
            f"{MQTT_DISCOVERY_PREFIX}/climate/"
            f"heatmiser_{zone_id}_floor/config"
        )

        info = client.publish(
            topic,
            json.dumps(floor_config),
            qos=MQTT_QOS,
            retain=True,
        )
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            all_ok = False

    return all_ok

# endregion
#--------------------------------------------------------------------------------------------------


#--------------------------------------------------------------------------------------------------
# region Incoming command handling
#--------------------------------------------------------------------------------------------------

_MQTT_COMMANDS = {
    ("thermostat", "enabled"):            ("thermostat_set_enabled",     "bool"),
    ("thermostat", "target_temperature"): ("thermostat_set_temperature", "float"),
    ("floor",      "limit_temperature"):  ("thermostat_set_floor_limit", "float"),
    ("timeclock",  "enabled"):            ("timeclock_set_enabled",      "bool"),
}


_mqtt_asyncio_loop: asyncio.AbstractEventLoop | None = None

def mqtt_set_command_processing_loop(loop: asyncio.AbstractEventLoop) -> None:
    """Set the loop on which the commands are executed"""
    # call in main with (asyncio.get_running_loop()) as argument
    global _mqtt_asyncio_loop
    _mqtt_asyncio_loop = loop

def _on_message(client, userdata, message) -> None:

    # Do not process an accidentally-retained command.
    if message.retain:
        LOGGER.warning(
            "Ignoring retained MQTT command: %s",
            message.topic,
        )
        return

    # Split the topic
    # heatmiser/<zone_id>/<type>/set/<field>
    parts = message.topic.split("/")
    if (
        len(parts) != 5
        or parts[0] != MQTT_ROOT
        or parts[3] != "set"
    ):
        LOGGER.warning(f"MQTT: Unknown command topic: '{message.topic}'")
        return

    try:
        zone_id = int(parts[1])
        command = (parts[2], parts[4])

        # get the command definition
        command_definition = _MQTT_COMMANDS.get(command)
        if command_definition is None:
            raise ValueError("Unsupported command")

        handler_name, argument_type = command_definition

        handler = getattr(mqtt_handlers, handler_name)
        if handler is None:
            raise RuntimeError(f"Handler '{handler_name}' is not configured")

        if _mqtt_asyncio_loop is None:
            raise RuntimeError("asyncio event loop is not configured")

        payload = message.payload.decode("utf-8")
        value = _parse(payload, argument_type)

        future = asyncio.run_coroutine_threadsafe(
            handler(zone_id, value),
            _mqtt_asyncio_loop,
        )

        future.add_done_callback(
            lambda f, topic=message.topic: _mqtt_command_done(topic, f)
        )

    except (UnicodeDecodeError, ValueError, RuntimeError) as exc:
        LOGGER.warning(
            "MQTT: Ignoring command '%s': %s",
            message.topic,
            exc,
        )


def _mqtt_command_done(topic: str, future) -> None:
    try:
        future.result()
    except Exception:
        LOGGER.exception(
            "MQTT command failed: %s",
            topic,
        )


_TRUE_VALUES = {"1", "true", "on", "heat"}
_FALSE_VALUES = {"0", "false", "off"}

def _parse(payload: str, type: str):
    if type == "bool":
        return _parse_bool(payload)
    elif type == "float":
        return _parse_float(payload)
    return None
    
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

# endregion
#--------------------------------------------------------------------------------------------------

#--------------------------------------------------------------------------------------------------
# region Loop on which to run command execution
#--------------------------------------------------------------------------------------------------

        
# endregion
#--------------------------------------------------------------------------------------------------
