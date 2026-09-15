#--------------------------------------------------------------------------------------------------
# region Imports
#--------------------------------------------------------------------------------------------------

import asyncio
import logging
import sys
from pathlib import Path

from websockets.exceptions import ConnectionClosed
from heatmiser_requests import (
    make_get_live_data_request,
    make_get_engineers_request,
    send_set_standby_command,
    send_set_temperature_setpoint_command,
    send_set_floor_limit_command,
)
from heatmiser_connection import open_neohub_websocket, close_neohub_websocket
from const import MAX_CONSECUTIVE_REQUEST_FAILURES, LOGGER, NEOHUB_HOST, MQTT_HOST, MQTT_PORT, MQTT_CONNECT_RETRY_SECONDS
from heatmiser_thermostat import ThermostatData
from mqtt_handler import (
    mqtt_set_command_handlers,
    mqtt_set_command_processing_loop,
    mqtt_start,
    mqtt_stop,
    mqtt_publish_zones,
    mqtt_publish_homeassistant_discovery
)

# endregion
#--------------------------------------------------------------------------------------------------


#--------------------------------------------------------------------------------------------------
# region Logging set-up
#--------------------------------------------------------------------------------------------------

logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

# endregion
#--------------------------------------------------------------------------------------------------


#--------------------------------------------------------------------------------------------------
# region Dictionary with all known zones
#--------------------------------------------------------------------------------------------------

all_zones: dict[int, ThermostatData] = {}

def get_or_add_zone_from_response(device_data, log_new: bool = False) -> ThermostatData | None:
    zone_id = device_data.get("DEVICE_ID",None)
    zone_name = device_data.get("ZONE_NAME",None)
    if zone_id is None:
        LOGGER.error(f"GET_LIVE_DATA returned a block of data with no DEVICE_ID: {device_data}")
        return None
    if zone_name is None:
        LOGGER.error(f"GET_LIVE_DATA returned a block of data with no ZONE_NAME: {device_data}")
        return None
    entry = all_zones.get(zone_id, None)
    if entry is None:
        entry = ThermostatData.create(zone_id, zone_name)
        all_zones[zone_id] = entry
        entry.update_from_heatmiser_live_data(device_data)
        if (log_new):
            LOGGER.info(f"Added zone {zone_id}: {zone_name}")
    return entry

# endregion
#--------------------------------------------------------------------------------------------------


#--------------------------------------------------------------------------------------------------
# region Main processing loop
#--------------------------------------------------------------------------------------------------

_neohub_websocket = None

async def run_connected(websocket) -> None:
    consecutive_failures = 0
    countdown = 1

    while True:

        try:

            await get_and_process_live_data(websocket)

            countdown -= 1
            if countdown <= 0:
                countdown = 10
                await get_and_process_engineers_data(websocket)

            mqtt_publish_zones(all_zones)
            consecutive_failures = 0

        # If the connection hs been dropped, we exit. The surrounding loop will try
        # a reconnect
        except ConnectionClosed as exc:
            LOGGER.error(
                "WebSocket closed unexpectedly: code=%s reason=%s",
                exc.code,
                exc.reason,
            )
            return

        # Anything else - we retry

        # First, log the error; if too many close the socket and return
        except Exception as exc:
            consecutive_failures += 1

            LOGGER.error(
                "NeoHub request failed (%d/%d consecutive failures): %s",
                consecutive_failures,
                MAX_CONSECUTIVE_REQUEST_FAILURES,
                exc,
            )

            if consecutive_failures >= MAX_CONSECUTIVE_REQUEST_FAILURES:
                LOGGER.error(
                    "%d consecutive NeoHub requests failed; closing WebSocket "
                    "and reconnecting",
                    MAX_CONSECUTIVE_REQUEST_FAILURES,
                )

                try:
                    await websocket.close()
                except Exception as close_exc:
                    LOGGER.error(
                        "Error while closing WebSocket after request failures: %s",
                        close_exc,
                    )

                return

        # if there is no exception up to this point, we wait until the next request
        # is due, but detect a remote close immediately rather than waiting for the next send().
        try:
            await asyncio.wait_for(
                websocket.wait_closed(),
                timeout=1,
            )
        except TimeoutError:
            pass
        else:
            LOGGER.error(
                "WebSocket closed unexpectedly: code=%s reason=%s",
                websocket.close_code,
                websocket.close_reason,
            )
            return


async def get_and_process_live_data(websocket, log_new: bool = False):
    response = await make_get_live_data_request(websocket)
    devices = response.get("devices", [])
    for device_data in devices:
        entry = get_or_add_zone_from_response(device_data, log_new = log_new)
        if entry is None:
            known_zones = ",".join(v.zone_name for v in all_zones.values())
            LOGGER.error(f"GET_LIVE_DATA returned an unknown or invalid zone")
            LOGGER.error(f" - Known zones: {known_zones}")
            continue
        entry.update_from_heatmiser_live_data(device_data)


async def get_and_process_engineers_data(websocket):
    response = await make_get_engineers_request(websocket)
    for device_name, device_data in response.items():
        entry = next((v for v in all_zones.values() if v.zone_name == device_name), None)
        if entry is None:
            known_zones = ",".join(v.zone_name for v in all_zones.values())
            LOGGER.error(f"GET_ENGINEERS returned a block of data with an unknown zone name: {device_name}")
            LOGGER.error(f" - Known zones: {known_zones}")
            continue
        entry.add_heatmiser_engineers_data(device_data)


# endregion
#--------------------------------------------------------------------------------------------------


async def main() -> int:

    # set a 
    mqtt_set_command_processing_loop(asyncio.get_running_loop())

    # Establish the connectin to the MQTT broker. After this initial connection succeeds,
    # the MQTT module's network thread handles reconnection automatically.
    try:
        mqtt_set_command_handlers(
            thermostat_set_enabled      = handle_device_enabled,
            timeclock_set_enabled       = handle_device_enabled,
            thermostat_set_temperature  = handle_set_temperature,
            thermostat_set_floor_limit  = handle_set_floor_limit,
        )
        await connect_mqtt_broker()

        while True:
            # open the websocket connection
            uri = f"wss://{NEOHUB_HOST}:4243"
            websocket = await open_neohub_websocket(uri)

            if websocket is None:
                # give up
                return 1

            global _neohub_websocket
            _neohub_websocket = websocket

            try:
                # Make two initial requests for data so we have all device data
                # ready for MQTT publishing
                await get_and_process_live_data(websocket, log_new = False)
                await get_and_process_engineers_data(websocket)

                # Publish the discovery data for the zones we have now
                mqtt_publish_homeassistant_discovery(all_zones)

                # run the main loop until the websocket drops or we get killed
                await run_connected(websocket)

            finally:
                # Clean-up
                await close_neohub_websocket(websocket)

    finally:
        mqtt_stop()

async def handle_device_enabled(zone_id: int, enabled: bool) -> None:
    zone = all_zones.get(zone_id, None)
    if zone is None:
        raise Exception(f"Invalid zone Id:{zone_id}")
    await send_set_standby_command(_neohub_websocket, zone.zone_name, not enabled)

async def handle_set_temperature(zone_id: int, value: float) -> None:
    zone = all_zones.get(zone_id, None)
    if zone is None:
        raise Exception(f"Invalid zone Id:{zone_id}")
    await send_set_temperature_setpoint_command(_neohub_websocket, zone.zone_name, value)

async def handle_set_floor_limit(zone_id: int, value: float) -> None:
    zone = all_zones.get(zone_id, None)
    if zone is None:
        raise Exception(f"Invalid zone Id:{zone_id}")
    await send_set_floor_limit_command(_neohub_websocket, zone.zone_name, value)


async def connect_mqtt_broker() -> None:
    while True:
        try:
            # mqtt_start() contains a blocking socket connect, so keep it
            # off the asyncio event loop.
            await asyncio.to_thread(
                mqtt_start,
                MQTT_HOST,
                MQTT_PORT,
            )
            return

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            LOGGER.error(
                "MQTT: Unable to connect to broker %s:%s: %s; "
                "retrying in %d seconds",
                MQTT_HOST,
                MQTT_PORT,
                exc,
                MQTT_CONNECT_RETRY_SECONDS,
            )

            await asyncio.sleep(MQTT_CONNECT_RETRY_SECONDS)

if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        pass