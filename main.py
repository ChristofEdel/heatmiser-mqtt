import asyncio
import logging
import sys
from pathlib import Path

from websockets.exceptions import ConnectionClosed
from heatmiser_requests import make_get_zones_request, make_get_live_data_request, make_get_engineers_request
from heatmiser_connection import open_neohub_websocket
from const import MAX_CONSECUTIVE_REQUEST_FAILURES, LOGGER, NEOHUB_HOST, MQTT_HOST, MQTT_PORT, MQTT_CONNECT_RETRY_SECONDS 
from heatmiser_thermostat import ThermostatData
from mqtt_handler import mqtt_start, mqtt_stop, mqtt_publish, mqtt_publish_homeassistant_discovery


LOG_FILE = Path(__file__).with_name("neohub_mqtt.log")

all_zones: dict[int, ThermostatData] = {}

logging.basicConfig(
#    filename=LOG_FILE,
    stream=sys.stderr,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


async def run_connected(websocket) -> None:
    consecutive_failures = 0
    countdown = 10

    while True:

        # Let's get a request and process it
        try:

            response = await make_get_live_data_request(websocket)
            devices = response.get("devices", [])
            
            for device_data in devices:
                zone_id = device_data.get("DEVICE_ID",None)
                if zone_id is None:
                    LOGGER.error(f"GET_LIVE_DATA returned a block of data with no zone_id: {device_data}")
                    continue
                entry = all_zones.get(zone_id, None)
                if entry is None:
                    known_zones = ",".join(v.zone_name for v in all_zones.values())
                    LOGGER.error(f"GET_LIVE_DATA returned a block of data with an unknown zone id: {zone_id}")
                    LOGGER.error(f" - Known zones: {known_zones}")
                    continue
                entry.update_from_heatmiser_live_data(device_data)

            countdown -= 1
            if countdown <= 0:
                countdown = 10
                response = await make_get_engineers_request(websocket)
                for device_name, device_data in response.items():
                    entry = next((v for v in all_zones.values() if v.zone_name == device_name), None)
                    if entry is None:
                        known_zones = ",".join(v.zone_name for v in all_zones.values())
                        LOGGER.error(f"GET_ENGINEERS returned a block of data with an unknown zone name: {device_name}")
                        LOGGER.error(f" - Known zones: {known_zones}")
                        continue
                    entry.add_heatmiser_engineers_data(device_data)

            mqtt_publish(all_zones)
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


async def main() -> int:

    # Establish the connectin to the MQTT broker. After this initial connection succeeds,
    # the MQTT module 's network thread handles reconnection automatically.
    try: 
        await connect_mqtt_broker()

        while True:
            # open the websocket connection
            uri = f"wss://{NEOHUB_HOST}:4243"
            websocket = await open_neohub_websocket(uri)

            if websocket is None:
                # give up
                return 1

            try:
                zone_data = await make_get_zones_request(websocket)
                for zone_name, zone_id in zone_data.items():
                    if zone_id not in all_zones:
                        all_zones[zone_id] = ThermostatData.create(zone_id, zone_name)
                    else:
                        all_zones[zone_id].zone_name = zone_name

                # Ensure discovery exists whenever the zone list is established.
                mqtt_publish_homeassistant_discovery(all_zones)
                # Otherwise run the main loop until the websocket drops or we get killed
                await run_connected(websocket)

            finally:
                # Clean-up
                try:
                    LOGGER.info("Closing WebSocket")
                    await websocket.close()
                except Exception as exc:
                    LOGGER.error("Error while closing WebSocket: %s", exc)

    finally:
        try:
            LOGGER.info("Stopping MQTT")
            mqtt_stop()
        except Exception as exc:
            LOGGER.error(
                "Error while stopping MQTT: %s",
                exc,
            )


async def connect_mqtt_broker() -> None:
    while True:
        try:
            LOGGER.info(
                "Connecting to MQTT broker %s:%s",
                MQTT_HOST,
                MQTT_PORT,
            )

            # mqtt_start() contains a blocking socket connect, so keep it
            # off the asyncio event loop.
            await asyncio.to_thread(
                mqtt_start,
                MQTT_HOST,
                MQTT_PORT,
            )

            LOGGER.info("MQTT client started")
            return

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            LOGGER.error(
                "Unable to connect to MQTT broker %s:%s: %s; "
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