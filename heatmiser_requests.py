
import json
from pathlib import Path
import asyncio
from const import REQUEST_TIMEOUT_SECONDS, LOGGER

ACCESS_TOKEN = "6b4f25a5-9de5-460a-a3ac-5845d3fbe095"


async def make_get_zones_request(websocket) -> dict:
    return await make_neohub_request(
        websocket,
        "{ 'GET_ZONES': 0 }",
    )


async def make_get_live_data_request(websocket) -> dict:
    return await make_neohub_request(
        websocket,
        "{ 'GET_LIVE_DATA': 0 }",
    )


async def make_info_request(
    websocket,
    zone_names: list[str],
) -> dict:
    response = await make_neohub_request(
        websocket,
        "{ 'INFO': [" 
        + ",".join(f"'{s}'" for s in zone_names)
        + "]}"
    )

    requested_zones = set(zone_names)

    response["devices"] = [
        device
        for device in response.get("devices", [])
        if device.get("device") in requested_zones
    ]

    return response


async def make_get_engineers_request(websocket) -> dict:
    return await make_neohub_request(
        websocket,
        "{ 'GET_ENGINEERS': 0 }",
    )



def build_request(command: str) -> str:
    message = {
        "token": ACCESS_TOKEN,
        "COMMANDS": [
            {
                "COMMAND": command,
                "COMMANDID": 1,
            }
        ],
    }

    request = {
        "message_type": "hm_get_command_queue",
        "message": json.dumps(message, separators=(",", ":")),
    }

    return json.dumps(request, separators=(",", ":"))


async def make_neohub_request(websocket, command: str) -> dict:

    request = build_request(command)

    LOGGER.debug(f"Sending command: {request}'")
    await websocket.send(request)

    raw_response = await asyncio.wait_for(
        websocket.recv(),
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    LOGGER.debug(f"Received response: {raw_response:<200}'")

    try:
        response_wrapper = json.loads(raw_response)
        response = json.loads(response_wrapper["response"])

    except Exception as exc:
        LOGGER.error(f"Error in neohub response: {exc}", exc)
        LOGGER.error(f" - Request: {request}", exc)
        LOGGER.error(f" - Response: {raw_response}", exc)
        raise Exception(f"Error in neohub response: {exc}", exc)

    if "error" in response:
        error_message = response["error"]
        LOGGER.error(f"Error in neohub request: '{error_message}'")
        LOGGER.error(f" - Request: '{request}'")
        raise Exception(f"Error in neohub request: '{error_message}'")


    return response