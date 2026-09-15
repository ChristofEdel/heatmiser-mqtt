#--------------------------------------------------------------------------------------------------
# region Imports
#--------------------------------------------------------------------------------------------------

import json
from pathlib import Path
import asyncio
from const import REQUEST_TIMEOUT_SECONDS, LOGGER

ACCESS_TOKEN = "6b4f25a5-9de5-460a-a3ac-5845d3fbe095"

# endregion
#--------------------------------------------------------------------------------------------------


#--------------------------------------------------------------------------------------------------
# region Requests for data from the Neohub
#--------------------------------------------------------------------------------------------------

async def make_get_zones_request(websocket) -> dict:
    """Make a GET_ZONES request to the Neohub and return the result"""
    return await _make_neohub_request(
        websocket,
        "{ 'GET_ZONES': 0 }",
    )


async def make_get_live_data_request(websocket) -> dict:
    """Make a GET_LIVE_DATA request to the Neohub and return the result"""
    return await _make_neohub_request(
        websocket,
        "{ 'GET_LIVE_DATA': 0 }",
    )

async def make_get_engineers_request(websocket) -> dict:
    """Make a GET_ENGINEERS request to the Neohub and return the result"""
    return await _make_neohub_request(
        websocket,
        "{ 'GET_ENGINEERS': 0 }",
    )

# endregion
#--------------------------------------------------------------------------------------------------


#--------------------------------------------------------------------------------------------------
# region Commands
#--------------------------------------------------------------------------------------------------

async def send_set_floor_limit_command(websocket, zone_name: str, limit_temp: float):
    # { 'SET_FLOOR' :[28, '<zone name>'] }
    request = f"{{ 'SET_FLOOR' : [{limit_temp:.0f}, '{zone_name}'] }}"
    LOGGER.info(f"Neohub: Command '{request}'")
    response = await _make_neohub_request(websocket, request)
    LOGGER.info(f"Neohub:  --> {response}")

async def send_set_temperature_setpoint_command(websocket, zone_name: str, temp_setpoint: float):
    # { 'SET_TEMP' :[21.5, '<zone name>'] }
    request = f"{{ 'SET_TEMP' : [{temp_setpoint:.1f}, '{zone_name}'] }}"
    LOGGER.info(f"Neohub: Command '{request}'")
    response = await _make_neohub_request(websocket, request)
    LOGGER.info(f"Neohub:  --> {response}")

async def send_set_standby_command(websocket, zone_name: str, standby: bool):
    command = "FROST_ON" if standby else "FROST_OFF"
    request = f"{{ '{command}' : ['{zone_name}'] }}"
    LOGGER.info(f"Neohub: Command '{request}'")
    response = await _make_neohub_request(websocket, request)
    LOGGER.info(f"Neohub:  --> {response}")

# endregion
#--------------------------------------------------------------------------------------------------


#--------------------------------------------------------------------------------------------------
# region Helpers: Request and response wrap/unwraping
#--------------------------------------------------------------------------------------------------

_neohub_request_lock = asyncio.Lock()

async def _make_neohub_request(websocket, command: str) -> dict:
    """
        Send a request to the websocket and wait for the response

        Wraps the command into the Neohub command queue data and unwraps the response
    """

    async with _neohub_request_lock:

        request = _build_request(command)

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


def _build_request(command: str) -> str:
    """Wrap the given command into the Neohub request structure"""
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




# endregion
#--------------------------------------------------------------------------------------------------
