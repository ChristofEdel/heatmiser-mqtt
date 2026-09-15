import asyncio
import ssl

import websockets
from websockets.exceptions import ConnectionClosed
from const import MAX_OPEN_RETRIES, OPEN_RETRY_DELAY_SECONDS, LOGGER


async def open_neohub_websocket(uri: str):

    ssl_context = _create_ssl_context()
    LOGGER.info(f"Neohub: Connecting to {uri}")

    # One initial attempt, followed by MAX_OPEN_RETRIES retries.
    for attempt in range(MAX_OPEN_RETRIES + 1):
        try:
            result = await websockets.connect(
                uri,
                ssl=ssl_context,
                open_timeout=10,
                ping_interval=None
            )
            LOGGER.info(f"Neohub: Connected")
            return result
        except Exception as exc:
            LOGGER.error(
                "Neohub: failed to open WebSocket (attempt %d/%d): %s",
                attempt + 1,
                MAX_OPEN_RETRIES + 1,
                exc,
            )

            if attempt == MAX_OPEN_RETRIES:
                LOGGER.error("Giving up")
                return None

            await asyncio.sleep(OPEN_RETRY_DELAY_SECONDS)


    return None

def _create_ssl_context() -> ssl.SSLContext:
    # NeoHub generates its WSS certificate locally, so Heatmiser requires
    # certificate verification to be disabled for local port 4243 connections.
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return context

async def close_neohub_websocket(websocket):
    try:
        LOGGER.info("Neohub: Closing WebSocket")
        await websocket.close()
    except Exception as exc:
        LOGGER.error("Neohub: Error while closing WebSocket: %s", exc)


