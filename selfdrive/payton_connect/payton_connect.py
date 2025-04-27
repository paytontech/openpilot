#!/usr/bin/env python3
import asyncio
import websockets
import aiohttp # use async http client
import json
import random # for jitter
import sys # for exit
import socket
import time # for backoff sleep
import traceback # for logging detailed errors
import argparse # for command-line arguments
from typing import Any # For pydantic model

# --- New Imports ---
import tenacity
import pydantic
from asyncio import timeout as asyncio_timeout # Use alias to avoid confusion
# --- End New Imports ---

from openpilot.common.params import Params
from selfdrive.payton_connect.payton_logger import log_message # Import the new logger

dongle_id = Params().get("DongleId")

# --- config ---
# Use secure websockets by default
DEFAULT_WS_SERVER = "wss://connect.paytondev.cloud/connect" # Default URL
LOCAL_API_BASE = "http://127.0.0.1:8082" # base url for sunnypilot api
# MAX_CONN_RETRIES = 30 # Max attempts before giving up (User rejected this)

# Timeout values (seconds)
CONNECT_TIMEOUT = 20
SEND_TIMEOUT = 10
REQ_TIMEOUT = 15 # Outer timeout for request block
READ_TIMEOUT = 10
# --- end config ---

# --- Pydantic Schema Definition ---
class WebsocketCommand(pydantic.BaseModel):
    endpoint: str
    data: Any # Can be any type, specific validation might be needed elsewhere if required
    method: str # Could add validation e.g., Literal["GET", "POST", "PUT", "DELETE"]

    @pydantic.validator('method')
    def method_must_be_valid(cls, v):
        allowed_methods = {"GET", "POST", "PUT", "DELETE"}
        if v.upper() not in allowed_methods:
            raise ValueError(f'Method must be one of {allowed_methods}')
        return v.upper()

    @pydantic.validator('endpoint')
    def endpoint_must_not_be_empty(cls, v):
        if not v or not v.strip():
            raise ValueError('Endpoint must not be empty')
        return v.strip('/') # Ensure leading/trailing slashes are removed
# --- End Pydantic Schema Definition ---

# Backoff function similar to athenad
def backoff(retries: int) -> float:
  return random.uniform(0, min(128, 2 ** retries))

# --- Tenacity Retry Logic for Local API ---
def log_retry_attempt(retry_state: tenacity.RetryCallState):
    """Log before tenacity retries."""
    log_message(f"Retrying local API call {retry_state.args[1]} {retry_state.args[2]} "
                f"due to {retry_state.outcome.exception()}, "
                f"attempt {retry_state.attempt_number}...")

local_api_retryer = tenacity.retry(
    retry=tenacity.retry_if_exception_type((
        aiohttp.ClientConnectionError,
        aiohttp.ClientError,
        asyncio.TimeoutError, # Also retry on timeouts
        ConnectionRefusedError,
        OSError, # Broader network issues
    )),
    wait=tenacity.wait_exponential(multiplier=1, min=1, max=10),
    stop=tenacity.stop_after_attempt(3),
    before_sleep=log_retry_attempt,
    reraise=True # Reraise the exception if all retries fail
)
# --- End Tenacity Retry Logic ---


async def connect_and_listen(ws_url: str, http_session: aiohttp.ClientSession):
    """Attempts to connect to the websocket server once and handles the message loop."""
    log_message(f"Attempting to connect to ws: {ws_url}...")
    ws = None # Initialize ws to None
    try:
        # Add timeout to connect
        async with asyncio_timeout(CONNECT_TIMEOUT):
            ws = await websockets.connect(ws_url, ping_interval=20, ping_timeout=20)

        log_message(f"Connected to ws: {ws_url}")
        dongle_id_str = dongle_id.decode('utf-8') if isinstance(dongle_id, bytes) else str(dongle_id)

        # Add timeout to initial send
        async with asyncio_timeout(SEND_TIMEOUT):
            await ws.send(dongle_id_str)

        # main loop listening for messages
        while True: # Loop until connection closes or error
            try:
                async with asyncio_timeout(None): # Rely on websocket's internal ping/pong timeout for reads
                    msg = await ws.recv()

                log_message(f"raw msg received: {msg}")
                message_data = json.loads(msg) # Assume msg is str, handle potential bytes if needed

                # --- Pydantic Validation ---
                try:
                    cmd = WebsocketCommand.model_validate(message_data)
                    log_message(f"Validated command: {cmd.method} {cmd.endpoint}")

                    target_url = f"{LOCAL_API_BASE}/{cmd.endpoint}"

                    # --- Request Handling ---
                    if cmd.endpoint == "locations" and cmd.method == "GET":
                        log_message(f"Handling specific request: {cmd.method} {target_url}")
                        await req_local_api(http_session, cmd.method, target_url, cmd.data, ws)
                        log_message(f"Finished handling specific request: {cmd.method} {target_url}")
                    else:
                        log_message(f"Handling general command: {cmd.method} {target_url}")
                        asyncio.create_task(req_local_api(http_session, cmd.method, target_url, cmd.data, ws))
                    # --- End Request Handling ---

                except pydantic.ValidationError as e:
                    log_message(f"Invalid command format (Pydantic): {e}. Raw: {msg}")
                # --- End Pydantic Validation ---

            except json.JSONDecodeError:
                log_message(f"ws msg decode failed (not json?): {msg}")
            except asyncio.TimeoutError:
                # This timeout is for the ws.recv() - indicates potential hang or lost pong
                log_message(f"WebSocket read timeout (potential connection issue).")
                # The outer loop in run_client will handle reconnection.
                raise websockets.exceptions.ConnectionClosedError(1008, "Read Timeout") # Trigger reconnect
            except websockets.exceptions.ConnectionClosed:
                # Connection closed cleanly or unexpectedly, break loop to reconnect
                log_message("WebSocket connection closed during receive.")
                raise # Re-raise to be caught by run_client
            except asyncio.CancelledError:
                log_message("Receive loop cancelled.")
                raise # Propagate cancellation
            except Exception as e:
                log_message(f"Error processing ws message: {e}")
                tb_str = traceback.format_exc()
                log_message(f"Traceback:\n{tb_str}")
                # Continue processing other messages if possible, depends on error type

    except asyncio.TimeoutError:
        log_message(f"Timeout connecting to {ws_url} or sending initial message.")
        raise # Let run_client handle retry
    except websockets.exceptions.InvalidURI:
        log_message(f"Fatal: Invalid WebSocket URI: {ws_url}")
        raise # Let run_client handle exit
    except ConnectionRefusedError:
        log_message(f"Connection refused by {ws_url}")
        raise # Let run_client handle retry
    except (socket.gaierror, OSError) as e:
        log_message(f"Network/OS error during connect/initial send: {e}")
        raise # Let run_client handle retry
    except asyncio.CancelledError:
        log_message("Connection/Listen task cancelled.")
        # Ensure websocket is closed if cancellation happens mid-connection
        if ws and not ws.closed:
            await ws.close(code=1001, reason="Client shutting down")
        raise # Propagate cancellation
    except Exception as e:
        log_message(f"Unexpected error in connect_and_listen setup: {e}")
        if ws and not ws.closed:
             await ws.close(code=1011, reason="Unexpected client error")
        raise # Propagate to run_client


@local_api_retryer # Apply tenacity retry logic
async def req_local_api(session: aiohttp.ClientSession, method: str, url: str, payload: Any, socket: websockets.WebSocketClientProtocol):
    """Sends a request to the local API, handles response, includes timeouts and retries."""
    # Basic validation for GET requests (Payload validation primarily happens via Pydantic now)
    if method not in ["GET", "DELETE"] and not isinstance(payload, (dict, list)): # Allow lists for potential batch POSTs?
        log_message(f"invalid payload type for {method} {url}: {type(payload)}")
        return

    log_message(f"attempting {method} to {url} with data: {json.dumps(payload)}")
    try:
        request_args = {} # No default timeout here, handled by outer asyncio_timeout
        # No special handling needed for GET 'pass' anymore.
        # If payload is needed for GET, adjust API or this logic.
        if method != "GET":
             request_args['json'] = payload

        # Add outer timeout for the entire request block (connect, send, receive headers)
        async with asyncio_timeout(REQ_TIMEOUT):
            async with session.request(method, url, **request_args) as resp:
                # Add timeout for reading response body
                async with asyncio_timeout(READ_TIMEOUT):
                    resp_text = await resp.text()

                if resp.status >= 400:
                     log_message(f"error from local API {url}. status: {resp.status}, response: {resp_text}")
                     # Optionally inform server via websocket? Needs careful thought.
                     # await socket.send(...)
                else:
                     log_message(f"successfully called local API {url}. status: {resp.status}, response: {resp_text}")
                     # Add timeout for sending response back via websocket
                     async with asyncio_timeout(SEND_TIMEOUT):
                         # Ensure socket is still open before sending
                         if socket and not socket.closed:
                             await socket.send(resp_text)
                         else:
                             log_message(f"Cannot send local API response back, websocket closed.")

    except asyncio.TimeoutError:
        log_message(f"Timeout during local API call to {url} (stage specific).")
        # Tenacity will catch this and retry if configured
        raise # Re-raise for tenacity
    except aiohttp.ClientConnectionError as e:
        log_message(f"connection error contacting local API {url}: {e}")
        raise # Re-raise for tenacity
    except aiohttp.ClientError as e:
        log_message(f"client error contacting local API {url}: {e}")
        raise # Re-raise for tenacity
    except asyncio.CancelledError:
        log_message(f"Local API request to {url} cancelled.")
        raise # Propagate cancellation
    except Exception as e:
        # Catch unexpected errors during local API interaction
        tb_str = traceback.format_exc()
        log_message(f"unexpected error in req_local_api ({url}): {e}")
        log_message(f"Traceback:\n{tb_str}")
        # Don't automatically retry completely unknown errors, but log them.
        # Consider if specific non-aiohttp/timeout errors should be retried by Tenacity.

async def check_internet_connection():
    """Check if internet connection is available by attempting to connect to a reliable host"""
    try:
        # Try to connect to a reliable host (Google's DNS server)
        # Use asyncio's loop to create connection for async compatibility
        loop = asyncio.get_event_loop()
        # Add timeout to DNS check
        async with asyncio_timeout(5):
            await loop.create_connection(lambda: asyncio.Protocol(), "8.8.8.8", 53)
        return True
    except asyncio.TimeoutError:
        log_message("Timeout checking internet connection.")
        return False
    except OSError:
        return False
    except Exception as e:
        log_message(f"Error checking internet connection: {e}")
        return False


async def wait_for_internet():
    """Wait until internet connection is available"""
    while not await check_internet_connection():
        log_message("Waiting for internet connection...")
        await asyncio.sleep(5)  # Check every 5 seconds


async def run_client(ws_url: str):
    """Main async loop to manage connection, errors, and backoff."""
    conn_retries = 0
    conn_start_time = None

    # Create the ClientSession once, outside the loop
    async with aiohttp.ClientSession() as http_session:
        while True: # Main loop
            try:
                if conn_start_time is None:
                    conn_start_time = time.monotonic()

                # connect_and_listen attempts connection and handles the message loop.
                await connect_and_listen(ws_url, http_session)

                # If connect_and_listen returns gracefully (connection established and then closed cleanly by server):
                log_message("Websocket session ended gracefully by server. Resetting retry count.")
                conn_retries = 0
                conn_start_time = None
                await asyncio.sleep(1) # Brief pause

            # --- Handle specific connection errors for retry ---
            # Catch exceptions raised from connect_and_listen setup or during processing
            except (websockets.exceptions.ConnectionClosedError, websockets.exceptions.ConnectionClosedOK) as e:
                conn_retries += 1
                # Reset timer as connection attempt failed or session ended unexpectedly
                conn_start_time = None
                delay = backoff(conn_retries)
                log_message(f"ws connection closed: {e.code} {e.reason}. Retrying in {delay:.2f}s (attempt {conn_retries})...")
                await asyncio.sleep(delay)
            except ConnectionRefusedError as e:
                conn_retries += 1
                conn_start_time = None
                delay = backoff(conn_retries)
                log_message(f"connection refused by {ws_url}: {e}. Server down? Retrying in {delay:.2f}s (attempt {conn_retries})...")
                await asyncio.sleep(delay)
            except (socket.gaierror, OSError, asyncio.TimeoutError) as e: # Catch timeouts from connect/initial send here too
                conn_retries += 1
                conn_start_time = None
                delay = backoff(conn_retries)
                log_message(f"network/os/timeout error: {e}. Retrying in {delay:.2f}s (attempt {conn_retries})...")
                await asyncio.sleep(delay)
            except aiohttp.ClientConnectionError as e: # Should be less likely here now
                conn_retries += 1
                conn_start_time = None
                delay = backoff(conn_retries)
                log_message(f"http session connection error: {e}. Retrying in {delay:.2f}s (attempt {conn_retries})...")
                await asyncio.sleep(delay)
            except websockets.exceptions.InvalidURI:
                log_message(f"fatal: invalid ws uri: {ws_url}. check config or argument. stopping.")
                sys.exit(1) # Fatal config error

            # --- Handle unexpected errors & Cancellation ---
            except asyncio.CancelledError:
                log_message("Main client loop cancelled. Stopping.")
                # Perform any necessary cleanup before exiting
                # The aiohttp session is closed automatically by 'async with'
                break # Exit the while loop
            except Exception as e:
                conn_retries += 1
                conn_start_time = None
                delay = backoff(conn_retries)
                tb_str = traceback.format_exc()
                log_message(f"unexpected error in main loop: {e}")
                log_message(f"Traceback:\n{tb_str}")
                log_message(f"Retrying in {delay:.2f}s (attempt {conn_retries})...")
                await asyncio.sleep(delay)


def main():
    parser = argparse.ArgumentParser(description="Connect to Payton Connect WebSocket server.")
    parser.add_argument('--connect-url', type=str, default=DEFAULT_WS_SERVER,
                        help=f'WebSocket server URL (default: {DEFAULT_WS_SERVER})')
    args = parser.parse_args()

    ws_url_to_use = args.connect_url
    log_message(f"using websocket url: {ws_url_to_use}")

    log_message("starting payton_connect client daemon...")
    # Check dependencies synchronously before starting async loop
    try:
        import websockets
        import aiohttp
        import tenacity # Check new deps
        import pydantic
    except ImportError as e:
        log_message(f"missing dependency: {e}. please install required packages (websockets, aiohttp, tenacity, pydantic).")
        sys.exit(1)

    try:
        asyncio.run(wait_for_internet())
        log_message("Internet connection established.")
        log_message("Starting main client loop...")
        asyncio.run(run_client(ws_url_to_use))
    except KeyboardInterrupt:
        log_message("\nctrl+c detected, exiting.")
    except asyncio.CancelledError:
         log_message("Main program cancelled.") # Should be handled within run_client ideally
    except Exception as e:
        tb_str = traceback.format_exc()
        log_message(f"critical error in main execution: {e}")
        log_message(f"Traceback:\n{tb_str}")
    finally:
        log_message("payton_connect client stopped.")

if __name__ == "__main__":
    main()