#!/usr/bin/env python3
import asyncio
import websockets
import aiohttp # use async http client
import json
import random # for jitter
import sys # for exit
import socket
from openpilot.common.params import Params
from selfdrive.payton_connect.payton_logger import log_message # Import the new logger

dongle_id = Params().get("DongleId")

# --- config ---
WS_SERVER = "ws://connect.paytondev.cloud/connect" # fix this obvs
LOCAL_API_BASE = "http://127.0.0.1:8082" # base url for sunnypilot api
MAX_RETRY_DELAY = 60 # seconds
# --- end config ---

async def connect_and_listen():
    retry_delay = 1
    # make one http session for reuse across posts
    async with aiohttp.ClientSession() as http_session:
        while True:
            try:
                log_message(f"connecting to ws: {WS_SERVER}...")
                # add keepalive pings
                async with websockets.connect(WS_SERVER, ping_interval=20, ping_timeout=20) as ws:
                    log_message(f"connected to ws: {WS_SERVER}")
                    # Ensure dongle_id is bytes before decoding
                    dongle_id_str = dongle_id.decode('utf-8') if isinstance(dongle_id, bytes) else str(dongle_id)
                    await ws.send(dongle_id_str) # send dongle_id upon connection
                    retry_delay = 1 # reset backoff on successful connect

                    # main loop listening for messages
                    async for msg in ws:
                        try:
                            log_message(f"raw msg received: {msg}") # log raw msg
                            message_data = json.loads(msg)

                            # --- validate incoming message structure ---
                            if (isinstance(message_data, dict) and
                                "endpoint" in message_data and isinstance(message_data["endpoint"], str) and
                                "data" in message_data and
                                "method" in message_data and isinstance(message_data["method"], str)):

                                endpoint = message_data["endpoint"].strip('/')
                                data_payload = message_data["data"]
                                method = message_data["method"].upper()
                                target_url = f"{LOCAL_API_BASE}/{endpoint}"

                                # --- Special Handling for Server's 'get' Request ---
                                if endpoint == "locations" and method == "GET":
                                    log_message(f"Handling specific request: {method} {target_url}")
                                    # Call req_local_api and WAIT for it to complete and send response
                                    # Pass ws (aliased as socket within req_local_api)
                                    await req_local_api(http_session, method, target_url, data_payload, ws)
                                    log_message(f"Finished handling specific request: {method} {target_url}")
                                # --- Regular Asynchronous Command Handling ---
                                else:
                                    log_message(f"Handling general command: {method} {target_url}")
                                    # Fire and forget the post task for other commands
                                    asyncio.create_task(req_local_api(http_session, method, target_url, data_payload, ws))

                            else:
                                log_message(f"invalid command format: {msg}. expected {{'endpoint': str, 'data': <any>, 'method': str}}")
                        # --- end message validation ---

                        except json.JSONDecodeError:
                            log_message(f"ws msg decode failed (not json?): {msg}")
                        except Exception as e:
                            # catch errors within the message handling loop specifically
                            log_message(f"error processing ws message: {e}")

            # --- connection error handling & retry logic ---
            except (websockets.exceptions.ConnectionClosedError, websockets.exceptions.ConnectionClosedOK) as e:
                log_message(f"ws connection closed: {e}. reconnecting in {retry_delay:.2f}s...")
            except websockets.exceptions.InvalidURI:
                log_message(f"fatal: invalid ws uri: {WS_SERVER}. check config. stopping.")
                sys.exit(1) # exit script if uri is bad
            except ConnectionRefusedError:
                 log_message(f"connection refused by {WS_SERVER}. server down? reconnecting in {retry_delay:.2f}s...")
            except aiohttp.ClientConnectionError as e: # catch potential http session errors too if needed
                 log_message(f"http session connection error: {e}. reconnecting in {retry_delay:.2f}s...")
            except OSError as e: # broader network issues like dns fail
                 log_message(f"network os error: {e}. reconnecting in {retry_delay:.2f}s...")
            except Exception as e:
                import traceback
                tb_str = traceback.format_exc()
                # Log exception and traceback separately
                log_message(f"unexpected error in ws connect/listen loop: {e}")
                log_message(f"Traceback:\n{tb_str}")
                log_message(f"reconnecting in {retry_delay:.2f}s...")

            # exponential backoff with jitter
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2 + random.uniform(0, 1), MAX_RETRY_DELAY)
            # --- end connection error handling ---

async def req_local_api(session, method, url, payload, socket):
    # Basic validation for GET requests where payload might not be a dict initially
    # GET requests typically don't send a JSON body, but aiohttp handles empty dicts fine.
    # For robustness, ensure payload is a dict if method is POST/PUT etc.
    if method not in ["GET", "DELETE"] and not isinstance(payload, dict):
        log_message(f"invalid payload type for {method} {url}: {type(payload)}")
        return # Or send an error back via socket?

    log_message(f"attempting {method} to {url} with data: {json.dumps(payload)}")
    try:
        # Use params for GET, json for others. Handle potential empty payload for GET.
        request_args = {'timeout': 10}
        if method == "GET":
            # aiohttp uses 'params' for query string, not 'json' for GET body usually
            # If your local API *needs* a body for GET, use 'data=json.dumps(payload)' and set headers
             pass # No body or params needed for /locations GET based on current info
        else:
            request_args['json'] = payload

        async with session.request(method, url, **request_args) as resp:
            resp_text = await resp.text() # read response body
            # check status AND log response
            if resp.status >= 400:
                 log_message(f"error from local API {url}. status: {resp.status}, response: {resp_text}")
                 # Optionally send an error back via websocket
                 # await socket.send(json.dumps({"error": f"Local API failed: {resp.status}", "details": resp_text}))
            else:
                 log_message(f"successfully called local API {url}. status: {resp.status}, response: {resp_text}")
                 # Send the successful response text back via the websocket
                 await socket.send(resp_text) # Use await here
            # resp.raise_for_status() # optionally raise exception on bad status
    except aiohttp.ClientConnectionError as e:
        log_message(f"connection error contacting local API {url}: {e}")
        # await socket.send(json.dumps({"error": "Connection error to local API"}))
    except aiohttp.ClientError as e: # broader client errors (ssl, etc)
        log_message(f"client error contacting local API {url}: {e}")
        # await socket.send(json.dumps({"error": "Client error contacting local API"}))
    except asyncio.TimeoutError:
        log_message(f"timeout contacting local API {url}")
        # await socket.send(json.dumps({"error": "Timeout contacting local API"}))
    except Exception as e:
        import traceback
        tb_str = traceback.format_exc()
        # Log exception and traceback separately
        log_message(f"unexpected error in req_local_api ({url}): {e}")
        log_message(f"Traceback:\n{tb_str}")
        # Consider sending an error back via socket here too
        # await socket.send(json.dumps({"error": f"Internal error processing request for {url}"}))

async def check_internet_connection():
    """Check if internet connection is available by attempting to connect to a reliable host"""
    try:
        # Try to connect to a reliable host (Google's DNS server)
        socket.create_connection(("8.8.8.8", 53), timeout=5)
        return True
    except OSError:
        return False

async def wait_for_internet():
    """Wait until internet connection is available"""
    while not await check_internet_connection():
        log_message("Waiting for internet connection...")
        await asyncio.sleep(5)  # Check every 5 seconds

def main():
    log_message("starting websocket client daemon...")
    try:
        # requires `pip install websockets aiohttp`
        import websockets
        import aiohttp
    except ImportError as e:
        log_message(f"missing dependency: {e}. please install required packages.")
        sys.exit(1)

    try:
        # Wait for internet connection before proceeding
        asyncio.run(wait_for_internet())
        log_message("Internet connection established, starting websocket client...")
        asyncio.run(connect_and_listen())
    except KeyboardInterrupt:
        log_message("\nctrl+c detected, exiting.")
    finally:
        log_message("websocket client stopped.")

if __name__ == "__main__":
    main()