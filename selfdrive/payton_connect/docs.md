# Payton Connect Client - WebSocket Server Interaction

This document outlines how the `payton_connect.py` client interacts with the WebSocket server. This client runs on the openpilot device and acts as a bridge between the WebSocket server and the local sunnypilot API.

## Client Behavior

1.  **Connection**: The client attempts to connect to the WebSocket server specified by `WS_SERVER`.
2.  **Identification**: Upon successful connection, the client immediately sends its `DongleId` (a string) to the server.
3.  **Listening**: The client listens for incoming messages from the server.
4.  **Message Format**: The client expects messages from the server to be in JSON format with a specific structure:
    ```json
    {
      "endpoint": "<target_local_api_path>",
      "data": { ... payload ... }
    }
    ```
    -   `endpoint`: A string representing the path (without leading/trailing slashes) on the `LOCAL_API_BASE` (e.g., `http://127.0.0.1:8082`) where the `data` payload should be POSTed. Example: `"set_destination"`.
    -   `data`: A JSON object containing the data to be sent in the POST request body.
5.  **Action**: When a valid message is received, the client constructs the full local API URL (`LOCAL_API_BASE`/`endpoint`) and makes an asynchronous POST request to that URL with the `data` object as the JSON payload.
6.  **Error Handling**: The client handles connection errors, JSON decoding errors, and invalid message formats, logging them to its local file.
7.  **Reconnection**: If the connection is lost, the client attempts to reconnect with exponential backoff.

## Expected Server Messages

Based on the client's logic, here's how the server should format messages to interact with the local sunnypilot API.

### Example: Setting a Destination

To instruct the client to set a navigation destination, the server should send a JSON message like this:

```json
{
  "endpoint": "set_destination",
  "data": {
    "latitude": 34.0522,
    "longitude": -118.2437,
    "place_name": "Los Angeles City Hall",
    "save_type": "recent" 
  }
}
```

**Data Structure for `set_destination` endpoint:**

*   `latitude` (float, required): The latitude of the destination.
*   `longitude` (float, required): The longitude of the destination.
*   `place_name` (string, required): The name of the destination location.
*   `save_type` (string, optional): Specifies how the destination should be saved. Valid values are `"home"`, `"work"`, or `"recent"`. If omitted or invalid, the client-side API might default to `recent` or handle it as an error depending on its implementation (the `payton_connect.py` script simply passes it through).

**Notes for Server Developer:**

*   Ensure all messages sent to the client adhere to the `{"endpoint": "...", "data": {...}}` JSON structure.
*   The `payton_connect.py` script *does not* wait for a response from the local API POST request before processing the next WebSocket message. It fires off the POST request asynchronously.
*   The client identifies itself only once upon connection by sending the `DongleId`. The server should be prepared to receive this initial message. 