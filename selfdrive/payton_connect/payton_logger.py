import os
import datetime

LOG_DIR = "/data/openpilot/selfdrive/payton_connect/logs"
LOG_FILE = os.path.join(LOG_DIR, "log0.txt")

# Ensure the log directory exists
os.makedirs(LOG_DIR, exist_ok=True)

def log_message(message: str):
    """Appends a timestamped message to the log file."""
    print(message)
    try:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # Millisecond precision
        log_entry = f"[{timestamp}] {message}\n"
        with open(LOG_FILE, 'a') as f:
            f.write(log_entry)
    except Exception as e:
        # Fallback to print if logging fails
        print(f"Logging failed: {e}")
        print(f"Original message: {message}")

# Example usage (optional, for testing)
if __name__ == "__main__":
    log_message("Logging module initialized.")
    log_message("This is a test log message.")