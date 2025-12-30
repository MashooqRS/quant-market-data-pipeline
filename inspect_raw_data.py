import json
import os

# Define the directory where your raw data is stored
RAW_DATA_DIR = "raw_data"

# Alpaca raw JSON key that contains the bar data
BARS_KEY = "bars"


def inspect_and_count_data():
    """
    Loads the raw JSON files and prints the number of market data points (rows)
    for each stock symbol (Alpaca format).
    """
    if not os.path.exists(RAW_DATA_DIR):
        print(f"Error: The directory '{RAW_DATA_DIR}' was not found.")
        return

    print("\n--- Raw Data Inspection Report (Alpaca) ---")

    # Loop through all files in the 'raw_data' folder
    for filename in os.listdir(RAW_DATA_DIR):
        if filename.endswith(".json"):
            symbol = filename.split("_")[0]
            file_path = os.path.join(RAW_DATA_DIR, filename)

            try:
                with open(file_path, "r") as f:
                    data = json.load(f)

                # Alpaca ingest payload should contain "bars" as a list
                if BARS_KEY in data and isinstance(data[BARS_KEY], list):
                    record_count = len(data[BARS_KEY])
                    timeframe = data.get("timeframe", "unknown")
                    feed = data.get("feed", "unknown")
                    print(f"| {symbol:<5} | Rows: {record_count:,} | TF: {timeframe:<6} | Feed: {feed}")

                elif "message" in data:
                    # Some APIs return error messages in "message"
                    print(f"| {symbol:<5} | Error: {data['message']}")

                else:
                    print(f"| {symbol:<5} | WARNING: Could not find '{BARS_KEY}' list. Structure may be unexpected.")

            except json.JSONDecodeError:
                print(f"| {symbol:<5} | ERROR: File is not valid JSON.")
            except Exception as e:
                print(f"| {symbol:<5} | An unexpected error occurred: {e}")

    print("------------------------------------------\n")


if __name__ == "__main__":
    inspect_and_count_data()
