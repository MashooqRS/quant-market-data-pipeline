import os
from alpaca.data.historical import StockHistoricalDataClient

client = StockHistoricalDataClient(
    os.getenv("ALPACA_API_KEY"),
    os.getenv("ALPACA_API_SECRET")
)

print("✅ Alpaca API connected successfully")


# import os
# print("KEY:", repr(os.getenv("ALPACA_API_KEY")))
# print("SECRET:", "SET" if os.getenv("ALPACA_API_SECRET") else None)
