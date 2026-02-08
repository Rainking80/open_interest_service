from hyperliquid.info import Info
from hyperliquid.utils import constants
import time

info = Info(constants.MAINNET_API_URL, skip_ws=True)

# Test Funding History
print("Fetching Funding History for BTC (last 24h)...")
now = int(time.time() * 1000)
start = now - (24 * 3600 * 1000)
try:
    fh = info.funding_history("BTC", start, now)
    print(f"Count: {len(fh)}")
    if len(fh) > 0:
        print("First item:", fh[0])
except Exception as e:
    print(f"Error fetching funding history: {e}")

# Test Candles to see if OI is there?
print("\nFetching Candles for BTC (1h)...")
try:
    candles = info.candles_snapshot("BTC", "1h", start, now)
    print(f"Count: {len(candles)}")
    if len(candles) > 0:
        print("First candle keys:", candles[0].keys() if isinstance(candles[0], dict) else candles[0])
        print("First candle:", candles[0])
except Exception as e:
    print(f"Error fetching candles: {e}")

# Check if there is any other method for OI history
print("\nChecking for other methods...")
methods = [m for m in dir(info) if not m.startswith("__")]
print("Methods:", methods)
