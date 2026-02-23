from hyperliquid.info import Info
from hyperliquid.utils import constants

info = Info(constants.MAINNET_API_URL, skip_ws=True)
import time
now_ms = int(time.time() * 1000)
start_ms = now_ms - 3600000 * 2 # 2 hours ago
res = info.candles_snapshot("BTC", "1h", start_ms, now_ms)
print(res)
