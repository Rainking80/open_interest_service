import time
import threading
from hyperliquid.info import Info
from hyperliquid.utils import constants

info = Info(constants.MAINNET_API_URL, skip_ws=False)
volume_cache = {"BTC": 0.0}

def trade_callback(message):
    if message.get("channel") == "trades":
        for trade in message.get("data", []):
            if trade.get("coin") == "BTC":
                volume_cache["BTC"] += float(trade.get("sz", "0"))

info.subscribe({"type": "trades", "coin": "BTC"}, trade_callback)

print("Started")
time.sleep(10)
print("Volume:", volume_cache["BTC"])
