from hyperliquid.info import Info
from hyperliquid.utils import constants
import time

def callback(message):
    print("Received:", message)

info = Info(constants.MAINNET_API_URL, skip_ws=False)
info.subscribe({"type": "trades", "coin": "BTC"}, callback)

print("Listening for trades for 5 seconds...")
time.sleep(5)
info.disconnect_websocket()
