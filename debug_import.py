import hyperliquid
from hyperliquid.info import Info
from hyperliquid.utils import constants

print("Info loaded.")
info = Info(constants.MAINNET_API_URL, skip_ws=True)

# Test methods
print("Available:", dir(info))
