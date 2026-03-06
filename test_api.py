import requests
import time

now = int(time.time() * 1000)
start = now - (2 * 3600 * 1000)
url = "https://api.hyperliquid.xyz/info"

# trying types:
types_to_try = ["openInterestHistory", "historicalOpenInterest", "oiHistory", "fundingHistory", "assetCtxHistory"]
for t in types_to_try:
    try:
        res = requests.post(url, json={"type": t, "coin": "BTC", "startTime": start, "endTime": now})
        print(t, ":", res.json() if res.status_code == 200 else res.status_code)
    except Exception as e:
        print(t, str(e))

