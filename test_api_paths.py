import requests
import urllib.parse

base_url = "http://127.0.0.1:8000"

pair = "BTC/USDC:USDC"
# Query parameters work better

print(f"Testing pair: {pair} via Query String")
url = f"{base_url}/data"
params = {"pair": pair}

print(f"\nRequesting: {url} with params {params}")
try:
    r = requests.get(url, params=params)
    print(f"Status: {r.status_code}")
    if r.status_code == 200:
        print("Success! Data:", r.json())
    else:
        print("Failed:", r.text)
except Exception as e:
    print(f"Error: {e}")
