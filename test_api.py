import requests
import json
import time

url = "https://api.hyperliquid.xyz/info"
headers = {"Content-Type": "application/json"}

def test_endpoint(payload):
    print(f"Testing payload: {json.dumps(payload)}")
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        print("Success!")
        # Print a snippet
        print(str(data)[:500])
        return data
    except Exception as e:
        print(f"Error: {e}")
        if response is not None:
             print(response.text[:500])
        return None

# Test 1: meta
print("\n--- Test 1: Meta ---")
meta = test_endpoint({"type": "meta"})

# Test 2: funding history for a coin
print("\n--- Test 2: Funding History ---")
# Need to find the universe index for BTC from meta? Or just coin name?
# Usually funding history takes a coin (like "BTC" or "ETH")
# Check if universe is needed.
# Let's try sending "userFunding" or "fundingHistory"
# Checking docs from search results: "fundingHistory" method in SDK. 
# Payload likely: {"type": "fundingHistory", "coin": "BTC", "startTime": START_TIME, "endTime": END_TIME}

now = int(time.time() * 1000)
start = now - (24 * 3600 * 1000) # 24 hours ago

test_endpoint({
    "type": "fundingHistory",
    "coin": "BTC",
    "startTime": start,
    "endTime": now
})

# Test 3: Open Interest History?
print("\n--- Test 3: Open Interest History (Guess) ---")
test_endpoint({
    "type": "openInterestHistory",
    "coin": "BTC",
    "startTime": start,
    "endTime": now
})

# Test 4: Candle Snapshot?
print("\n--- Test 4: Candle Snapshot ---")
test_endpoint({
    "type": "candleSnapshot",
    "coin": "BTC",
    "interval": "1h",
    "startTime": start,
    "endTime": now
})
