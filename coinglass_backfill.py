import asyncio
import aiohttp
import os
import logging
from datetime import datetime, timedelta, timezone
from database import AsyncSessionLocal, OIFunding, init_db
from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("coinglass_fetcher")

# Configuration
COINGLASS_API_KEY = os.getenv("COINGLASS_API_KEY", "YOUR_COINGLASS_API_KEY_HERE")
COINGLASS_BASE_URL = "https://open-api-v4.coinglass.com"  # Updated to Coinglass V4 Base URL
EXCHANGES = ["Binance", "Hyperliquid"]
TIMEFRAME = "h1"
DAYS_BACK = 180

# The base PAIRS we track in the system. 
# We'll map these to Coinglass symbol format (e.g. BTC/USDC:USDC -> BTC)
TARGET_PAIRS = [
    "AAVE/USDC:USDC", "ARB/USDC:USDC", "AVAX/USDC:USDC", "BTC/USDC:USDC",
    "DOGE/USDC:USDC", "ETH/USDC:USDC", "ETHFI/USDC:USDC", "FIL/USDC:USDC",
    "LINK/USDC:USDC", "NEAR/USDC:USDC", "SOL/USDC:USDC",
    "SUI/USDC:USDC", "WLD/USDC:USDC"
]

def parse_symbol(pair: str) -> str:
    # Extracts "BTC" from "BTC/USDC:USDC"
    return pair.split('/')[0]

async def fetch_coinglass_data(session: aiohttp.ClientSession, url: str, params: dict) -> dict:
    headers = {
        "accept": "application/json",
        "CG-API-KEY": COINGLASS_API_KEY  # V4 API Uses CG-API-KEY
    }
    
    try:
        async with session.get(url, headers=headers, params=params) as response:
            if response.status == 200:
                data = await response.json()
                if str(data.get("code", "")) == "0" or data.get("success"):
                    return data.get("data", {})
                else:
                    logger.error(f"Coinglass API Error: {data.get('msg')} for URL {url}")
            elif response.status == 429:
                logger.warning(f"Rate limited on {url}. Sleeping 5 seconds...")
                await asyncio.sleep(5)
                # Simple retry
                return await fetch_coinglass_data(session, url, params)
            else:
                text = await response.text()
                logger.error(f"HTTP Error {response.status} on {url}: {text}")
    except Exception as e:
        logger.error(f"Request failed: {e}")
    
    return {}

async def process_pair(session, pair, start_time, end_time):
    base_coin = parse_symbol(pair)
    logger.info(f"Processing {base_coin} ({pair}) from {start_time} to {end_time}")
    
    merged_data = {}
    
    def get_or_create(exchange_name, ts):
        key = (exchange_name, ts)
        if key not in merged_data:
            from datetime import datetime
            merged_data[key] = {
                "exchange": exchange_name,
                "pair": pair,
                "timestamp": datetime.fromtimestamp(ts / 1000, tz=timezone.utc),
                "open_interest": 0.0,
                "funding_rate": 0.0,
                "mark_price": 0.0,
                "taker_buy_volume": 0.0,
                "taker_sell_volume": 0.0,
                "cvd": 0.0,
            }
        return merged_data[key]

    for exchange in EXCHANGES:
        logger.info(f"Fetching data for {base_coin} on {exchange}")
        
        # Determine symbol format based on exchange
        if exchange == "Hyperliquid":
            symbol = base_coin  # Hyperliquid uses base coin, e.g., 'BTC'
        else:
            symbol = f"{base_coin}USDT"  # Binance generally uses 'BTCUSDT'
            
        # V4 Endpoints (adjust if using a different plan)
        oi_url = f"{COINGLASS_BASE_URL}/api/futures/open-interest/history"
        funding_url = f"{COINGLASS_BASE_URL}/api/futures/funding-rate/history"
        vol_url = f"{COINGLASS_BASE_URL}/api/futures/v2/taker-buy-sell-volume/history"
        
        common_params = {
            "exchange": exchange,     # Note: Coinglass v4 shifted to 'exchange' param 
            "symbol": symbol,
            "interval": TIMEFRAME,
            "startTime": int(start_time.timestamp() * 1000),
            "endTime": int(end_time.timestamp() * 1000)
        }

        # 1. Fetch Open Interest History
        oi_data = await fetch_coinglass_data(session, oi_url, common_params) 
        if oi_data and isinstance(oi_data, list):
            for item in oi_data:
                ts = item.get("createTime") or item.get("t")
                if ts:
                    entry = get_or_create(exchange, ts)
                    entry["open_interest"] += float(item.get("o") or item.get("openInterest") or 0)
                    if "c" in item or "price" in item:
                        entry["mark_price"] = float(item.get("c") or item.get("price") or 0)

        # 2. Fetch Funding Rates History
        funding_data = await fetch_coinglass_data(session, funding_url, common_params)
        if funding_data and isinstance(funding_data, list):
            for item in funding_data:
                ts = item.get("createTime") or item.get("t")
                if ts:
                    entry = get_or_create(exchange, ts)
                    entry["funding_rate"] = float(item.get("fundingRate") or item.get("fr") or 0)

        # 3. Fetch Taker Buy / Taker Sell Volume History
        vol_data = await fetch_coinglass_data(session, vol_url, common_params)
        if vol_data and isinstance(vol_data, list):
            for item in vol_data:
                ts = item.get("createTime") or item.get("t")
                if ts:
                    entry = get_or_create(exchange, ts)
                    entry["taker_buy_volume"] += float(item.get("buyVol") or item.get("v") or 0)
                    entry["taker_sell_volume"] += float(item.get("sellVol") or item.get("sv") or 0)
                    entry["cvd"] = entry["taker_buy_volume"] - entry["taker_sell_volume"]

        await asyncio.sleep(1.0)
        
    return list(merged_data.values())

async def store_batch_data(records):
    """Upsert records into the database."""
    if not records:
        return

    logger.info(f"Storing {len(records)} records to database...")
    async with AsyncSessionLocal() as db_session:
        for idx in range(0, len(records), 500):
            batch = records[idx:idx+500]
            # Convert dict records to sqlalchemy format, relying on SQLite ignore/on_conflict capabilities.
            # Using basic merge for simplicity, or we can check each. Let's do a reliable approach:
            for r in batch:
                # Check if exists
                stmt = select(OIFunding).where(
                    OIFunding.pair == r["pair"],
                    OIFunding.timestamp == r["timestamp"],
                    OIFunding.exchange == r["exchange"]
                )
                existing = (await db_session.execute(stmt)).scalars().first()
                
                if existing:
                    # Update fields
                    existing.open_interest = r["open_interest"]
                    existing.funding_rate = r["funding_rate"]
                    existing.mark_price = r["mark_price"] or existing.mark_price
                    existing.taker_buy_volume = r["taker_buy_volume"]
                    existing.taker_sell_volume = r["taker_sell_volume"]
                    existing.cvd = r["cvd"]
                else:
                    new_item = OIFunding(**r)
                    db_session.add(new_item)
            
            await db_session.commit()
            logger.info(f"Committed batch of {len(batch)} records")

async def main():
    await init_db()
    
    now = datetime.now(timezone.utc)
    # Align to current hour
    end_time = now.replace(minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(days=DAYS_BACK)
    
    logger.info(f"Starting Coinglass backfill for {DAYS_BACK} days.")
    logger.info(f"Time Range: {start_time} to {end_time}")
    logger.info(f"Exchanges: {EXCHANGES}")
    
    async with aiohttp.ClientSession() as http_session:
        for pair in TARGET_PAIRS:
            try:
                records = await process_pair(http_session, pair, start_time, end_time)
                if records:
                    await store_batch_data(records)
                else:
                    logger.warning(f"No data retrieved for {pair}.")
            except Exception as e:
                logger.error(f"Failed to process pair {pair}: {e}")
            
            # Simple delay to avoid hammering the API between coins
            await asyncio.sleep(2)
            
    logger.info("Coinglass backfill complete.")

if __name__ == "__main__":
    asyncio.run(main())
