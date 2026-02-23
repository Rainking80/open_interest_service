import asyncio
from datetime import datetime, timedelta, timezone
from hyperliquid.info import Info
from hyperliquid.utils import constants
from database import AsyncSessionLocal, OIFunding, init_db
from sqlalchemy import select
import logging
import requests
import threading
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fetcher")

# Target pairs provided by user
PAIRS_CONFIG = [
    "AAVE/USDC:USDC", "ARB/USDC:USDC", "AVAX/USDC:USDC", "BTC/USDC:USDC",
    "DOGE/USDC:USDC", "ETH/USDC:USDC", "ETHFI/USDC:USDC", "FIL/USDC:USDC",
    "LINK/USDC:USDC", "NEAR/USDC:USDC", "SOL/USDC:USDC",
    "SUI/USDC:USDC", "WLD/USDC:USDC"
]

def parse_pair(p):
    return p.split('/')[0]

TARGET_COINS = {parse_pair(p): p for p in PAIRS_CONFIG}

info = Info(constants.MAINNET_API_URL, skip_ws=False)

# Track subscriptions and volume cache
subscribed_coins = set()
volume_cache = defaultdict(lambda: {"buy": 0.0, "sell": 0.0})
volume_lock = threading.Lock()

def trade_callback(message):
    if message.get("channel") == "trades":
        data = message.get("data", [])
        for trade in data:
            coin = trade.get("coin")
            if coin in TARGET_COINS:
                side = trade.get("side") # 'B' for taking ask (buy), 'A' for taking bid (sell)
                try:
                    vol = float(trade.get("sz", "0")) # Base asset volume
                    with volume_lock:
                        if side == 'B':
                            volume_cache[coin]["buy"] += vol
                        elif side == 'A':
                            volume_cache[coin]["sell"] += vol
                except ValueError:
                    pass

def subscribe_if_needed(coin):
    if coin not in subscribed_coins:
        try:
            info.subscribe({"type": "trades", "coin": coin}, trade_callback)
            subscribed_coins.add(coin)
            logger.info(f"Subscribed to trades for {coin}")
        except Exception as e:
            logger.error(f"Failed to subscribe to trades for {coin}: {e}")

async def initialize_pairs():
    # 1. Load existing pairs from database to ensure monitored pairs are never dropped
    try:
        async with AsyncSessionLocal() as session:
            result = await session.execute(select(OIFunding.pair).distinct())
            existing_pairs = result.scalars().all()
            
        count_restored = 0
        for pair in existing_pairs:
            coin = parse_pair(pair)
            if coin not in TARGET_COINS:
                TARGET_COINS[coin] = pair
                if pair not in PAIRS_CONFIG:
                    PAIRS_CONFIG.append(pair)
                count_restored += 1
            
            # Ensure websocket subscription
            subscribe_if_needed(coin)
                
        if count_restored > 0:
            logger.info(f"Restored {count_restored} pairs from database history.")
            
    except Exception as e:
        logger.error(f"Error restoring pairs from DB: {e}")

    # 2. Update with current top 50 (sync network call)
    await asyncio.to_thread(update_pairs_to_top_50)

def update_pairs_to_top_50():
    logger.info("Updating pairs to include top 50 by volume...")
    try:
        # Use existing info instance
        meta_data = info.meta_and_asset_ctxs()
        universe = meta_data[0]['universe']
        asset_ctxs = meta_data[1]
        logger.info(f"Retrieved {len(universe)} assets from Hyperliquid")
        
        market_data = []
        for i, asset in enumerate(universe):
            market_data.append({
                "name": asset['name'],
                "volume": float(asset_ctxs[i].get('dayNtlVlm', 0))
            })
            
        # Sort by volume descending
        sorted_by_volume = sorted(market_data, key=lambda x: x['volume'], reverse=True)
        top_50 = sorted_by_volume[:50]
        
        count_added = 0
        for item in top_50:
            coin = item['name']
            if coin not in TARGET_COINS:
                # Construct pair string. Assumption: All are USDC pairs.
                pair_str = f"{coin}/USDC:USDC"
                PAIRS_CONFIG.append(pair_str)
                logger.info(f"Added new top pair: {pair_str} (Vol: {item['volume']:,.2f})")
            
            # Ensure websocket subscription     
            subscribe_if_needed(coin)
                
        logger.info(f"Added {count_added} new pairs. Total pairs: {len(PAIRS_CONFIG)}")
        
    except Exception as e:
        logger.error(f"Error updating top 50 pairs: {e}")

async def store_data(pair, timestamp, oi, funding, taker_buy_volume, taker_sell_volume, cvd):
    async with AsyncSessionLocal() as session:
        # Check if exists
        result = await session.execute(
            select(OIFunding).where(
                OIFunding.pair == pair,
                OIFunding.timestamp == timestamp
            )
        )
        existing = result.scalars().first()
        if not existing:
            new_entry = OIFunding(
                pair=pair,
                timestamp=timestamp,
                open_interest=oi,
                funding_rate=funding,
                taker_buy_volume=taker_buy_volume,
                taker_sell_volume=taker_sell_volume,
                cvd=cvd
            )
            session.add(new_entry)
            await session.commit()
            logger.info(f"Stored {pair} at {timestamp}: OI={oi}, Funding={funding}, TakerBuyV={taker_buy_volume}, TakerSellV={taker_sell_volume}, CVD={cvd}")
        else:
            logger.info(f"Entry already exists for {pair} at {timestamp}")

async def fetch_and_store():
    logger.info("Starting fetch job...")
    
    # 1. Fetch global state for OI (Snapshots are efficient)
    try:
        # meta_and_asset_ctxs is synchronous in the SDK but conducts network calls.
        # Run in executor to avoid blocking if using async scheduler (though simple scheduling is fine)
        # For simplicity, just call it.
        meta_data = info.meta_and_asset_ctxs()
        universe = meta_data[0]['universe']
        asset_ctxs = meta_data[1]
    except Exception as e:
        logger.error(f"Error fetching meta_and_asset_ctxs: {e}")
        return

    # Map coin name to asset context
    # universe has names. asset_ctxs is a list corresponding to universe?
    # Actually meta_and_asset_ctxs returns (meta, asset_ctxs)
    # meta['universe'] is list of dicts with 'name'
    # asset_ctxs is list of contexts.
    
    coin_to_ctx = {}
    for i, asset in enumerate(universe):
        name = asset['name']
        if name in TARGET_COINS:
            coin_to_ctx[name] = asset_ctxs[i]

    # Current time
    now = datetime.now(timezone.utc)
    # Align to the start of the current hour (or end of previous).
    # If this runs at 0 minute, we are recording the state at that time.
    current_candle_time = now.replace(minute=0, second=0, microsecond=0)
    
    # 2. Loop through target pairs
    for coin, pair_name in TARGET_COINS.items():
        if coin not in coin_to_ctx:
            logger.warning(f"Coin {coin} not found in Hyperliquid universe")
            continue
        
        ctx = coin_to_ctx[coin]
        
        # Get Open Interest
        # ctx structure: {'dayNtlVlm': ..., 'funding': ..., 'openInterest': '...', ...}
        try:
            oi = float(ctx.get('openInterest', 0))
            logger.info(f"[{coin}] Fetched OI: {oi}")
        except ValueError:
            oi = 0.0
            logger.warning(f"[{coin}] Failed to parse OI, default to 0.0")

        # Get Funding Rate
        
        start_ts = int((current_candle_time - timedelta(hours=1)).timestamp() * 1000)
        end_ts = int((current_candle_time + timedelta(minutes=5)).timestamp() * 1000)
        
        funding_rate = 0.0
        try:
            history = info.funding_history(coin, start_ts, end_ts)
            logger.debug(f"[{coin}] Funding history items: {len(history)}")
            
            found = False
            for item in history:
                ftime = item['time']
                ft_dt = datetime.fromtimestamp(ftime/1000, timezone.utc)
                if abs((ft_dt - current_candle_time).total_seconds()) < 600: # 10 mins
                    funding_rate = float(item['fundingRate'])
                    found = True
                    logger.info(f"[{coin}] Found matched funding rate: {funding_rate} at {ft_dt}")
                    break
            
            if not found:
                 logger.warning(f"[{coin}] No specific funding found at {current_candle_time}. Fallback to predicted.")
                 funding_rate = float(ctx.get('funding', 0))
                 logger.info(f"[{coin}] Predicted funding rate: {funding_rate}")

        except Exception as e:
            if "429" in str(e):
                logger.warning(f"[{coin}] [V2] Handled 429 Rate Limit for funding history. Falling back to predicted.")
                await asyncio.sleep(5.0) # Additional massive backoff to cool down
            else:
                logger.error(f"Error fetching funding history for {coin}: {e}")
            funding_rate = float(ctx.get('funding', 0))

        # Get Accumlated Volume from Websocket Cache
        taker_buy_volume = 0.0
        taker_sell_volume = 0.0
        with volume_lock:
            # We snapshot the current accumulated volumes
            taker_buy_volume = volume_cache[coin]["buy"]
            taker_sell_volume = volume_cache[coin]["sell"]
            # Reset cache for the next hour
            volume_cache[coin]["buy"] = 0.0
            volume_cache[coin]["sell"] = 0.0
            
        cvd = taker_buy_volume - taker_sell_volume
        logger.info(f"[{coin}] WS Accumulated Taker Buy: {taker_buy_volume:.4f}, Taker Sell: {taker_sell_volume:.4f}, CVD: {cvd:.4f}")

        # Store
        await store_data(pair_name, current_candle_time, oi, funding_rate, taker_buy_volume, taker_sell_volume, cvd)
        
        # Add a delay to avoid rate-limiting from the exchange (max 60 req/min)
        await asyncio.sleep(2.0)

if __name__ == "__main__":
    async def main():
        await init_db()
        await initialize_pairs()
        await fetch_and_store()
        
    import os
    asyncio.run(main())
    os._exit(0)
