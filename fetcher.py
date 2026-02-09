import asyncio
from datetime import datetime, timedelta, timezone
from hyperliquid.info import Info
from hyperliquid.utils import constants
from database import AsyncSessionLocal, OIFunding, init_db
from sqlalchemy import select
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fetcher")

# Target pairs provided by user
PAIRS_CONFIG = [
    "AAVE/USDC:USDC", "ARB/USDC:USDC", "AVAX/USDC:USDC", "BTC/USDC:USDC",
    "DOGE/USDC:USDC", "ETH/USDC:USDC", "ETHFI/USDC:USDC", "FIL/USDC:USDC",
    "LINK/USDC:USDC", "NEAR/USDC:USDC", "PEPE/USDC:USDC", "SOL/USDC:USDC",
    "SUI/USDC:USDC", "WLD/USDC:USDC"
]

def parse_pair(p):
    return p.split('/')[0]

TARGET_COINS = {parse_pair(p): p for p in PAIRS_CONFIG}

info = Info(constants.MAINNET_API_URL, skip_ws=True)

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
                TARGET_COINS[coin] = pair_str
                count_added += 1
                logger.info(f"Added new top pair: {pair_str} (Vol: {item['volume']:,.2f})")
                
        logger.info(f"Added {count_added} new pairs. Total pairs: {len(PAIRS_CONFIG)}")
        
    except Exception as e:
        logger.error(f"Error updating top 50 pairs: {e}")

async def store_data(pair, timestamp, oi, funding):
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
                funding_rate=funding
            )
            session.add(new_entry)
            await session.commit()
            logger.info(f"Stored {pair} at {timestamp}: OI={oi}, Funding={funding}")
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
            logger.error(f"Error fetching funding history for {coin}: {e}")
            funding_rate = float(ctx.get('funding', 0))

        # Store
        await store_data(pair_name, current_candle_time, oi, funding_rate)

if __name__ == "__main__":
    async def main():
        await init_db()
        await initialize_pairs()
        await fetch_and_store()
        
    asyncio.run(main())
