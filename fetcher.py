import asyncio
from datetime import datetime, timedelta, timezone
from hyperliquid.info import Info
from hyperliquid.utils import constants
from database import AsyncSessionLocal, OIFunding, init_db
from sqlalchemy import select, delete
import logging
import requests
import threading
from collections import defaultdict
import aiohttp

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

# Blacklist pairs that should not be monitored
BLACKLIST_PAIRS = {
    "OM/USDC:USDC", "FTT/USDC:USDC", "kBONK/USDC:USDC", "kPEPE/USDC:USDC", "kSHIB/USDC:USDC"
}

def parse_pair(p):
    return p.split('/')[0]

TARGET_COINS = {parse_pair(p): p for p in PAIRS_CONFIG if p not in BLACKLIST_PAIRS}

def create_info_instance():
    import time
    for attempt in range(6):
        try:
            return Info(constants.MAINNET_API_URL, skip_ws=False)
        except Exception as e:
            if attempt < 5:
                logger.warning(f"Failed to initialize Info object (attempt {attempt+1}/6). Retrying in 10 seconds... Error: {e}")
                time.sleep(10)
            else:
                logger.error("Could not create Hyperliquid Info object due to repeated errors.")
                raise e

async def create_info_instance_async():
    for attempt in range(6):
        try:
            return await asyncio.to_thread(Info, constants.MAINNET_API_URL, skip_ws=False)
        except Exception as e:
            if attempt < 5:
                logger.warning(f"Failed to initialize Info object (attempt {attempt+1}/6). Retrying in 10 seconds... Error: {e}")
                await asyncio.sleep(10)
            else:
                logger.error("Could not create Hyperliquid Info object due to repeated errors.")
                raise e

info = create_info_instance()

# Track subscriptions and volume cache
subscribed_coins = set()
volume_cache = defaultdict(lambda: {"buy": 0.0, "sell": 0.0})
volume_lock = threading.Lock()

def trade_callback(message):
    try:
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
    except Exception as e:
        logger.error(f"Error processing trade callback: {e}")

def subscribe_if_needed(coin):
    global info, subscribed_coins
    if coin not in subscribed_coins:
        try:
            info.subscribe({"type": "trades", "coin": coin}, trade_callback)
            subscribed_coins.add(coin)
            logger.info(f"Subscribed to trades for {coin}")
        except Exception as e:
            logger.error(f"Failed to subscribe to trades for {coin}: {e}")

async def initialize_pairs():
    # 0. Purge blacklisted pairs from the database
    if BLACKLIST_PAIRS:
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    delete(OIFunding).where(OIFunding.pair.in_(BLACKLIST_PAIRS))
                )
                await session.commit()
                if result.rowcount > 0:
                    logger.info(f"Purged {result.rowcount} rows for blacklisted pairs: {BLACKLIST_PAIRS}")
        except Exception as e:
            logger.error(f"Error purging blacklisted pairs from DB: {e}")

    # 1. Load existing pairs from database to ensure monitored pairs are never dropped
    try:
        async with AsyncSessionLocal() as session:
            result = await session.execute(select(OIFunding.pair).distinct())
            existing_pairs = result.scalars().all()
            
        count_restored = 0
        for pair in existing_pairs:
            if pair in BLACKLIST_PAIRS:
                continue
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
            pair_str = f"{coin}/USDC:USDC"
            
            if pair_str in BLACKLIST_PAIRS:
                continue
                
            if coin not in TARGET_COINS:
                # Construct pair string. Assumption: All are USDC pairs.
                PAIRS_CONFIG.append(pair_str)
                TARGET_COINS[coin] = pair_str
                logger.info(f"Added new top pair: {pair_str} (Vol: {item['volume']:,.2f})")
            
            # Ensure websocket subscription     
            subscribe_if_needed(coin)
                
        logger.info(f"Added {count_added} new pairs. Total pairs: {len(PAIRS_CONFIG)}")
        
    except Exception as e:
        logger.error(f"Error updating top 50 pairs: {e}")

async def check_websocket_health():
    """Watchdog to ensure WS connection is alive. Restarts if dead."""
    global info, subscribed_coins
    if info.ws_manager and not info.ws_manager.is_alive():
        logger.error("Websocket thread is dead! Reconnecting...")
        try:
            # Create a new Info instance which starts a new WS connection natively in async way
            info = await create_info_instance_async()
            subscribed_coins.clear()
            logger.info("New Info instance created. Resubscribing...")
            for coin in list(TARGET_COINS.keys()):
                subscribe_if_needed(coin)
        except Exception as e:
            logger.error(f"Error reconnecting websocket: {e}")

async def store_data(pair, timestamp, oi, funding, mark_price, taker_buy_volume, taker_sell_volume, cvd, exchange="Hyperliquid", open_val=0.0, close_val=0.0, high_val=0.0, low_val=0.0, volume_val=0.0, btc_info=0.0, eth_info=0.0, sma4h=0.0):
    async with AsyncSessionLocal() as session:
        # Check if exists
        result = await session.execute(
            select(OIFunding).where(
                OIFunding.pair == pair,
                OIFunding.timestamp == timestamp,
                OIFunding.exchange == exchange
            )
        )
        existing = result.scalars().first()
        if not existing:
            new_entry = OIFunding(
                pair=pair,
                exchange=exchange,
                timestamp=timestamp,
                open_interest=oi,
                funding_rate=funding,
                mark_price=mark_price,
                taker_buy_volume=taker_buy_volume,
                taker_sell_volume=taker_sell_volume,
                cvd=cvd,
                open=open_val,
                close=close_val,
                high=high_val,
                low=low_val,
                volume=volume_val,
                btc_informative=btc_info,
                eth_informative=eth_info,
                sma_slope_4h=sma4h
            )
            session.add(new_entry)
            await session.commit()
            logger.info(f"Stored {pair} at {timestamp}: OI={oi}, Funding={funding}, MarkPx={mark_price}, TakerBuyV={taker_buy_volume}, TakerSellV={taker_sell_volume}, CVD={cvd}")
        else:
            logger.info(f"Entry already exists for {pair} at {timestamp}")

async def fetch_and_store():
    logger.info("Starting fetch job...")
    
    # 1. Fetch global state for OI with retry logic
    meta_data = None
    for attempt in range(3):
        try:
            meta_data = await asyncio.to_thread(info.meta_and_asset_ctxs)
            break
        except Exception as e:
            logger.error(f"Error fetching meta_and_asset_ctxs (attempt {attempt+1}/3): {e}")
            if attempt < 2:
                wait_time = 10 * (attempt + 1)
                logger.info(f"Retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
            else:
                logger.error("All retries exhausted for meta_and_asset_ctxs. Aborting Hyperliquid fetch.")
                return
    
    universe = meta_data[0]['universe']
    asset_ctxs = meta_data[1]

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
    hl_results = {}
    start_ts = int((current_candle_time - timedelta(hours=1)).timestamp() * 1000)
    end_ts = int((current_candle_time + timedelta(minutes=5)).timestamp() * 1000)
    prev_hour_ts = int((current_candle_time - timedelta(hours=1)).timestamp() * 1000)
    
    for coin, pair_name in TARGET_COINS.items():
        try:
            if coin not in coin_to_ctx:
                logger.warning(f"Coin {coin} not found in Hyperliquid universe")
                continue
            
            ctx = coin_to_ctx[coin]
            
            # Get Open Interest
            try:
                oi = float(ctx.get('openInterest', 0))
                logger.info(f"[{coin}] Fetched OI: {oi}")
            except ValueError:
                oi = 0.0
                logger.warning(f"[{coin}] Failed to parse OI, default to 0.0")

            # Get Mark Price
            try:
                mark_price = float(ctx.get('markPx', 0))
                logger.info(f"[{coin}] Fetched Mark Price: {mark_price}")
            except ValueError:
                mark_price = 0.0
                logger.warning(f"[{coin}] Failed to parse Mark Price, default to 0.0")

            # Get Funding Rate
            funding_rate = 0.0
            try:
                history = await asyncio.to_thread(info.funding_history, coin, start_ts, end_ts)
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
                    await asyncio.sleep(5.0)
                else:
                    logger.error(f"Error fetching funding history for {coin}: {e}")
                funding_rate = float(ctx.get('funding', 0))

            # Get Accumulated Volume from Websocket Cache
            taker_buy_volume = 0.0
            taker_sell_volume = 0.0
            with volume_lock:
                taker_buy_volume = volume_cache[coin]["buy"]
                taker_sell_volume = volume_cache[coin]["sell"]
                volume_cache[coin]["buy"] = 0.0
                volume_cache[coin]["sell"] = 0.0
                
            cvd = taker_buy_volume - taker_sell_volume
            logger.info(f"[{coin}] WS Accumulated Taker Buy: {taker_buy_volume:.4f}, Taker Sell: {taker_sell_volume:.4f}, CVD: {cvd:.4f}")

            # Fetch OHLCV candle data from Hyperliquid
            open_val = 0.0
            close_val = 0.0
            high_val = 0.0
            low_val = 0.0
            volume_val = 0.0
            try:
                candles = await asyncio.to_thread(
                    info.candles_snapshot, coin, "1h", prev_hour_ts, end_ts
                )
                for c in candles:
                    if c["t"] == prev_hour_ts:
                        open_val = float(c["o"])
                        high_val = float(c["h"])
                        low_val = float(c["l"])
                        close_val = float(c["c"])
                        volume_val = float(c["v"])
                        logger.info(f"[{coin}] Candle OHLCV: O={open_val}, H={high_val}, L={low_val}, C={close_val}, V={volume_val}")
                        break
                else:
                    logger.warning(f"[{coin}] No candle found for {current_candle_time - timedelta(hours=1)}")
            except Exception as e:
                logger.error(f"[{coin}] Error fetching candles: {e}")

            # Store
            hl_results[coin] = {
                "pair": pair_name,
                "oi": oi,
                "funding_rate": funding_rate,
                "mark_price": mark_price,
                "taker_buy_volume": taker_buy_volume,
                "taker_sell_volume": taker_sell_volume,
                "cvd": cvd,
                "open_val": open_val,
                "close_val": close_val,
                "high_val": high_val,
                "low_val": low_val,
                "volume_val": volume_val
            }
            
            # Add a delay to avoid rate-limiting from the exchange (max 60 req/min)
            await asyncio.sleep(2.0)
        
        except Exception as e:
            logger.error(f"[{coin}] Unexpected error processing coin, skipping: {e}", exc_info=True)

    # Compute informative prices from BTC and ETH candle close
    btc_info = hl_results.get("BTC", {}).get("close_val", 0.0)
    eth_info = hl_results.get("ETH", {}).get("close_val", 0.0)

    def sort_order(c):
        if c in ("BTC", "ETH"): return 1
        return 0

    for coin in sorted(hl_results.keys(), key=sort_order):
        d = hl_results[coin]
        await store_data(
            d["pair"], current_candle_time, d["oi"], d["funding_rate"], d["mark_price"],
            d["taker_buy_volume"], d["taker_sell_volume"], d["cvd"],
            open_val=d["open_val"], close_val=d["close_val"], high_val=d["high_val"],
            low_val=d["low_val"], volume_val=d["volume_val"],
            btc_info=btc_info, eth_info=eth_info
        )

    logger.info(f"Hyperliquid fetch complete: {len(hl_results)}/{len(TARGET_COINS)} pairs stored at {current_candle_time}")

async def fetch_and_store_binance():
    logger.info("Starting Binance fetch job...")
    
    now = datetime.now(timezone.utc)
    current_candle_time = now.replace(minute=0, second=0, microsecond=0)
    prev_hour_ts = int((current_candle_time - timedelta(hours=1)).timestamp() * 1000)
    
    try:
        async with aiohttp.ClientSession() as session:
            # 1. Fetch Premium Index
            async with session.get("https://fapi.binance.com/fapi/v1/premiumIndex") as resp:
                if resp.status == 200:
                    premium_data = await resp.json()
                    premium_map = {item['symbol']: item for item in premium_data}
                else:
                    logger.warning(f"Binance premiumIndex returned {resp.status}")
                    premium_map = {}
                    
            binance_results = {}
            for coin, pair_name in TARGET_COINS.items():
                symbol = f"{coin}USDT"
                
                mark_price = 0.0
                funding_rate = 0.0
                if symbol in premium_map:
                    mark_price = float(premium_map[symbol].get('markPrice', 0))
                    funding_rate = float(premium_map[symbol].get('lastFundingRate', 0))
                else:
                    logger.warning(f"[Binance {coin}] Symbol {symbol} not found in premiumIndex")
                
                oi = 0.0
                try:
                    async with session.get(f"https://fapi.binance.com/fapi/v1/openInterest?symbol={symbol}") as oi_resp:
                        if oi_resp.status == 200:
                            oi_data = await oi_resp.json()
                            oi = float(oi_data.get('openInterest', 0))
                        else:
                            # Not an error necessarily, some pairs might not exist on Binance Futures
                            logger.warning(f"[Binance {coin}] openInterest returned {oi_resp.status}")
                except Exception as e:
                    logger.error(f"Error fetching OI for {coin} on Binance: {e}")

                taker_buy_volume = 0.0
                taker_sell_volume = 0.0
                cvd = 0.0
                open_val = 0.0
                close_val = 0.0
                high_val = 0.0
                low_val = 0.0
                volume_val = 0.0
                try:
                    # Fetch Klines to get real Taker Volume
                    async with session.get(f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=1h&limit=2") as kline_resp:
                        if kline_resp.status == 200:
                            klines = await kline_resp.json()
                            for k in klines:
                                if k[0] == prev_hour_ts:
                                    open_val = float(k[1])
                                    high_val = float(k[2])
                                    low_val = float(k[3])
                                    close_val = float(k[4])
                                    volume_val = float(k[5])
                                    total_volume = float(k[5])
                                    taker_buy_volume = float(k[9])
                                    taker_sell_volume = total_volume - taker_buy_volume
                                    cvd = taker_buy_volume - taker_sell_volume
                                    break
                        else:
                            logger.warning(f"[Binance {coin}] klines returned {kline_resp.status}")
                except Exception as e:
                    logger.error(f"Error fetching klines for {coin} on Binance: {e}")

                logger.info(f"[Binance {coin}] OI={oi:.4f}, MarkPx={mark_price:.4f}, FR={funding_rate:.6f}, BuyV={taker_buy_volume:.4f}, SellV={taker_sell_volume:.4f}")

                binance_results[coin] = {
                    "pair": pair_name,
                    "oi": oi,
                    "funding_rate": funding_rate,
                    "mark_price": mark_price,
                    "taker_buy_volume": taker_buy_volume,
                    "taker_sell_volume": taker_sell_volume,
                    "cvd": cvd,
                    "open_val": open_val,
                    "close_val": close_val,
                    "high_val": high_val,
                    "low_val": low_val,
                    "volume_val": volume_val
                }
                
                await asyncio.sleep(0.5)

            btc_info = binance_results.get("BTC", {}).get("close_val", 0.0)
            eth_info = binance_results.get("ETH", {}).get("close_val", 0.0)

            def sort_order_binance(c):
                if c in ("BTC", "ETH"): return 1
                return 0

            for coin in sorted(binance_results.keys(), key=sort_order_binance):
                d = binance_results[coin]
                await store_data(
                    d["pair"], current_candle_time, d["oi"], d["funding_rate"], d["mark_price"],
                    d["taker_buy_volume"], d["taker_sell_volume"], d["cvd"], exchange="Binance",
                    open_val=d["open_val"], close_val=d["close_val"], high_val=d["high_val"],
                    low_val=d["low_val"], volume_val=d["volume_val"],
                    btc_info=btc_info, eth_info=eth_info
                )

    except Exception as e:
        logger.error(f"Binance Fetch Job Failed: {e}")

if __name__ == "__main__":
    async def main():
        await init_db()
        await initialize_pairs()
        await fetch_and_store()
        await fetch_and_store_binance()
        
    import os
    asyncio.run(main())
    os._exit(0)
