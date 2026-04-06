from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import select
from sqlalchemy import desc
from database import AsyncSessionLocal, OIFunding, init_db
from fetcher import fetch_and_store, fetch_and_store_binance, PAIRS_CONFIG, update_pairs_to_top_50, initialize_pairs, check_websocket_health
from healthcheck import run_healthcheck
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, timezone
import asyncio
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("service")

app = FastAPI()
scheduler = AsyncIOScheduler()

@app.on_event("startup")
async def startup_event():
    await init_db()
    
    async def healthcheck_job():
        # Using current UTC start of the hour
        candle_time = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        # PAIRS_CONFIG might have been dynamically updated by fetcher.py
        await run_healthcheck(candle_time, PAIRS_CONFIG)
    
    # Initialize pairs (DB restore + Top 50 update)
    logger.info("Calling initialize_pairs()...")
    await initialize_pairs()
    
    # Schedule fetching job
    # Run every hour at minute 0
    # Hyperliquid at minute=2 to give the exchange time to finalize the previous hour's candle
    scheduler.add_job(fetch_and_store, "cron", minute=2)
    scheduler.add_job(fetch_and_store_binance, "cron", minute=0)
    scheduler.add_job(healthcheck_job, "cron", minute=10)
    
    # Schedule WS watchdog every 1 minute
    scheduler.add_job(check_websocket_health, "interval", minutes=1)
    
    scheduler.start()
    logger.info("Scheduler started.")
    
    # Run immediately on startup
    logger.info("Triggering initial fetch...")
    asyncio.create_task(fetch_and_store())
    asyncio.create_task(fetch_and_store_binance())

@app.get("/pairs")
async def get_pairs():
    return PAIRS_CONFIG

@app.get("/data")
async def get_data(
    pair: str = Query(..., description="Trading pair, e.g. BTC/USDC:USDC"),
    exchange: str = Query("Hyperliquid", description="Exchange name"),
    timestamp: datetime = None,
    limit: int = Query(None, gt=0),
    since: datetime = Query(None, description="Cursor for pagination: returns rows with timestamp < since (older than this value)")
):
    async with AsyncSessionLocal() as session:
        query = select(OIFunding).where(OIFunding.pair == pair, OIFunding.exchange == exchange)
        
        if timestamp:
            # Query by specific timestamp
            query = query.where(OIFunding.timestamp == timestamp)
            result = await session.execute(query)
            item = result.scalars().first()
            if not item:
                raise HTTPException(status_code=404, detail="Data not found for timestamp")
            return item
        
        if limit:
            # Pagination: if `since` is provided, only return rows older than that timestamp
            if since:
                query = query.where(OIFunding.timestamp < since)
            # Query candles into the past, ordered newest-first
            query = query.order_by(desc(OIFunding.timestamp)).limit(limit)
            result = await session.execute(query)
            items = result.scalars().all()
            return items
            
        # Default: return current (latest)
        query = query.order_by(desc(OIFunding.timestamp)).limit(1)
        result = await session.execute(query)
        item = result.scalars().first()
        if not item:
            raise HTTPException(status_code=404, detail="No data found for pair")
        return item

@app.post("/trigger_fetch")
async def trigger_fetch():
    """Manually trigger data fetch"""
    asyncio.create_task(fetch_and_store())
    asyncio.create_task(fetch_and_store_binance())
    return {"status": "Fetch triggered"}
