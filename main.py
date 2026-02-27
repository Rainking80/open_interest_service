from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import select
from sqlalchemy import desc
from database import AsyncSessionLocal, OIFunding, init_db
from fetcher import fetch_and_store, PAIRS_CONFIG, update_pairs_to_top_50, initialize_pairs, check_websocket_health
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
    scheduler.add_job(fetch_and_store, "cron", minute=0)
    scheduler.add_job(healthcheck_job, "cron", minute=5)
    
    # Schedule WS watchdog every 1 minute
    scheduler.add_job(check_websocket_health, "interval", minutes=1)
    
    scheduler.start()
    logger.info("Scheduler started.")
    
    # Run immediately on startup
    logger.info("Triggering initial fetch...")
    asyncio.create_task(fetch_and_store())

@app.get("/pairs")
async def get_pairs():
    return PAIRS_CONFIG

@app.get("/data")
async def get_data(pair: str = Query(..., description="Trading pair, e.g. BTC/USDC:USDC"), timestamp: datetime = None, limit: int = Query(None, gt=0)):
    async with AsyncSessionLocal() as session:
        query = select(OIFunding).where(OIFunding.pair == pair)
        
        if timestamp:
            # Query by specific timestamp
            query = query.where(OIFunding.timestamp == timestamp)
            result = await session.execute(query)
            item = result.scalars().first()
            if not item:
                raise HTTPException(status_code=404, detail="Data not found for timestamp")
            return item
        
        if limit:
            # Query candles into the past
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
    return {"status": "Fetch triggered"}
