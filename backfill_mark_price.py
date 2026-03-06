"""
One-off script to backfill mark_price for historical rows.

Usage:
    python backfill_mark_price.py              # dry-run (shows what would be updated)
    python backfill_mark_price.py --apply      # actually writes to the database
"""

import asyncio
import sys
import time
import logging
from datetime import datetime, timezone
from collections import defaultdict

from hyperliquid.info import Info
from hyperliquid.utils import constants
from sqlalchemy import select, update
from database import AsyncSessionLocal, OIFunding, init_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("backfill_mark_price")

DRY_RUN = "--apply" not in sys.argv


def parse_coin(pair: str) -> str:
    """Extract coin name from pair string, e.g. 'BTC/USDC:USDC' -> 'BTC'"""
    return pair.split("/")[0]


async def get_rows_missing_mark_price():
    """Return all rows where mark_price is NULL, ordered by pair and timestamp."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(OIFunding)
            .where(OIFunding.mark_price.is_(None))
            .order_by(OIFunding.pair, OIFunding.timestamp)
        )
        return result.scalars().all()


def fetch_candles(info: Info, coin: str, start_ms: int, end_ms: int) -> dict:
    """
    Fetch 1h candles from Hyperliquid and return a dict mapping
    candle open-time (datetime UTC) -> close price (float).

    The API may return at most ~5000 candles per call, so we paginate
    forward in chunks of 4000 hours (~166 days) to be safe.
    """
    CHUNK_HOURS = 4000
    CHUNK_MS = CHUNK_HOURS * 3600 * 1000
    candle_map = {}

    cursor = start_ms
    while cursor < end_ms:
        chunk_end = min(cursor + CHUNK_MS, end_ms)
        try:
            candles = info.candles_snapshot(coin, "1h", cursor, chunk_end)
            for c in candles:
                open_time = datetime.fromtimestamp(c["t"] / 1000, tz=timezone.utc)
                close_price = float(c["c"])
                candle_map[open_time] = close_price
            logger.info(
                f"  [{coin}] Fetched {len(candles)} candles "
                f"({datetime.fromtimestamp(cursor/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')} "
                f"-> {datetime.fromtimestamp(chunk_end/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')})"
            )
        except Exception as e:
            logger.error(f"  [{coin}] Error fetching candles chunk: {e}")

        cursor = chunk_end
        time.sleep(0.5)  # rate-limit courtesy

    return candle_map


async def backfill():
    await init_db()

    rows = await get_rows_missing_mark_price()
    if not rows:
        logger.info("No rows with NULL mark_price found. Nothing to do.")
        return

    logger.info(f"Found {len(rows)} rows with NULL mark_price.")

    # Group rows by coin
    coin_rows = defaultdict(list)
    for row in rows:
        coin = parse_coin(row.pair)
        coin_rows[coin].append(row)

    logger.info(f"Coins to backfill: {list(coin_rows.keys())}")

    # Create Hyperliquid Info instance (skip websocket, we only need REST)
    info = Info(constants.MAINNET_API_URL, skip_ws=True)

    total_updated = 0
    total_missed = 0

    for coin, coin_row_list in coin_rows.items():
        # Determine time range needed
        timestamps = [r.timestamp.replace(tzinfo=timezone.utc) for r in coin_row_list]
        min_ts = min(timestamps)
        max_ts = max(timestamps)

        start_ms = int(min_ts.timestamp() * 1000) - 3600_000  # 1h before earliest
        end_ms = int(max_ts.timestamp() * 1000) + 3600_000    # 1h after latest

        logger.info(
            f"[{coin}] {len(coin_row_list)} rows to fill, "
            f"range {min_ts.strftime('%Y-%m-%d %H:%M')} -> {max_ts.strftime('%Y-%m-%d %H:%M')}"
        )

        # Fetch all candles for this coin in one batch
        candle_map = fetch_candles(info, coin, start_ms, end_ms)
        logger.info(f"[{coin}] Got {len(candle_map)} candles total")

        # Match and update
        updates = []
        missed = 0
        for row in coin_row_list:
            row_ts = row.timestamp.replace(tzinfo=timezone.utc)
            if row_ts in candle_map:
                updates.append({"row_id": row.id, "mark_price": candle_map[row_ts]})
            else:
                missed += 1
                logger.warning(
                    f"[{coin}] No candle match for row id={row.id} at {row_ts}"
                )

        if updates:
            if DRY_RUN:
                logger.info(
                    f"[{coin}] DRY RUN: Would update {len(updates)} rows. "
                    f"Sample: id={updates[0]['row_id']} -> mark_price={updates[0]['mark_price']}"
                )
            else:
                async with AsyncSessionLocal() as session:
                    for u in updates:
                        await session.execute(
                            update(OIFunding)
                            .where(OIFunding.id == u["row_id"])
                            .values(mark_price=u["mark_price"])
                        )
                    await session.commit()
                logger.info(f"[{coin}] Updated {len(updates)} rows.")

        total_updated += len(updates)
        total_missed += missed

        # Delay between coins to avoid rate limits
        time.sleep(1.0)

    mode = "DRY RUN" if DRY_RUN else "APPLIED"
    logger.info(f"Done ({mode}). Updated: {total_updated}, Missed: {total_missed}")


if __name__ == "__main__":
    if DRY_RUN:
        logger.info("=== DRY RUN MODE (pass --apply to write changes) ===")
    else:
        logger.info("=== APPLY MODE — changes will be written to DB ===")

    asyncio.run(backfill())
