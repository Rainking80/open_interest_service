"""
Backfill OHLCV data for Hyperliquid rows that have NULL open/high/low/close/volume.

Uses the Hyperliquid SDK's candles_snapshot() to fetch historical 1h candles
and updates existing database rows in-place.

Usage:
    python backfill_ohlc_hyperliquid.py
"""

import sqlite3
import time
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from hyperliquid.info import Info
from hyperliquid.utils import constants

DB_PATH = 'data/open_interest.db'

def main():
    # --- 1. Connect to DB and find rows needing backfill ---
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute("PRAGMA journal_mode = MEMORY")
    cur = conn.cursor()

    # Ensure OHLC columns exist (for older databases that predate these columns)
    columns_to_add = [
        ("open", "FLOAT"),
        ("close", "FLOAT"),
        ("high", "FLOAT"),
        ("low", "FLOAT"),
        ("volume", "FLOAT"),
        ("btc_informative", "FLOAT"),
        ("eth_informative", "FLOAT"),
        ("sma_slope_4h", "FLOAT"),
    ]
    for col_name, col_type in columns_to_add:
        try:
            cur.execute(f'ALTER TABLE oi_funding ADD COLUMN "{col_name}" {col_type}')
        except Exception:
            pass  # Column already exists
    conn.commit()

    cur.execute("""
        SELECT id, pair, timestamp
        FROM oi_funding
        WHERE exchange = 'Hyperliquid'
          AND ("open" IS NULL OR "high" IS NULL OR "low" IS NULL OR "close" IS NULL OR "volume" IS NULL)
        ORDER BY pair, timestamp
    """)
    rows = cur.fetchall()

    if not rows:
        print("✅ No Hyperliquid rows with NULL OHLC found. Nothing to do.")
        conn.close()
        return

    print(f"📋 Found {len(rows):,} Hyperliquid rows with NULL OHLC data\n")

    # Group by coin for efficient batch API calls
    # pair format: "BTC/USDC:USDC" -> coin = "BTC"
    coin_rows = defaultdict(list)
    for row_id, pair, ts_str in rows:
        coin = pair.split('/')[0]
        coin_rows[coin].append((row_id, pair, ts_str))

    print(f"🪙  Coins to backfill: {', '.join(sorted(coin_rows.keys()))}\n")

    # --- 2. Connect to Hyperliquid API ---
    print("🔌 Connecting to Hyperliquid API...")
    info = Info(constants.MAINNET_API_URL, skip_ws=True)
    print("   Connected.\n")

    total_updated = 0
    total_skipped = 0

    for coin, entries in sorted(coin_rows.items()):
        print(f"━━━ {coin} ({len(entries)} rows) ━━━")

        # Parse all timestamps for this coin to find the time range
        timestamps = []
        for row_id, pair, ts_str in entries:
            try:
                # Handle both "YYYY-MM-DD HH:MM:SS" and "YYYY-MM-DD HH:MM:SS.000000"
                clean_ts = ts_str.split('.')[0]
                dt = datetime.strptime(clean_ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                timestamps.append((row_id, pair, ts_str, dt))
            except Exception as e:
                print(f"  ⚠️  Could not parse timestamp '{ts_str}': {e}")
                total_skipped += 1

        if not timestamps:
            continue

        # Sort by time
        timestamps.sort(key=lambda x: x[3])
        earliest = timestamps[0][3]
        latest = timestamps[-1][3]

        print(f"  📅 Range: {earliest} → {latest}")

        # Fetch candles in chunks (API may have limits on range).
        # Process in 30-day windows to avoid huge responses.
        CHUNK_DAYS = 30
        candle_map = {}  # timestamp_ms -> candle dict

        chunk_start = earliest
        while chunk_start <= latest:
            chunk_end_dt = min(chunk_start + timedelta(days=CHUNK_DAYS), latest + timedelta(hours=1))

            start_ms = int(chunk_start.timestamp() * 1000)
            end_ms = int(chunk_end_dt.timestamp() * 1000)

            try:
                candles = info.candles_snapshot(coin, "1h", start_ms, end_ms)
                for c in candles:
                    candle_map[c["t"]] = c
            except Exception as e:
                print(f"  ❌ API error for {coin} ({chunk_start} - {chunk_end_dt}): {e}")
                # If rate limited, wait and retry
                if "429" in str(e):
                    print("     Rate limited, waiting 60s...")
                    time.sleep(60)
                    try:
                        candles = info.candles_snapshot(coin, "1h", start_ms, end_ms)
                        for c in candles:
                            candle_map[c["t"]] = c
                    except Exception as e2:
                        print(f"  ❌ Retry also failed: {e2}")

            chunk_start = chunk_end_dt
            time.sleep(0.5)  # Rate limiting between chunks

        print(f"  📥 Fetched {len(candle_map)} candles from API")

        # --- 3. Match candles to DB rows and build update batch ---
        updates = []
        skipped = 0

        for row_id, pair, ts_str, dt in timestamps:
            # The DB row timestamp is the candle hour. The candle data we want
            # is for the PREVIOUS completed hour (that's what the fetcher stores).
            # But actually, the row's timestamp IS the candle time the data was recorded at.
            # We need to figure out which candle to use. Let's check:
            # - The fetcher records at current_candle_time (top of hour)
            # - It fetches the candle for prev_hour_ts (one hour before)
            # - So the DB row at timestamp T should have the candle that CLOSED at T,
            #   i.e., the candle with open time T-1h.
            # However, looking at the Binance fetcher, it also uses prev_hour_ts klines
            # but stores the candle data inline with current_candle_time.
            #
            # To be safe, try matching:
            #   1. Candle starting at (T - 1h) -- the completed candle model  
            #   2. Candle starting at T -- if the row represents candle-open time
            
            target_ms_prev = int((dt - timedelta(hours=1)).timestamp() * 1000)
            target_ms_same = int(dt.timestamp() * 1000)

            candle = candle_map.get(target_ms_prev) or candle_map.get(target_ms_same)

            if candle:
                open_val = float(candle["o"])
                high_val = float(candle["h"])
                low_val = float(candle["l"])
                close_val = float(candle["c"])
                volume_val = float(candle["v"])
                updates.append((open_val, high_val, low_val, close_val, volume_val, row_id))
            else:
                skipped += 1

        # Execute batch update
        if updates:
            cur.executemany(
                """UPDATE oi_funding 
                   SET "open" = ?, "high" = ?, "low" = ?, "close" = ?, "volume" = ?
                   WHERE id = ?""",
                updates
            )
            conn.commit()

        updated_count = len(updates)
        total_updated += updated_count
        total_skipped += skipped
        print(f"  ✅ Updated: {updated_count}, Skipped (no candle): {skipped}\n")

        # Rate limiting between coins
        time.sleep(1.0)

    # --- 4. Backfill btc_informative and eth_informative ---
    print("━━━ Backfilling btc_informative & eth_informative ━━━")

    # Build lookup of close prices for BTC and ETH from the DB (now that OHLC is filled)
    cur.execute("""
        SELECT timestamp, "close"
        FROM oi_funding
        WHERE exchange = 'Hyperliquid' AND pair = 'BTC/USDC:USDC' AND "close" IS NOT NULL
    """)
    btc_close_map = {ts: close for ts, close in cur.fetchall()}

    cur.execute("""
        SELECT timestamp, "close"
        FROM oi_funding
        WHERE exchange = 'Hyperliquid' AND pair = 'ETH/USDC:USDC' AND "close" IS NOT NULL
    """)
    eth_close_map = {ts: close for ts, close in cur.fetchall()}

    # Update all Hyperliquid rows that have null informative values
    cur.execute("""
        SELECT id, timestamp
        FROM oi_funding
        WHERE exchange = 'Hyperliquid'
          AND (btc_informative IS NULL OR eth_informative IS NULL)
    """)
    info_rows = cur.fetchall()

    info_updates = []
    for row_id, ts_str in info_rows:
        btc_val = btc_close_map.get(ts_str, 0.0) or 0.0
        eth_val = eth_close_map.get(ts_str, 0.0) or 0.0
        info_updates.append((btc_val, eth_val, row_id))

    if info_updates:
        cur.executemany(
            """UPDATE oi_funding
               SET btc_informative = ?, eth_informative = ?
               WHERE id = ?""",
            info_updates
        )
        conn.commit()
        print(f"  ✅ Updated {len(info_updates):,} rows with BTC/ETH informative prices\n")
    else:
        print("  ✅ No rows need informative price updates\n")

    # --- 5. Final summary ---
    cur.execute("""
        SELECT COUNT(*) FROM oi_funding
        WHERE exchange = 'Hyperliquid'
          AND ("open" IS NULL OR "high" IS NULL OR "low" IS NULL OR "close" IS NULL OR "volume" IS NULL)
    """)
    remaining_nulls = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM oi_funding WHERE exchange = 'Hyperliquid'")
    total_hl_rows = cur.fetchone()[0]

    print(f"📊 Summary:")
    print(f"   Total Hyperliquid rows:   {total_hl_rows:,}")
    print(f"   OHLC updated:             {total_updated:,}")
    print(f"   Skipped (no candle data): {total_skipped:,}")
    print(f"   Remaining NULL OHLC:      {remaining_nulls:,}")

    conn.close()
    print(f"\n💾 Done! Database: {DB_PATH}")


if __name__ == "__main__":
    main()
