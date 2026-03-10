import os
import glob
import sqlite3
import pandas as pd

DB_PATH = 'data/open_interest.db'
CSV_DIR = 'binance_output'

def main():
    csv_files = sorted(glob.glob(os.path.join(CSV_DIR, '*_Historical_Data_*.csv')))
    if not csv_files:
        print(f"❌ No csv files found in {CSV_DIR}/")
        return
    
    print(f"📂 Found {len(csv_files)} csv files\n")
    
    conn = sqlite3.connect(DB_PATH)
    # Performance pragmas
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute("PRAGMA journal_mode = MEMORY")
    cur = conn.cursor()
    
    # Ensure correct schema
    columns_to_add = [
        ("exchange", "VARCHAR DEFAULT 'Hyperliquid'"),
        ("mark_price", "FLOAT"),
        ("taker_buy_volume", "FLOAT"),
        ("taker_sell_volume", "FLOAT"),
        ("cvd", "FLOAT")
    ]
    for col_name, col_type in columns_to_add:
        try:
            cur.execute(f"ALTER TABLE oi_funding ADD COLUMN {col_name} {col_type}")
        except Exception:
            pass
    conn.commit()

    # Determine timestamp format from existing entries to match it exactly if needed
    cur.execute("SELECT timestamp FROM oi_funding LIMIT 1")
    sample_ts = cur.fetchone()
    if sample_ts:
        print(f"Sample existing timestamp in DB: {sample_ts[0]}")

    # Load existing pairs to memory for fast lookup
    print("Loading existing Binance data into memory for fast lookup...")
    cur.execute("SELECT id, pair, timestamp FROM oi_funding WHERE exchange = 'Binance'")
    
    existing_data = {}
    for row in cur.fetchall():
        id_val, pair_val, ts_val = row
        existing_data[(pair_val, ts_val)] = id_val

    total_inserted = 0
    total_updated = 0

    for fpath in csv_files:
        fname = os.path.basename(fpath)
        # Assuming format like AAVEUSDT_Historical_Data_...
        symbol = fname.split('_')[0]
        # remove USDT
        if symbol.endswith('USDT'):
            coin = symbol[:-4]
        else:
            coin = symbol
            
        pair = f"{coin}/USDC:USDC"
        
        df = pd.read_csv(fpath)
        if df.empty:
            continue
            
        # Parse datetimes
        df['datetime'] = pd.to_datetime(df['datetime'])
        df = df.sort_values('datetime')
        
        inserted = 0
        updated = 0
        
        inserts = []
        updates = []
        
        for _, row in df.iterrows():
            # Format timestamp as SQLite likes it: YYYY-MM-DD HH:MM:SS.000000
            # Some existing scripts might insert string as YYYY-MM-DD HH:MM:SS or YYYY-MM-DD HH:MM:SS.000000
            # From coinglass_backfill: datetime.fromtimestamp(ts, tz=utc) -> string representation in SQLite depends on SQLAlchemy which usually includes milliseconds if present, else not.
            # Using standard pandas strftime:
            ts_str = str(row['datetime'])  # Gives YYYY-MM-DD HH:MM:SS if seconds are 0
            if '.' not in ts_str:
                ts_str += '.000000'
                
            oi = float(row.get('sum_open_interest', 0.0))
            if pd.isna(oi): oi = 0.0
            
            fr = float(row.get('fundingRate', 0.0))
            if pd.isna(fr): fr = 0.0
            
            mp = float(row.get('markPrice', 0.0))
            if pd.isna(mp): mp = 0.0
            
            tbv = float(row.get('taker_buy_base_asset_volume', 0.0))
            if pd.isna(tbv): tbv = 0.0
            
            tsv = float(row.get('taker_sell_base_asset_volume', 0.0))
            if pd.isna(tsv): tsv = 0.0
            
            cvd = tbv - tsv
            
            # Key matching existing data
            key = (pair, ts_str)
            
            # Try without .000000 too
            alt_key = (pair, ts_str.replace('.000000', ''))
            
            if key in existing_data:
                row_id = existing_data[key]
                updates.append((oi, fr, mp, tbv, tsv, cvd, row_id))
                updated += 1
            elif alt_key in existing_data:
                row_id = existing_data[alt_key]
                updates.append((oi, fr, mp, tbv, tsv, cvd, row_id))
                updated += 1
            else:
                # Insert
                inserts.append(('Binance', pair, ts_str, oi, fr, mp, tbv, tsv, cvd))
                inserted += 1
                
        if inserts:
            cur.executemany(
                """INSERT INTO oi_funding 
                   (exchange, pair, timestamp, open_interest, funding_rate, mark_price, taker_buy_volume, taker_sell_volume, cvd)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                inserts
            )
        if updates:
            cur.executemany(
                """UPDATE oi_funding 
                   SET open_interest = ?, funding_rate = ?, mark_price = ?, taker_buy_volume = ?, taker_sell_volume = ?, cvd = ?
                   WHERE id = ?""",
                updates
            )
            
        conn.commit()
        total_inserted += inserted
        total_updated += updated
        print(f"  ✅ {symbol} -> {pair}: {inserted} inserted, {updated} updated")

    # Verify
    cur.execute("SELECT COUNT(*) FROM oi_funding WHERE exchange = 'Binance'")
    binance_rows = cur.fetchone()[0]

    print(f"\n📊 oi_funding Table Summary (Binance):")
    print(f"   Total Binance rows:  {binance_rows:,}")
    print(f"   New inserts: {total_inserted:,}")
    print(f"   Updated:     {total_updated:,}")
    
    conn.close()
    print(f"\n💾 Done! Data saved to {DB_PATH}")

if __name__ == "__main__":
    main()
