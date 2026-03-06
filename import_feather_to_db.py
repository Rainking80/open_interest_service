"""Import all feather files from output/ into the oi_funding table in data/open_interest.db."""
import os
import glob
import sqlite3
import pandas as pd

DB_PATH = 'data/open_interest.db'
FEATHER_DIR = 'output'

def main():
    feathers = sorted(glob.glob(os.path.join(FEATHER_DIR, 'hl_orderflow_*_1h.feather')))
    if not feathers:
        print("❌ No feather files found in output/")
        return

    print(f"📂 Found {len(feathers)} feather files\n")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Ensure the taker volume & cvd columns exist on oi_funding
    for col, coltype in [("taker_buy_volume", "FLOAT"), ("taker_sell_volume", "FLOAT"), ("cvd", "FLOAT")]:
        try:
            cur.execute(f"ALTER TABLE oi_funding ADD COLUMN {col} {coltype}")
        except Exception:
            pass  # Column already exists
    conn.commit()

    total_inserted = 0
    total_updated = 0
    total_skipped = 0

    for fpath in feathers:
        # Extract coin name from filename: hl_orderflow_ETH_1h.feather -> ETH
        fname = os.path.basename(fpath)
        coin = fname.replace('hl_orderflow_', '').replace('_1h.feather', '')
        pair = f"{coin}/USDC:USDC"

        df = pd.read_feather(fpath)
        df['date'] = pd.to_datetime(df['date'], utc=True)
        df = df.sort_values('date')

        inserted = 0
        updated = 0
        skipped = 0

        for _, row in df.iterrows():
            ts = row['date'].strftime('%Y-%m-%d %H:%M:%S.000000')
            tbv = row['taker_buy_volume']
            tsv = row['taker_sell_volume']
            fr = row['funding_rate']
            cvd = tbv - tsv

            # Check if row already exists
            cur.execute(
                "SELECT id, taker_buy_volume FROM oi_funding WHERE pair = ? AND timestamp = ?",
                (pair, ts)
            )
            existing = cur.fetchone()

            if existing:
                # Row exists — update only if taker volumes are missing (NULL or 0)
                if existing[1] is None or existing[1] == 0.0:
                    cur.execute(
                        """UPDATE oi_funding 
                           SET taker_buy_volume = ?, taker_sell_volume = ?, cvd = ?, funding_rate = COALESCE(funding_rate, ?)
                           WHERE id = ?""",
                        (tbv, tsv, cvd, fr, existing[0])
                    )
                    updated += 1
                else:
                    skipped += 1
            else:
                # Insert new row with OI=0 (S3 data doesn't have OI)
                cur.execute(
                    """INSERT INTO oi_funding 
                       (pair, timestamp, open_interest, funding_rate, taker_buy_volume, taker_sell_volume, cvd)
                       VALUES (?, ?, 0.0, ?, ?, ?, ?)""",
                    (pair, ts, fr, tbv, tsv, cvd)
                )
                inserted += 1

        conn.commit()
        total_inserted += inserted
        total_updated += updated
        total_skipped += skipped
        print(f"  ✅ {pair}: {inserted} inserted, {updated} updated, {skipped} skipped")

    # Verify
    cur.execute("SELECT COUNT(*) FROM oi_funding")
    total_rows = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT pair) FROM oi_funding")
    total_pairs = cur.fetchone()[0]
    cur.execute("SELECT MIN(timestamp), MAX(timestamp) FROM oi_funding")
    min_ts, max_ts = cur.fetchone()

    print(f"\n📊 oi_funding Table Summary:")
    print(f"   Total rows:  {total_rows:,}")
    print(f"   Total pairs: {total_pairs}")
    print(f"   Date range:  {min_ts} → {max_ts}")
    print(f"   New inserts: {total_inserted:,}")
    print(f"   Updated:     {total_updated:,}")
    print(f"   Skipped:     {total_skipped:,}")

    conn.close()
    print(f"\n💾 Done! Data saved to {DB_PATH}")

if __name__ == "__main__":
    main()
