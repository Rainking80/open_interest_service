import os
import io
import time
import json
import requests
import pandas as pd
import numpy as np
import ccxt
import boto3
import lz4.frame
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
CCXT_SYMBOLS = [
    "AAVE/USDC:USDC", "ADA/USDC:USDC", "AERO/USDC:USDC", "APT/USDC:USDC",
    "ARB/USDC:USDC", "ASTER/USDC:USDC", "ATOM/USDC:USDC", "AVAX/USDC:USDC",
    "AXS/USDC:USDC", "AZTEC/USDC:USDC", "BCH/USDC:USDC", "BERA/USDC:USDC",
    "BNB/USDC:USDC", "BTC/USDC:USDC", "CC/USDC:USDC", "CRV/USDC:USDC",
    "DASH/USDC:USDC", "DOGE/USDC:USDC", "DOT/USDC:USDC", "EIGEN/USDC:USDC",
    "ENA/USDC:USDC", "ETH/USDC:USDC", "ETHFI/USDC:USDC", "FARTCOIN/USDC:USDC",
    "FIL/USDC:USDC", "FTT/USDC:USDC", "HYPE/USDC:USDC", "INIT/USDC:USDC",
    "IP/USDC:USDC", "JUP/USDC:USDC", "KAITO/USDC:USDC", "kBONK/USDC:USDC",
    "kPEPE/USDC:USDC", "kSHIB/USDC:USDC", "LINK/USDC:USDC", "LIT/USDC:USDC",
    "LTC/USDC:USDC", "MET/USDC:USDC", "MON/USDC:USDC", "MOODENG/USDC:USDC",
    "MORPHO/USDC:USDC", "MOVE/USDC:USDC", "NEAR/USDC:USDC", "OM/USDC:USDC",
    "OP/USDC:USDC", "PAXG/USDC:USDC", "PENDLE/USDC:USDC", "PENGU/USDC:USDC",
    "POL/USDC:USDC", "PROMPT/USDC:USDC", "PUMP/USDC:USDC", "RESOLV/USDC:USDC",
    "SEI/USDC:USDC", "SKR/USDC:USDC", "SNX/USDC:USDC", "SOL/USDC:USDC",
    "SPX/USDC:USDC", "STABLE/USDC:USDC", "SUI/USDC:USDC", "TAO/USDC:USDC",
    "TIA/USDC:USDC", "TON/USDC:USDC", "TRUMP/USDC:USDC", "TRX/USDC:USDC",
    "UNI/USDC:USDC", "VIRTUAL/USDC:USDC", "VVV/USDC:USDC", "WIF/USDC:USDC",
    "WLFI/USDC:USDC", "WLD/USDC:USDC", "XLM/USDC:USDC", "XMR/USDC:USDC",
    "XPL/USDC:USDC", "XRP/USDC:USDC", "YGG/USDC:USDC", "ZEC/USDC:USDC",
    "ZRO/USDC:USDC",
]
# Auto-derive: ('ETH', 'ETH/USDC:USDC'), ('kBONK', 'kBONK/USDC:USDC'), etc.
COINS = [(sym.split('/')[0], sym) for sym in CCXT_SYMBOLS]
START_DATE = '2025-09-01'       # YYYY-MM-DD
END_DATE = '2026-03-01'         # YYYY-MM-DD
FINAL_DIR = 'output'

# S3 bucket config
S3_BUCKET = 'hl-mainnet-node-data'
S3_REGION = 'us-east-2'
S3_PREFIX = 'node_fills_by_block/hourly'


def get_feather_path(coin):
    return os.path.join(FINAL_DIR, f'hl_orderflow_{coin}_1h.feather')


def download_and_parse_hourly_fills(s3_client, date_str, hour, coins_set):
    """Downloads and parses a single hourly LZ4 fills file from S3, filtering for multiple coins at once."""
    object_key = f"{S3_PREFIX}/{date_str}/{hour}.lz4"

    try:
        response = s3_client.get_object(
            Bucket=S3_BUCKET,
            Key=object_key,
            RequestPayer='requester'
        )
        compressed_data = response['Body'].read()
        decompressed = lz4.frame.decompress(compressed_data)
        lines = decompressed.decode('utf-8').strip().split('\n')

        # Dict of coin -> list of fills
        fills_by_coin = {coin: [] for coin in coins_set}
        for line in lines:
            if not line.strip():
                continue
            block = json.loads(line)
            for event in block.get('events', []):
                # Each event is [address, fill_dict]
                if isinstance(event, list) and len(event) == 2:
                    fill = event[1]
                    coin = fill.get('coin')
                    if coin in coins_set:
                        fills_by_coin[coin].append({
                            'time': fill['time'],
                            'sz': float(fill['sz']),
                            'px': float(fill['px']),
                            'side': fill['side'],
                            'crossed': fill.get('crossed', False),
                        })
        return fills_by_coin

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404' or error_code == 'NoSuchKey':
            return {coin: [] for coin in coins_set}
        else:
            print(f"   ⚠️ S3 Error for {object_key}: {e}")
            return {coin: [] for coin in coins_set}
    except Exception as e:
        print(f"   ❌ Error processing {object_key}: {e}")
        return {coin: [] for coin in coins_set}


def download_daily_fills(s3_client, date_str, coins_set):
    """Downloads all 24 hourly fill files for a given date, returns fills for all coins."""
    combined = {coin: [] for coin in coins_set}
    for hour in range(24):
        fills_by_coin = download_and_parse_hourly_fills(s3_client, date_str, hour, coins_set)
        for coin in coins_set:
            combined[coin].extend(fills_by_coin[coin])
    return combined


def process_fills_to_hourly(all_fills):
    """Converts raw fill records into hourly taker buy/sell volume DataFrame."""
    if not all_fills:
        return pd.DataFrame()

    df = pd.DataFrame(all_fills)
    df['datetime'] = pd.to_datetime(df['time'], unit='ms', utc=True)
    df['side'] = df['side'].astype(str).str.upper()

    # Only keep taker (aggressor) fills — crossed=True means the order crossed the spread.
    # Without this filter, every trade is counted twice (once per side), giving a fake 50/50 split.
    df = df[df['crossed'] == True]

    # B = Taker Buy, A = Taker Sell
    df['taker_buy_volume'] = np.where(df['side'] == 'B', df['sz'], 0.0)
    df['taker_sell_volume'] = np.where(df['side'] == 'A', df['sz'], 0.0)

    hourly = df.set_index('datetime')[['taker_buy_volume', 'taker_sell_volume']].resample('1h').sum()
    return hourly


def fetch_api_data(symbol, start_dt, end_dt):
    """Fetches Funding Rates via CCXT."""
    exchange = ccxt.hyperliquid({'enableRateLimit': True})
    start_ts = int(start_dt.timestamp() * 1000)
    end_ts = int(end_dt.timestamp() * 1000)

    print(f"   ⬇️ Fetching Funding Rates for {symbol}...")
    funding_records = []
    curr_ts = start_ts
    while curr_ts < end_ts:
        try:
            batch = exchange.fetch_funding_rate_history(symbol, since=curr_ts, limit=500)
            if not batch: break
            funding_records.extend(batch)
            curr_ts = batch[-1]['timestamp'] + 1
            time.sleep(0.2)
        except Exception as e:
            print(f"   ⚠️ Error fetching funding: {e}")
            break

    funding_df = pd.DataFrame(funding_records)
    if not funding_df.empty:
        funding_df['datetime'] = pd.to_datetime(funding_df['timestamp'], unit='ms', utc=True)
        funding_df = funding_df.set_index('datetime')['fundingRate'].resample('1h').last().to_frame('funding_rate')
    else:
        funding_df = pd.DataFrame(columns=['funding_rate'])

    return funding_df


def load_existing_data(filepath):
    """Load existing feather file if it exists, returning DataFrame and set of dates already fetched."""
    if os.path.exists(filepath):
        df = pd.read_feather(filepath)
        df['date'] = pd.to_datetime(df['date'], utc=True)
        df = df.set_index('date')
        existing_dates = set(df.index.normalize().unique().strftime('%Y%m%d'))
        return df, existing_dates
    return pd.DataFrame(), set()


def save_incremental(master_df, filepath):
    """Save the current master DataFrame to feather."""
    master_df.reset_index().to_feather(filepath)


def main():
    os.makedirs(FINAL_DIR, exist_ok=True)

    start_dt = pd.to_datetime(START_DATE, utc=True)
    end_dt = pd.to_datetime(END_DATE, utc=True)
    date_list = pd.date_range(start_dt, end_dt, freq='D')

    coin_names = [c[0] for c in COINS]
    coins_set = set(coin_names)

    print("🚀 Starting Hyperliquid Orderflow Extraction Pipeline\n")
    print(f"   Source: s3://{S3_BUCKET}/{S3_PREFIX}/")
    print(f"   Coins: {', '.join(coin_names)}")
    print(f"   Date Range: {START_DATE} → {END_DATE}\n")

    # Load existing progress for each coin
    master_dfs = {}
    existing_dates_per_coin = {}
    for coin in coin_names:
        fpath = get_feather_path(coin)
        df, dates = load_existing_data(fpath)
        master_dfs[coin] = df
        existing_dates_per_coin[coin] = dates
        if dates:
            print(f"   📂 {coin}: {len(df)} rows, {len(dates)} days already fetched.")

    # Initialize S3 client
    s3_client = boto3.client('s3', region_name=S3_REGION)

    # 1. Download and Process S3 Fills (day by day, saving after each)
    new_days = 0
    for dt in date_list:
        date_str = dt.strftime('%Y%m%d')

        # Check which coins still need this date
        coins_needed = {c for c in coin_names if date_str not in existing_dates_per_coin[c]}
        if not coins_needed:
            print(f"⏭️  Skipping {date_str} (all coins already fetched)")
            continue

        print(f"\n⬇️ Downloading fills for {date_str} (24 hourly files)...")
        fills_by_coin = download_daily_fills(s3_client, date_str, coins_needed)

        for coin in coins_needed:
            fills = fills_by_coin[coin]
            if fills:
                df_hourly = process_fills_to_hourly(fills)
                if not df_hourly.empty:
                    master_dfs[coin] = pd.concat([master_dfs[coin], df_hourly])
                    master_dfs[coin] = master_dfs[coin][~master_dfs[coin].index.duplicated(keep='last')]
                    master_dfs[coin] = master_dfs[coin].sort_index()
                    master_dfs[coin].index.name = 'date'
                    save_incremental(master_dfs[coin], get_feather_path(coin))
                    print(f"   ✅ {coin}: {len(fills):,} fills → {len(df_hourly)} bars 💾")
            else:
                print(f"   ⚠️ {coin}: No fills for {date_str}")

        new_days += 1

    print(f"\n📥 S3 download complete. {new_days} new day(s) fetched.")

    # 2. Fetch Funding Rates & Final Merge per coin
    print("\n🔗 Fetching Funding Rates & Final Merge...")
    full_timeline = pd.date_range(start=start_dt, end=end_dt + timedelta(hours=23), freq='1h', tz='UTC')

    for coin_s3, coin_ccxt in COINS:
        if master_dfs[coin_s3].empty:
            print(f"   ⚠️ {coin_s3}: No data, skipping.")
            continue

        funding_df = fetch_api_data(coin_ccxt, start_dt, end_dt + timedelta(days=1))

        master_df = master_dfs[coin_s3].reindex(full_timeline)
        master_df = master_df.join(funding_df, how='left')

        master_df['taker_buy_volume'] = master_df['taker_buy_volume'].fillna(0.0)
        master_df['taker_sell_volume'] = master_df['taker_sell_volume'].fillna(0.0)
        master_df['funding_rate'] = master_df['funding_rate'].ffill().fillna(0.0)
        master_df.index.name = 'date'

        # Completeness Check
        expected = len(full_timeline)
        actual = len(master_df)
        zero_vol = len(master_df[(master_df['taker_buy_volume'] == 0) & (master_df['taker_sell_volume'] == 0)])
        status = "✅" if expected == actual else "⚠️"
        print(f"   {status} {coin_s3}: {actual}/{expected} rows, {zero_vol} zero-volume hours")

        fpath = get_feather_path(coin_s3)
        master_df.reset_index().to_feather(fpath)
        print(f"   💾 {fpath}")

    print("\n✨ Pipeline Completed Successfully!")


if __name__ == "__main__":
    main()