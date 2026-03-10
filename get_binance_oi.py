import pandas as pd
import requests
from io import BytesIO
from zipfile import ZipFile
from datetime import datetime, timedelta

def fetch_historical_funding_rates(symbol, start_date, end_date):
    import time
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
    
    start_ts = int(start.timestamp() * 1000)
    end_ts = int(end.timestamp() * 1000)
    
    all_rates = []
    limit = 1000
    
    print(f"\nStarte Funding Rates Download für {symbol} über REST API...")
    current_start = start_ts
    while current_start < end_ts:
        url = f"https://fapi.binance.com/fapi/v1/fundingRate?symbol={symbol}&startTime={current_start}&endTime={end_ts}&limit={limit}"
        
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if not data:
                    break
                
                for item in data:
                    all_rates.append({
                        'fundingTime': item['fundingTime'],
                        'fundingRate': float(item['fundingRate']),
                        'markPrice': float(item['markPrice'])
                    })
                
                current_start = data[-1]['fundingTime'] + 1
                time.sleep(0.5)
            elif response.status_code == 429:
                print("Rate limit erreicht. Warte 5 Sekunden...")
                time.sleep(5)
            else:
                print(f" Fehler beim Abrufen der Funding Rates: HTTP {response.status_code}")
                break
        except requests.exceptions.RequestException as e:
            print(f"Netzwerk/API Fehler: {e}. Warte 5 Sekunden...")
            time.sleep(5)
            
    if all_rates:
        df_f = pd.DataFrame(all_rates)
        df_f['datetime'] = pd.to_datetime(df_f['fundingTime'], unit='ms')
        df_f.drop(columns=['fundingTime'], inplace=True)
        return df_f
    return pd.DataFrame()

def download_binance_oi(symbol, start_date, end_date):
    """
    Lädt historische Open Interest (Metrics) Daten von data.binance.vision herunter
    und fügt sie zu einer sauberen CSV-Datei zusammen.
    """
    # Die Basis-URL für das USD-M Futures Metrics Verzeichnis
    base_url = "https://data.binance.vision/data/futures/um/daily/metrics"
    
    # Strings in Datetime-Objekte umwandeln
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    all_data = []
    
    print(f"Starte Download für {symbol} von {start_date} bis {end_date}...\n")
    
    current_date = start
    while current_date <= end:
        date_str = current_date.strftime("%Y-%m-%d")
        file_name = f"{symbol}-metrics-{date_str}.zip"
        url = f"{base_url}/{symbol}/{file_name}"
        
        print(f"Rufe Daten für {date_str} ab...")
        
        response = requests.get(url)
        
        # 200 bedeutet, dass die Datei auf dem Binance-Server existiert
        if response.status_code == 200:
            # Zip-Datei direkt im Arbeitsspeicher öffnen (kein lokales Speichern der Zips nötig)
            with ZipFile(BytesIO(response.content)) as z:
                # Normalerweise enthält die Zip genau eine CSV-Datei mit demselben Namen
                csv_filename = z.namelist()[0]
                with z.open(csv_filename) as f:
                    df = pd.read_csv(f)
                    
                    # Binance nutzt manchmal 'create_time', manchmal 'timestamp'
                    time_col = 'create_time' if 'create_time' in df.columns else 'timestamp'
                    
                    # Extrahiere nur die Spalten, die wir wirklich brauchen
                    if 'sum_open_interest' in df.columns:
                        df_filtered = df[[time_col, 'sum_open_interest', 'sum_open_interest_value']].copy()
                        all_data.append(df_filtered)
                    else:
                        print(f"  -> Warnung: Keine Open Interest Spalte in {date_str} gefunden.")
        else:
            print(f"  -> Übersprungen: Keine Daten auf Binance für {date_str} gefunden (HTTP {response.status_code}).")
            
        current_date += timedelta(days=1)
        
    # Wenn Daten gefunden wurden, alles zusammenführen
    if all_data:
        master_df = pd.concat(all_data, ignore_index=True)
        
        # Die Zeitstempel von Binance sind oft in Millisekunden oder als lesbarer String formatiert.
        # Wir versuchen, sie für die CSV in ein sauberes, universelles Datetime-Format zu konvertieren.
        try:
            # Falls es sich um einen UNIX-Timestamp in Millisekunden handelt
            master_df['datetime'] = pd.to_datetime(master_df[time_col], unit='ms')
            master_df.drop(columns=[time_col], inplace=True)
        except Exception:
            # Falls es bereits ein String-Datum ist, belassen oder umbenennen wir es
            master_df.rename(columns={time_col: 'datetime'}, inplace=True)
            pass
        
        # Chronologisch sortieren
        master_df.sort_values(by='datetime', inplace=True)
        
        # JETZT: Klines für das gleiche Symbol und Datum herunterladen
        print(f"\nStarte Klines Download für {symbol} (1h) von {start_date} bis {end_date}...\n")
        kline_base_url = "https://data.binance.vision/data/futures/um/daily/klines"
        all_klines = []
        current_date = start
        while current_date <= end:
            date_str = current_date.strftime("%Y-%m-%d")
            file_name = f"{symbol}-1h-{date_str}.zip"
            url = f"{kline_base_url}/{symbol}/1h/{file_name}"
            
            print(f"Rufe Klines für {date_str} ab...")
            response = requests.get(url)
            
            if response.status_code == 200:
                with ZipFile(BytesIO(response.content)) as z:
                    csv_filename = z.namelist()[0]
                    with z.open(csv_filename) as f:
                        # Klines csv hat manchmal Header, manchmal nicht!
                        df_k = pd.read_csv(f, header=None)
                        
                        if str(df_k.iloc[0, 0]) == 'open_time':
                            # Es gab einen Header in der ersten Zeile
                            df_k.columns = df_k.iloc[0].values
                            df_k = df_k[1:].reset_index(drop=True)
                        else:
                            # Kein Header vorhanden
                            kline_cols = [
                                'open_time', 'open', 'high', 'low', 'close', 'volume',
                                'close_time', 'quote_asset_volume', 'number_of_trades',
                                'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
                            ]
                            df_k.columns = kline_cols
                            
                        # Spalten können leicht unterschiedlich heißen (taker_buy_volume vs taker_buy_base_asset_volume)
                        taker_buy_col = 'taker_buy_volume' if 'taker_buy_volume' in df_k.columns else 'taker_buy_base_asset_volume'
                        
                        # Datentypen für die Berechnungen sicherstellen
                        df_k['open_time'] = pd.to_numeric(df_k['open_time'])
                        df_k['volume'] = pd.to_numeric(df_k['volume'])
                        df_k[taker_buy_col] = pd.to_numeric(df_k[taker_buy_col])
                        
                        df_k['datetime'] = pd.to_datetime(df_k['open_time'], unit='ms')
                        df_k_filtered = df_k[['datetime', 'volume', taker_buy_col]].copy()
                        
                        # Berechne Taker Sell = Total Volume - Taker Buy
                        df_k_filtered['taker_sell_base_asset_volume'] = df_k_filtered['volume'] - df_k_filtered[taker_buy_col]
                        # Umbenennen für Einheitlichkeit
                        df_k_filtered.rename(columns={taker_buy_col: 'taker_buy_base_asset_volume'}, inplace=True)
                        
                        all_klines.append(df_k_filtered)
            else:
                print(f"  -> Übersprungen: Keine Klines auf Binance für {date_str} gefunden.")
                
            current_date += timedelta(days=1)
            
        missing_data = []
        if all_klines:
            klines_df = pd.concat(all_klines, ignore_index=True)
            # Mergen anhand des Datums/Uhrzeit
            master_df['datetime'] = pd.to_datetime(master_df['datetime'])
            master_df['minute'] = master_df['datetime'].dt.minute
            master_df_hourly = master_df[master_df['minute'] == 0].drop(columns=['minute']).copy()
            
            final_df = pd.merge(master_df_hourly, klines_df, on='datetime', how='inner')
        else:
            print("\nWarnung: Keine Klines gefunden. Speichere nur Metrics.")
            final_df = master_df
            missing_data.append("Klines")
            
        # JETZT: Funding Rates über REST API abrufen und dazumergen
        df_funding = fetch_historical_funding_rates(symbol, start_date, end_date)
        if not df_funding.empty:
            final_df = pd.merge(final_df, df_funding, on='datetime', how='left')
            # Für die ML-Modellierung (Random Forest) verteilen wir die letzte bekannte
            # Funding-Rate über die nächsten 8 Stunden durch `.ffill()`. 
            # Die allerersten Zeilen ohne vorherige Funding-Rate setzen wir sicherheitshalber auf 0.
            final_df['fundingRate'] = final_df['fundingRate'].ffill().fillna(0.0)
            
            # Für Mark Price nutzen wir ebenfalls Forward-Fill (ffill),
            # da wir sonst nur alle 8 Stunden einen Wert von der Funding-Tabelle erhalten.
            final_df['markPrice'] = final_df['markPrice'].ffill()
        else:
            final_df['fundingRate'] = 0.0
            final_df['markPrice'] = 0.0
            missing_data.append("Funding Rates")

        # Check for timeline gaps
        expected_hours = (end - start).days * 24 + 24
        if len(final_df) < expected_hours * 0.9:
            missing_data.append(f"Significant missing timeline data (Expected ~{expected_hours} hours, got {len(final_df)} rows)")

        # Das finale Ergebnis als CSV speichern
        import os
        output_dir = "binance_output"
        os.makedirs(output_dir, exist_ok=True)
        
        output_filename = os.path.join(output_dir, f"{symbol}_Historical_Data_{start_date}_to_{end_date}.csv")
        
        final_df.to_csv(output_filename, index=False)
        print(f"\nErfolg! {len(final_df)} zusammengeführte Zeilen (1-Stunden-Snapshots) wurden in {output_filename} gespeichert.")
        return missing_data
    else:
        print("\nFehler: Es konnten keine Daten heruntergeladen werden. Bitte überprüfe den Ticker und das Datum.")
        return ["Metrics (Open Interest)"]

# --- Hier definierst du, was du brauchst ---
import time

PAIRS_CONFIG = [
    "AAVE/USDC:USDC", "ARB/USDC:USDC", "AVAX/USDC:USDC", "BTC/USDC:USDC",
    "DOGE/USDC:USDC", "ETH/USDC:USDC", "ETHFI/USDC:USDC", "FIL/USDC:USDC",
    "LINK/USDC:USDC", "NEAR/USDC:USDC", "SOL/USDC:USDC",
    "SUI/USDC:USDC", "WLD/USDC:USDC"
]

def parse_pair(p):
    return p.split('/')[0]

start_date = "2024-01-01"
end_date = "2026-03-08"

missing_data_summary = {}

for pair in PAIRS_CONFIG:
    coin = parse_pair(pair)
    symbol = f"{coin}USDT"
    missing = download_binance_oi(symbol=symbol, start_date=start_date, end_date=end_date)
    if missing:
        missing_data_summary[pair] = missing
    time.sleep(1)  # Kleine Pause um Rate Limits zu vermeiden

if missing_data_summary:
    print("\n" + "="*80)
    print("WARNING: Major data was missing for the following pairs:")
    print("="*80)
    for pair, missing in missing_data_summary.items():
        print(f"  - {pair}: {', '.join(missing)}")
    print("="*80 + "\n")
else:
    print("\n" + "="*80)
    print("SUCCESS: All major data was successfully downloaded for all pairs.")
    print("="*80 + "\n")