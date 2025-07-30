import requests
import pandas as pd
import time
import os
import json
from datetime import datetime, timedelta
from tqdm import tqdm

API_KEY = os.getenv("COINALYZE_API_KEY")
BASE_URL = "https://api.coinalyze.net/v1"
HEADERS = {"api_key": API_KEY}
DATA_FILE = "data/perps_volume_data.json"
DELAY = 1.5
TARGET_TOKENS = {"BTC", "ETH", "BNB", "SOL", "HYPE"}
START_DATE = datetime(2024, 6, 1)

def get_daily_ohlcv(symbol, from_ts, to_ts):
    params = {
        "symbols": symbol,
        "interval": "daily",
        "from": from_ts,
        "to": to_ts
    }
    res = requests.get(f"{BASE_URL}/ohlcv-history", headers=HEADERS, params=params)
    if res.status_code == 429:
        retry = int(res.headers.get("Retry-After", 5))
        time.sleep(retry)
        return get_daily_ohlcv(symbol, from_ts, to_ts)
    res.raise_for_status()
    return res.json()[0]["history"] if res.json() else []

def load_existing_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            return json.load(f)
    return []

def get_existing_dates(data):
    return set((row["Date"], row["Exchange"]) for row in data)

def fetch_perp_markets():
    markets = requests.get(f"{BASE_URL}/future-markets", headers=HEADERS).json()
    exchanges = requests.get(f"{BASE_URL}/exchanges", headers=HEADERS).json()
    ex_map = {e["code"]: e["name"] for e in exchanges}

    filtered = []
    for m in markets:
        if m["is_perpetual"] and m["base_asset"] in TARGET_TOKENS:
            filtered.append({
                "exchange_code": m["exchange"],
                "exchange_name": ex_map.get(m["exchange"], m["exchange"]),
                "symbol": m["symbol"],
                "base_asset": m["base_asset"]
            })
    return filtered

def main():
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    existing = load_existing_data()
    existing_keys = get_existing_dates(existing)

    markets = fetch_perp_markets()
    df = pd.DataFrame(markets)

    output = []

    days = (today - START_DATE).days
    for delta in range(days):
        date = START_DATE + timedelta(days=delta)
        date_str = date.strftime("%Y-%m-%d")
        from_ts = int(date.timestamp())
        to_ts = int((date + timedelta(days=1)).timestamp()) - 1

        day_exchange_map = {}  # {(exchange): {token: volume}}

        for _, row in tqdm(df.iterrows(), total=len(df), desc=f"Fetching {date_str}"):
            symbol = row["symbol"]
            token = row["base_asset"]
            exchange = row["exchange_name"]

            if (date_str, exchange) in existing_keys:
                continue

            try:
                candles = get_daily_ohlcv(symbol, from_ts, to_ts)
            except Exception as e:
                print(f"Error {symbol}: {e}")
                continue

            for c in candles:
                if "t" not in c or "v" not in c or "c" not in c:
                    continue
                usd_volume = c["v"] * c["c"]
                if exchange not in day_exchange_map:
                    day_exchange_map[exchange] = {t: 0 for t in TARGET_TOKENS}
                day_exchange_map[exchange][token] += usd_volume
            time.sleep(DELAY)

        for exchange, token_map in day_exchange_map.items():
            row = {
                "Date": date_str,
                "Exchange": exchange,
                **token_map,
                "Total": sum(token_map.values())
            }
            output.append(row)

    # Write to file
    combined = existing + output
    os.makedirs("data", exist_ok=True)
    with open(DATA_FILE, "w") as f:
        json.dump(combined, f, indent=2)
    print(f"✅ Updated {DATA_FILE} with {len(output)} new rows.")

if __name__ == "__main__":
    main()
