#!/usr/bin/env python3
import os, json, time, requests
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm

API_KEY       = os.getenv("COINALYZE_API_KEY")
BASE_URL      = "https://api.coinalyze.net/v1"
HEADERS       = {"api_key": API_KEY}
DATA_FILE     = "data/perps_volume_data.json"
DELAY         = 1.5
TARGET_TOKENS = {"BTC","ETH","BNB","SOL","HYPE"}
START_DATE    = datetime(2024,6,1)

def get_daily_ohlcv(symbol, from_ts, to_ts):
    r = requests.get(f"{BASE_URL}/ohlcv-history",
                     headers=HEADERS,
                     params={"symbols":symbol,"interval":"daily","from":from_ts,"to":to_ts})
    if r.status_code==429:
        wait = float(r.headers.get("Retry-After","5"))
        time.sleep(wait)
        return get_daily_ohlcv(symbol,from_ts,to_ts)
    r.raise_for_status()
    data = r.json()
    return data[0].get("history",[]) if data else []

def fetch_perp_markets():
    mkts = requests.get(f"{BASE_URL}/future-markets", headers=HEADERS).json()
    exs  = requests.get(f"{BASE_URL}/exchanges",     headers=HEADERS).json()
    exmap= {e["code"]:e["name"] for e in exs}
    out=[]
    for m in mkts:
        if m.get("is_perpetual") and m.get("base_asset") in TARGET_TOKENS:
            out.append({
                "exchange":   exmap.get(m["exchange"],m["exchange"]),
                "symbol":     m["symbol"],
                "base_asset": m["base_asset"]
            })
    return pd.DataFrame(out)

def load_existing_dates():
    if not os.path.exists(DATA_FILE):
        return []
    with open(DATA_FILE) as f:
        arr = json.load(f)
    return sorted(set(row["Date"] for row in arr))

def append_rows(rows):
    # ensure data dir
    os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
    lines = [json.dumps(r, separators=(",",":"), ensure_ascii=False) for r in rows]
    if not os.path.exists(DATA_FILE):
        # first-time create
        with open(DATA_FILE,"w") as f:
            f.write("[\n")
            f.write(",\n".join(lines))
            f.write("\n]")
    else:
        # preserve existing exactly, then append
        with open(DATA_FILE,"r") as f:
            text = f.read().rstrip()
        # strip final ']'
        assert text.endswith("]"), "Malformed JSON, missing closing ]"
        prefix = text[:-1].rstrip()
        # append new lines
        new_text = prefix + ",\n" + "\n".join(lines) + "\n]"
        with open(DATA_FILE,"w") as f:
            f.write(new_text)

def main():
    today = datetime.utcnow().replace(hour=0,minute=0,second=0,microsecond=0)
    last_existing = load_existing_dates()
    if last_existing:
        # missing dates are those from (first or last?) — we'll backfill holes later
        existing_set = set(last_existing)
    else:
        existing_set = set()

    # build full calendar from START_DATE to yesterday
    end_date = today - timedelta(days=1)
    days = (end_date - START_DATE).days + 1
    all_dates = [(START_DATE+timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days)]

    missing_dates = [d for d in all_dates if d not in existing_set]
    if not missing_dates:
        print("✅ No missing dates. Up to date.")
        return

    df = fetch_perp_markets()
    new_rows = []
    for date_str in tqdm(missing_dates, desc="Backfilling dates"):
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        from_ts = int(dt.timestamp())
        to_ts   = int((dt+timedelta(days=1)).timestamp())

        # aggregate per exchange
        daily = {}
        for _,row in df.iterrows():
            exch = row["exchange"]
            sym  = row["symbol"]
            tok  = row["base_asset"]
            try:
                candles = get_daily_ohlcv(sym, from_ts, to_ts)
            except Exception:
                continue
            for c in candles:
                if not all(k in c for k in ("v","c")):
                    continue
                vol = c["v"]*c["c"]
                daily.setdefault(exch,{t:0 for t in TARGET_TOKENS})
                daily[exch][tok] += vol
            time.sleep(DELAY)

        for exch,tm in daily.items():
            entry = {"Date":date_str,"Exchange":exch}
            # fill tokens
            for t in TARGET_TOKENS:
                entry[t] = round(tm.get(t,0),2)
            entry["Total"] = round(sum(tm.values()),2)
            new_rows.append(entry)

    append_rows(new_rows)
    print(f"✅ Appended {len(new_rows)} rows for dates: {missing_dates}")

if __name__=="__main__":
    main()
