#!/usr/bin/env python3
import requests
import csv
import time
import datetime
from urllib.parse import quote
import os

CSV_PATH = "/mnt/c/Users/arbaz/Desktop/nifty50_prices.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

# ---------- NSE Session ----------
def create_session():
    s = requests.Session()
    s.get("https://www.nseindia.com", headers=HEADERS)
    return s

# ---------- Get NIFTY50 List ----------
def get_nifty50_symbols(s):
    url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
    r = s.get(url, headers=HEADERS)
    data = r.json()
    symbols = [item["symbol"] for item in data["data"]]
    # remove index label if present
    symbols = [sym for sym in symbols if sym.upper() != "NIFTY 50"]
    return symbols

# ---------- extract volume robustly ----------
def extract_volume(data):
    try:
        return data["marketDeptOrderBook"]["tradeInfo"]["totalTradedVolume"]
    except:
        pass
    try:
        return data["priceInfo"]["totalTradedVolume"]
    except:
        pass
    try:
        return data["securityInfo"]["totalTradedVolume"]
    except:
        pass
    try:
        return data.get("preOpenMarket", {}).get("totalTradedVolume", 0)
    except:
        return 0

# ---------- Fetch Live Stock Data ----------
def fetch_stock_data(s, symbol):
    encoded = quote(symbol)
    url = f"https://www.nseindia.com/api/quote-equity?symbol={encoded}"
    r = s.get(url, headers=HEADERS, timeout=10)
    r.raise_for_status()
    data = r.json()

    priceInfo = data.get("priceInfo", {})
    last_price = priceInfo.get("lastPrice")
    change = priceInfo.get("change")
    pchange = priceInfo.get("pChange")
    volume = extract_volume(data)
    return last_price, change, pchange, volume

# ---------- Ensure header (with BOM for Excel) ----------
def ensure_csv_header():
    header = ["timestamp", "symbol", "lastPrice", "change", "pChange", "volume"]

    # If file doesn't exist or is empty -> create with BOM so Excel shows UTF-8 header
    if not os.path.exists(CSV_PATH) or os.path.getsize(CSV_PATH) == 0:
        # use utf-8-sig to write BOM
        with open(CSV_PATH, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(header)
        print(f"✅ Created CSV with header at {CSV_PATH} (utf-8 BOM written).")
    else:
        # check first line; if it's not header, prepend header safely
        with open(CSV_PATH, "r", encoding="utf-8") as f:
            first = f.readline().strip()
        # basic check: header should contain 'timestamp' and 'symbol'
        if "timestamp" not in first.lower() or "symbol" not in first.lower():
            # read existing content, then rewrite with header + old content
            with open(CSV_PATH, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
            with open(CSV_PATH, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.writer(f)
                writer.writerow(header)
                f.write(content)
            print("✅ Header was missing — prepended header with utf-8 BOM.")
        else:
            print("✅ CSV already has header.")

    # Print first line for verification
    with open(CSV_PATH, "r", encoding="utf-8", errors="ignore") as f:
        print("First line in CSV:", f.readline().strip())

# ---------- MAIN ----------
def main():
    print("✅ Initializing session...")
    s = create_session()

    print("✅ Fetching NIFTY50 symbols...")
    symbols = get_nifty50_symbols(s)
    print("✅ Symbols count:", len(symbols))

    ensure_csv_header()

    print("✅ Starting capture (every 60s). Stop with Ctrl+C.\n")
    while True:
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows = []
        for sym in symbols:
            try:
                last_price, change, pchange, volume = fetch_stock_data(s, sym)
                rows.append([ts, sym, last_price, change, pchange, volume])
            except Exception as e:
                print(f"⚠ Error fetching {sym}: {e}")
        # append rows using utf-8 (do not write BOM again)
        with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(rows)
        print(f"✅ {len(rows)} rows appended at {ts}")
        time.sleep(60)

if __name__ == "__main__":
    main()
