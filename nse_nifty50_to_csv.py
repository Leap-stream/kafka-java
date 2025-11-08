#!/usr/bin/env python3
import requests
import time
import datetime

# -----------------------------------------
# IMPORTANT: NSE SESSION CREATION
# -----------------------------------------
def make_nse_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Referer": "https://www.nseindia.com/"
    })
    # Get cookies
    session.get("https://www.nseindia.com", timeout=10)
    return session

# -----------------------------------------
# SYMBOL SANITIZER FOR NSE
# -----------------------------------------
def sanitize_symbol(sym: str) -> str:
    overrides = {
        "M&M": "M%26M",       # ✅ M&M uses encoded ampersand
        "L&T": "LT",          # ✅ L&T is actually LT
    }
    if sym in overrides:
        return overrides[sym]

    return sym.replace("&", "_").replace(" ", "")

# -----------------------------------------
# FETCH NIFTY50 SYMBOLS
# -----------------------------------------
def get_nifty50_symbols(session):
    url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
    r = session.get(url, timeout=10)
    data = r.json()

    # ✅ Remove the index symbol itself
    symbols = [x['symbol'] for x in data['data'] if x['symbol'] != "NIFTY 50"]
    return symbols

# -----------------------------------------
# FETCH QUOTE FOR SYMBOL
# -----------------------------------------
def fetch_quote(session, display_symbol):
    api_symbol = sanitize_symbol(display_symbol)
    url = f"https://www.nseindia.com/api/quote-equity?symbol={api_symbol}"

    r = session.get(url, timeout=10)
    data = r.json()

    if "priceInfo" not in data:
        return None

    return {
        "symbol": display_symbol,
        "lastPrice": data["priceInfo"]["lastPrice"],
        "change": data["priceInfo"]["change"],
        "pChange": data["priceInfo"]["pChange"]
    }

# -----------------------------------------
# MAIN CAPTURE LOOP
# -----------------------------------------
def main():
    print("✅ Initializing NSE session…")
    session = make_nse_session()

    print("✅ Downloading NIFTY 50 symbols…")
    symbols = get_nifty50_symbols(session)
    print("✅ Final symbols:", symbols)

    print("✅ Starting LIVE capture (every 60 seconds)…")

    while True:
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n⏳ Capturing at {now}")

        for sym in symbols:
            try:
                quote = fetch_quote(session, sym)

                if quote is None:
                    print(f"⚠️ Error fetching {sym}")
                    continue

                print(f"✅ {sym}: {quote['lastPrice']} ({quote['pChange']}%)")

            except Exception as e:
                print(f"⚠️ Exception for {sym}: {e}")

        print("✅ Capture complete")
        time.sleep(60)


if __name__ == "__main__":
    main()
