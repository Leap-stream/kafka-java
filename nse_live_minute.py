import csv
import time
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter, Retry

BASE = "https://www.nseindia.com"

HEADERS = {
    "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.nseindia.com",
}


def make_session():
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update(HEADERS)

    s.get(BASE)
    s.get(BASE + "/market-data/pre-open-market")
    return s


def fetch_all_prices(s):
    url = BASE + "/api/market-data-pre-open?key=ALL"
    r = s.get(url, timeout=20)
    r.raise_for_status()
    return r.json()["data"]


if __name__ == "__main__":
    session = make_session()

    output = "/mnt/c/Users/Arbaz/Desktop/nse_live_prices.csv"
    print("Saving live data to:", output)

    # Create CSV with header if not exists
    try:
        with open(output, "x", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "timestamp",
                "symbol",
                "lastPrice",
                "change",
                "pChange",
                "volume"
            ])
    except FileExistsError:
        pass  # file already exists

    print("✅ Starting live capture… (Ctrl + C to stop)\n")

    while True:
        try:
            stocks = fetch_all_prices(session)
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            with open(output, "a", newline="") as f:
                writer = csv.writer(f)

                for s in stocks:
                    meta = s.get("metadata", {})
                    detail = s.get("detail", {})
                    pre = detail.get("preOpenMarket", {})

                    writer.writerow([
                        ts,
                        meta.get("symbol"),
                        meta.get("lastPrice"),
                        meta.get("change"),
                        meta.get("pChange"),
                        pre.get("totalTradedVolume")
                    ])

            print(f"✅ Captured {len(stocks)} stocks at {ts}")

            time.sleep(60)  # wait 1 minute

        except KeyboardInterrupt:
            print("\n⛔ Stopped by user.")
            break

        except Exception as e:
            print("⚠ Error:", e)
            time.sleep(10)
