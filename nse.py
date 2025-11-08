import csv
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

    # Initialize cookies
    s.get(BASE, timeout=10)
    s.get(BASE + "/market-data/pre-open-market", timeout=10)

    return s


def get_all_stocks(s):
    print("Downloading data from NSE...")
    url = BASE + "/api/market-data-pre-open?key=ALL"
    r = s.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()
    return data["data"]


if __name__ == "__main__":
    session = make_session()
    stocks = get_all_stocks(session)

    print("Total stocks:", len(stocks))

    output = "/mnt/c/Users/Arbaz/Desktop/all_nse_stocks.csv"
    print("Saving to:", output)

    with open(output, "w", newline="") as f:
        writer = csv.writer(f)

        writer.writerow([
            "symbol",
            "lastPrice",
            "change",
            "pChange",
            "yearHigh",
            "yearLow",
            "marketCap",
            "totalTradedVolume",
            "finalPrice",
            "previousClose",
        ])

        for s in stocks:
            meta = s.get("metadata", {})
            detail = s.get("detail", {})
            pre = detail.get("preOpenMarket", {})

            writer.writerow([
                meta.get("symbol"),
                meta.get("lastPrice"),
                meta.get("change"),
                meta.get("pChange"),
                meta.get("yearHigh"),
                meta.get("yearLow"),
                meta.get("marketCap"),
                pre.get("totalTradedVolume"),
                pre.get("finalPrice"),
                meta.get("previousClose"),
            ])

    print("✅ DONE — File saved on Desktop!")
