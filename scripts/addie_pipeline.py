"""
Addie — Data Pipeline v1
=========================
Pulls a categorized universe of stocks/ETFs (via yfinance) and crypto (via
CoinGecko's free public API — no key required): price, day change, 52-week
range, a 90-day sparkline series, and real daily OHLC history for candlestick
charting (5y for equities, 365d for crypto — CoinGecko's free-tier OHLC cap —
plus an extended close-only series for crypto beyond that window).

Five of these symbols (SPY, QQQ, NVDA, BTC, ETH — marked is_watchlist) are
also the ones Addie's multi-agent reasoning pipeline (build_dashboard_data.py
+ a Workflow run) writes deep synthesis for. The rest of the universe exists
so the dashboard's "Markets" view has real breadth to browse/search/chart,
not just those five — but only the five get bull/bear/risk analysis.

WHY THIS SCRIPT RUNS IN GITHUB ACTIONS, NOT THE CLOUD SANDBOX:
The sandbox's outbound network access is limited to an allowlist and does
not include finance.yahoo.com or api.coingecko.com. This script needs a host
with normal internet access — GitHub Actions, scheduled via cron, is that
host. See README.md for the full setup.

Run it with:  python3 addie_pipeline.py
Requires:     pip install yfinance requests
"""

import json
import os
import time
from datetime import datetime, timezone

import requests
import yfinance as yf

# ---------------------------------------------------------------------------
# Universe — edit freely. "is_watchlist" symbols also get deep multi-agent
# reasoning (see build_dashboard_data.py); everything else is browse/chart
# data only. Categories mirror how the dashboard's Markets view filters.
# ---------------------------------------------------------------------------
EQUITY_UNIVERSE = [
    # symbol, category, display name, is_watchlist
    ("SPY", "index", "S&P 500 ETF", True),
    ("QQQ", "index", "Nasdaq-100 ETF", True),
    ("DIA", "index", "Dow Jones ETF", False),
    ("IWM", "index", "Russell 2000 ETF", False),
    ("VTI", "index", "Total US Market ETF", False),
    ("EFA", "index", "Intl Developed Markets ETF", False),
    ("NVDA", "tech", "Nvidia", True),
    ("AAPL", "tech", "Apple", False),
    ("MSFT", "tech", "Microsoft", False),
    ("GOOGL", "tech", "Alphabet", False),
    ("META", "tech", "Meta Platforms", False),
    ("AMZN", "tech", "Amazon", False),
    ("TSLA", "tech", "Tesla", False),
    ("AVGO", "tech", "Broadcom", False),
    ("AMD", "tech", "Advanced Micro Devices", False),
    ("CRM", "tech", "Salesforce", False),
    ("ORCL", "tech", "Oracle", False),
    ("NFLX", "tech", "Netflix", False),
    ("INTC", "tech", "Intel", False),
    ("IBM", "tech", "IBM", False),
    ("PLTR", "tech", "Palantir", False),
    ("BABA", "china", "Alibaba", False),
    ("PDD", "china", "PDD Holdings", False),
    ("JD", "china", "JD.com", False),
    ("NIO", "china", "NIO Inc.", False),
    ("BIDU", "china", "Baidu", False),
    ("LI", "china", "Li Auto", False),
    ("CAT", "industrial", "Caterpillar", False),
    ("DE", "industrial", "Deere & Co.", False),
    ("GE", "industrial", "GE Aerospace", False),
    ("BA", "industrial", "Boeing", False),
    ("HON", "industrial", "Honeywell", False),
    ("UPS", "industrial", "United Parcel Service", False),
    ("WMT", "consumer", "Walmart", False),
    ("COST", "consumer", "Costco", False),
    ("MCD", "consumer", "McDonald's", False),
    ("NKE", "consumer", "Nike", False),
    ("SBUX", "consumer", "Starbucks", False),
    ("HD", "consumer", "Home Depot", False),
    ("TGT", "consumer", "Target", False),
    ("DIS", "consumer", "Walt Disney", False),
    ("JPM", "finance", "JPMorgan Chase", False),
    ("GS", "finance", "Goldman Sachs", False),
    ("BAC", "finance", "Bank of America", False),
    ("V", "finance", "Visa", False),
    ("MA", "finance", "Mastercard", False),
    ("WFC", "finance", "Wells Fargo", False),
    ("MS", "finance", "Morgan Stanley", False),
    ("BRK-B", "finance", "Berkshire Hathaway", False),
    ("XOM", "energy", "Exxon Mobil", False),
    ("CVX", "energy", "Chevron", False),
    ("OXY", "energy", "Occidental Petroleum", False),
    ("COP", "energy", "ConocoPhillips", False),
    ("SLB", "energy", "Schlumberger", False),
    ("UNH", "health", "UnitedHealth Group", False),
    ("JNJ", "health", "Johnson & Johnson", False),
    ("PFE", "health", "Pfizer", False),
    ("LLY", "health", "Eli Lilly", False),
    ("ABBV", "health", "AbbVie", False),
    ("MRK", "health", "Merck", False),
    ("ADM", "ag", "Archer-Daniels-Midland", False),
    ("CORN", "ag", "Corn futures ETF", False),
    ("WEAT", "ag", "Wheat futures ETF", False),
    ("SOYB", "ag", "Soybean futures ETF", False),
    ("GLD", "macro", "Gold ETF", False),
    ("UUP", "macro", "US Dollar Index ETF", False),
    ("TLT", "macro", "20+yr Treasury ETF", False),
    ("SLV", "macro", "Silver ETF", False),
    ("USO", "macro", "Oil ETF", False),
]

CRYPTO_UNIVERSE = [
    # coingecko id, category, display symbol, display name, is_watchlist
    ("bitcoin", "crypto", "BTC", "Bitcoin", True),
    ("ethereum", "crypto", "ETH", "Ethereum", True),
    ("solana", "crypto", "SOL", "Solana", False),
    ("ripple", "crypto", "XRP", "XRP", False),
    ("dogecoin", "crypto", "DOGE", "Dogecoin", False),
    ("cardano", "crypto", "ADA", "Cardano", False),
    ("avalanche-2", "crypto", "AVAX", "Avalanche", False),
    ("chainlink", "crypto", "LINK", "Chainlink", False),
    ("litecoin", "crypto", "LTC", "Litecoin", False),
    ("binancecoin", "crypto", "BNB", "BNB", False),
    ("the-open-network", "crypto", "TON", "Toncoin", False),
    ("shiba-inu", "crypto", "SHIB", "Shiba Inu", False),
    ("polkadot", "crypto", "DOT", "Polkadot", False),
    ("uniswap", "crypto", "UNI", "Uniswap", False),
    ("near", "crypto", "NEAR", "NEAR Protocol", False),
]

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

HISTORY_POINTS = 90   # ~3 calendar months — sparkline / ticker-tape mini chart
COINGECKO_PAUSE = 1.3  # seconds between CoinGecko calls — free tier is rate-limited


def pull_stock(symbol: str, category: str, name: str, is_watchlist: bool) -> dict:
    t = yf.Ticker(symbol)
    hist = t.history(period="5y")
    info = t.fast_info

    price = float(info["lastPrice"])
    prev_close = float(info["previousClose"])
    day_change_pct = (price - prev_close) / prev_close * 100

    last_1y = hist.tail(252)  # ~1 trading year of the 5y pull
    high_52w = float(last_1y["High"].max())
    low_52w = float(last_1y["Low"].min())
    range_position_pct = (price - low_52w) / (high_52w - low_52w) * 100

    recent = hist.tail(HISTORY_POINTS)
    sparkline = [
        {"t": idx.strftime("%Y-%m-%d"), "c": round(float(row["Close"]), 2)}
        for idx, row in recent.iterrows()
    ]
    ohlc = [
        {
            "t": idx.strftime("%Y-%m-%d"),
            "o": round(float(row["Open"]), 2),
            "h": round(float(row["High"]), 2),
            "l": round(float(row["Low"]), 2),
            "c": round(float(row["Close"]), 2),
        }
        for idx, row in hist.iterrows()
    ]

    return {
        "symbol": symbol,
        "name": name,
        "category": category,
        "is_watchlist": is_watchlist,
        "asset_class": "equity",
        "price": round(price, 2),
        "day_change_pct": round(day_change_pct, 2),
        "52w_high": round(high_52w, 2),
        "52w_low": round(low_52w, 2),
        "52w_range_position_pct": round(range_position_pct, 1),
        "history": sparkline,
        "ohlc": ohlc,          # up to 5y of daily bars — candlestick source
        "long_history": [],    # equities' `ohlc` already covers the long range
    }


def pull_crypto(coin_id: str, category: str, symbol: str, name: str, is_watchlist: bool) -> dict:
    resp = requests.get(
        "https://api.coingecko.com/api/v3/simple/price",
        params={
            "ids": coin_id,
            "vs_currencies": "usd",
            "include_24hr_change": "true",
            "include_market_cap": "true",
        },
        timeout=10,
    )
    resp.raise_for_status()
    d = resp.json()[coin_id]
    price = round(d["usd"], 2)

    record = {
        "symbol": symbol,
        "name": name,
        "category": category,
        "is_watchlist": is_watchlist,
        "asset_class": "crypto",
        "price": price,
        "day_change_pct": round(d["usd_24h_change"], 2),
        "market_cap_usd": d.get("usd_market_cap"),
        "history": [],
        "ohlc": [],
        "long_history": [],
    }
    time.sleep(COINGECKO_PAUSE)

    # Daily OHLC, free-tier capped at 365 days — this is the candlestick source
    # and also where the 52-week range comes from (max/min of the bars).
    try:
        ohlc_resp = requests.get(
            f"https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc",
            params={"vs_currency": "usd", "days": "365"},
            timeout=20,
        )
        ohlc_resp.raise_for_status()
        bars = ohlc_resp.json()  # [[ts_ms, open, high, low, close], ...]
        if bars:
            ohlc = []
            for ts_ms, o, h, l, c in bars:
                day = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
                ohlc.append({"t": day, "o": round(o, 2), "h": round(h, 2), "l": round(l, 2), "c": round(c, 2)})
            record["ohlc"] = ohlc
            highs = [b["h"] for b in ohlc]
            lows = [b["l"] for b in ohlc]
            if highs and lows:
                high_52w, low_52w = max(highs), min(lows)
                if high_52w > low_52w:
                    record["52w_high"] = round(high_52w, 2)
                    record["52w_low"] = round(low_52w, 2)
                    record["52w_range_position_pct"] = round(
                        (price - low_52w) / (high_52w - low_52w) * 100, 1
                    )
    except requests.RequestException:
        pass  # OHLC/range is a nice-to-have; price/change above already landed.
    time.sleep(COINGECKO_PAUSE)

    # Extended close-only series (beyond the 365-day OHLC window) for 5Y/MAX
    # line-mode charting, and the source for the 90-day sparkline slice.
    try:
        chart_resp = requests.get(
            f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart",
            params={"vs_currency": "usd", "days": "max", "interval": "daily"},
            timeout=20,
        )
        chart_resp.raise_for_status()
        prices = chart_resp.json().get("prices", [])
        if prices:
            long_history = []
            for ts_ms, price_pt in prices:
                day = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
                long_history.append({"t": day, "c": round(float(price_pt), 2)})
            record["long_history"] = long_history
            record["history"] = long_history[-HISTORY_POINTS:]
    except requests.RequestException:
        pass
    time.sleep(COINGECKO_PAUSE)

    return record


def reason_about(m: dict) -> str:
    """
    Addie v0's commentary logic — deliberately simple and transparent.
    Every clause here is traceable to a specific number, on purpose:
    a black-box "trust me" signal is exactly what we're avoiding.
    Deep (multi-agent) reasoning for the watchlist symbols lives separately
    in build_dashboard_data.py / the reasoning Workflow — this stays as the
    cheap, mechanical fallback commentary for every symbol in the universe.
    """
    lines = []
    chg = m["day_change_pct"]
    direction = "up" if chg > 0.15 else "down" if chg < -0.15 else "flat"
    lines.append(f"{direction.capitalize()} {abs(chg):.2f}% on the session.")

    if m.get("52w_range_position_pct") is not None:
        pos = m["52w_range_position_pct"]
        if pos >= 90:
            lines.append(
                "Within 10% of its 52-week high — momentum favors continuation, "
                "but there's little cushion before this stretches into new-high "
                "territory. A sentiment shift lands closer to the top of the "
                "range than the bottom."
            )
        elif pos <= 15:
            lines.append(
                "Near its 52-week low — either a genuine deterioration or a "
                "contrarian setup; distinguishing the two needs more than "
                "price alone (this is a flag for deeper research, not a signal)."
            )
        else:
            lines.append(
                f"Trading mid-range ({pos:.0f}% of the 52-week span) — no "
                "strong momentum or mean-reversion pull either way right now."
            )
    else:
        lines.append(
            "Crypto's realized volatility runs several multiples of equities — "
            "treat any single day's move (up or down) as noise unless it's "
            "part of a multi-day trend."
        )

    return " ".join(lines)


def run():
    records = []
    for symbol, category, name, is_watchlist in EQUITY_UNIVERSE:
        try:
            m = pull_stock(symbol, category, name, is_watchlist)
            m["addie_commentary"] = reason_about(m)
            records.append(m)
        except Exception as e:  # noqa: BLE001 — one bad symbol shouldn't sink the run
            print(f"WARN: pull_stock({symbol}) failed: {e}")

    for coin_id, category, symbol, name, is_watchlist in CRYPTO_UNIVERSE:
        try:
            m = pull_crypto(coin_id, category, symbol, name, is_watchlist)
            m["addie_commentary"] = reason_about(m)
            records.append(m)
        except Exception as e:  # noqa: BLE001
            print(f"WARN: pull_crypto({coin_id}) failed: {e}")

    snapshot = {
        "run_at_utc": datetime.now(timezone.utc).isoformat(),
        "watchlist": records,
    }

    # Timestamped copy — the permanent, append-only audit trail (never overwritten).
    ts_path = os.path.join(
        DATA_DIR, f"snapshot_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}Z.json"
    )
    with open(ts_path, "w") as f:
        json.dump(snapshot, f, indent=2)

    # Stable "latest" copy — this is the one the dashboard fetches by a fixed URL.
    latest_path = os.path.join(DATA_DIR, "latest.json")
    with open(latest_path, "w") as f:
        json.dump(snapshot, f, indent=2)

    print(f"Wrote {ts_path}")
    print(f"Wrote {latest_path}")
    print(f"\n{len(records)}/{len(EQUITY_UNIVERSE) + len(CRYPTO_UNIVERSE)} symbols pulled successfully")
    for r in records:
        print(f"{r['symbol']:6s} {r['category']:10s} ${r['price']:<12.2f} ({r['day_change_pct']:+.2f}%)"
              f"{'  [watchlist]' if r['is_watchlist'] else ''}")

    return snapshot


if __name__ == "__main__":
    run()
