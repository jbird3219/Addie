"""
Addie — Data Pipeline v0
=========================
Pulls a watchlist of stocks/ETFs (via yfinance) and crypto (via CoinGecko's
free public API — no key required), computes a handful of transparent,
rule-based metrics, generates plain-English commentary, and appends a
timestamped record to the decision log.

WHY THIS SCRIPT DOESN'T RUN INSIDE THE CLAUDE COWORK CLOUD SANDBOX:
The sandbox's outbound network access is limited to an allowlist (package
registries, Anthropic's own API) and does not include general internet
hosts like finance.yahoo.com or api.coingecko.com. This is why today's
first dashboard snapshot was hand-built from data pulled via the WebFetch
tool instead of by running this script. This script is real, runnable code
— it just needs a home with normal internet access (your own machine, or
eventually a small always-on server) to actually execute on a schedule.
See README.md in this folder for what to do next.

Run it with:  python3 addie_pipeline.py
Requires:     pip install yfinance requests
"""

import json
import os
from datetime import datetime, timezone

import requests
import yfinance as yf

# ---------------------------------------------------------------------------
# Watchlist — edit freely. This is Addie's universe, not a portfolio yet.
# ---------------------------------------------------------------------------
STOCK_WATCHLIST = ["SPY", "QQQ", "NVDA"]          # tickers, via yfinance
CRYPTO_WATCHLIST = ["bitcoin", "ethereum"]        # CoinGecko coin ids

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)


def pull_stock(ticker: str) -> dict:
    t = yf.Ticker(ticker)
    hist = t.history(period="1y")
    info = t.fast_info

    price = float(info["lastPrice"])
    prev_close = float(info["previousClose"])
    day_change_pct = (price - prev_close) / prev_close * 100

    high_52w = float(hist["High"].max())
    low_52w = float(hist["Low"].min())
    range_position_pct = (price - low_52w) / (high_52w - low_52w) * 100

    return {
        "symbol": ticker,
        "asset_class": "equity",
        "price": round(price, 2),
        "day_change_pct": round(day_change_pct, 2),
        "52w_high": round(high_52w, 2),
        "52w_low": round(low_52w, 2),
        "52w_range_position_pct": round(range_position_pct, 1),
    }


def pull_crypto(coin_id: str) -> dict:
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
    return {
        "symbol": coin_id.upper(),
        "asset_class": "crypto",
        "price": round(d["usd"], 2),
        "day_change_pct": round(d["usd_24h_change"], 2),
        "market_cap_usd": d.get("usd_market_cap"),
    }


def reason_about(m: dict) -> str:
    """
    Addie v0's commentary logic — deliberately simple and transparent.
    Every clause here is traceable to a specific number, on purpose:
    a black-box "trust me" signal is exactly what we're avoiding in v0.
    """
    lines = []
    chg = m["day_change_pct"]
    direction = "up" if chg > 0.15 else "down" if chg < -0.15 else "flat"
    lines.append(f"{direction.capitalize()} {abs(chg):.2f}% on the session.")

    if m["asset_class"] == "equity":
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
    for ticker in STOCK_WATCHLIST:
        m = pull_stock(ticker)
        m["addie_commentary"] = reason_about(m)
        records.append(m)

    for coin_id in CRYPTO_WATCHLIST:
        m = pull_crypto(coin_id)
        m["addie_commentary"] = reason_about(m)
        records.append(m)

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
    for r in records:
        print(f"\n{r['symbol']}  ${r['price']}  ({r['day_change_pct']:+.2f}%)")
        print(f"  Addie: {r['addie_commentary']}")

    return snapshot


if __name__ == "__main__":
    run()
