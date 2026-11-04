"""Independent EMA(20) check for the realtime universe scan.

Used for the live check in docs/STREAMING_EVALUATION.md: a "Price Above EMA 20"
scan (15-minute bars) prints the EMA it compared against in each match
("CLOSE > 551.39"); this recomputes that value from a different data source.

Recomputes EMA(20) on Yahoo 15-minute bars the same way the app does (SMA seed
over the first 20 closes, then alpha = 2/21), plus the scan's two tick bars
(previous and last price), and prints two versions:

  * full     - every completed bar from 55 days before the evaluation time
               (what the scan should use);
  * first500 - only the first 500 bars of that range, extended hours included:
               what AlpacaBroker::get_history returned before ac3a2306
               (limit=500&sort=asc, next_page_token ignored).

Yahoo prints are consolidated while Alpaca's free feed is IEX-only, so expect
agreement to roughly 0.1-0.3 %, not to the cent.

usage: python ema_check.py [--at 'YYYY-MM-DD HH:MM:SS'] SYMBOL=APP_EMA[@PRICE] ...
       python ema_check.py AAPL MSFT          (no app values: just print)
Times are US/Eastern. Needs yfinance and pandas. Yahoo keeps ~60 days of 15m bars.
"""
import sys
from datetime import timedelta

import pandas as pd
import yfinance as yf

PERIOD = 20
ALPHA = 2.0 / (PERIOD + 1)


def ema(closes):
    closes = list(closes)
    if len(closes) < PERIOD:
        return float("nan")
    v = sum(closes[:PERIOD]) / PERIOD
    for c in closes[PERIOD:]:
        v = (c - v) * ALPHA + v
    return v


def with_ticks(e, price):
    # tick_bars(): prev and last price appended as two closed bars (prev ~ last)
    for _ in range(2):
        e = (price - e) * ALPHA + e
    return e


AT = None  # evaluation time (ET); default now. Set with --at "YYYY-MM-DD HH:MM:SS"


def eval_time():
    return AT if AT is not None else pd.Timestamp.now(tz="America/New_York")


def price_at(symbol):
    """Last 1-minute close at or before the evaluation time (extended hours included)."""
    df = yf.Ticker(symbol).history(period="1d", interval="1m", prepost=True, auto_adjust=False)
    df = df.tz_convert("America/New_York")
    df = df[df.index + pd.Timedelta(minutes=1) <= eval_time() + pd.Timedelta(minutes=1)]
    return float(df["Close"].iloc[-1])


def bars(symbol, prepost):
    df = yf.Ticker(symbol).history(period="60d", interval="15m", prepost=prepost, auto_adjust=False)
    df = df.tz_convert("America/New_York")
    now = eval_time()
    start = (now - timedelta(days=55)).normalize()
    df = df[(df.index >= start) & (df.index + pd.Timedelta(minutes=15) <= now)]  # completed bars only
    return df


def main(args):
    print(f"evaluated at (ET): {eval_time():%Y-%m-%d %H:%M:%S}")
    print(f"{'sym':5} {'price':>9} {'full EMA':>9} {'+ticks':>9} | {'first500 ends':>16} {'EMA':>9} {'+ticks':>9} | {'app EMA':>9}  verdict")
    for a in args:
        sym, app_ema, app_px = a, None, None
        if "=" in a:
            sym, rest = a.split("=", 1)
            app_ema, _, px = rest.partition("@")
            app_ema, app_px = float(app_ema), (float(px) if px else None)
        rth = bars(sym, prepost=False)
        ext = bars(sym, prepost=True)
        price = app_px if app_px else price_at(sym)
        full = ema(rth["Close"])
        first = ext.iloc[:500]
        stale = ema(first["Close"])
        full_t, stale_t = with_ticks(full, price), with_ticks(stale, price)
        verdict = ""
        if app_ema is not None:
            d_full, d_stale = abs(app_ema - full_t), abs(app_ema - stale_t)
            verdict = "matches FULL history" if d_full < d_stale else "matches FIRST-500 (stale) history"
            verdict += f"  (|d| full={d_full:.2f}, stale={d_stale:.2f})"
        print(f"{sym:5} {price:9.2f} {full:9.2f} {full_t:9.2f} | {first.index[-1]:%Y-%m-%d %H:%M} {stale:9.2f} {stale_t:9.2f} | "
              f"{(f'{app_ema:9.2f}' if app_ema else '        -')}  {verdict}")


if __name__ == "__main__":
    argv = sys.argv[1:]
    if "--at" in argv:
        i = argv.index("--at")
        AT = pd.Timestamp(argv[i + 1], tz="America/New_York")
        del argv[i:i + 2]
    sys.argv[1:] = argv
    main(sys.argv[1:] or ["AAPL", "MSFT", "NVDA", "TSLA", "AMD", "SPY"])
