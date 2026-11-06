"""Check the scan's FIRE lines against the gateway's own bars (Alpaca IEX, from PostgreSQL).

For each half (a time window of fincept.log) and each FIRE line:
  - closed 15m bars before the fire come from HISTORY 15m (the warm set starts at the same bar
    the terminal's did; checked with the warm fingerprint);
  - the scan evaluates EMA20 over closed bars + [prev tick, last tick] (tick_bars), so
    E = a*p + a*(1-a)*p_prev + (1-a)^2 * C   (C = EMA over closed bars, a = 2/21);
  - p and p_prev must lie in the 1m bar range around the fire; the logged target must fall
    inside the E range those prices allow, and the implied price must be above E.
Then, per minute while the symbol was armed, whether the 1m high crossed the EMA (a fire the
scan should have produced) — a rough check for missed signals.

Used for the market-hours check in gateway/README.md ("Live data for the terminal"). Needs a
running gateway with the symbols' history (--sync-symbols) for HISTORY on port 8588.

usage: python -I scan_ab_check.py FINCEPT_LOG DAY START END [START END ...]
       (DAY yyyy-mm-dd, times HH:MM:SS, both US Eastern standard time)
"""
import re
import socket
import sys
from datetime import datetime, timedelta, timezone

ET = timezone(timedelta(hours=-5))
A = 2.0 / 21.0
SYMS = ["AAPL", "MSFT", "NVDA", "TSLA", "AMD", "SPY"]


def ask(line):
    with socket.create_connection(("127.0.0.1", 8588), timeout=30) as s:
        s.sendall((line + "\n").encode())
        s.shutdown(socket.SHUT_WR)
        data = b""
        while chunk := s.recv(1 << 20):
            data += chunk
    return data.decode()


def history(tf, frm, to, syms):
    out = {s: [] for s in syms}
    lines = ask(f"HISTORY {tf} {frm} {to} {' '.join(syms)}").splitlines()
    assert lines[0].startswith("BARS"), lines[0]
    for ln in lines[1:]:
        p = ln.split()
        if len(p) != 7:
            continue
        out[p[0]].append((int(p[1]), float(p[2]), float(p[3]), float(p[4]), float(p[5]), float(p[6])))
    return out


def ema_closed(closes):
    v, n = 0.0, 0
    for x in closes:
        if n < 20:
            v += x
            n += 1
            if n == 20:
                v /= 20
            continue
        v = (x - v) * A + v
    return v


DAY = ""


def ts(hms):
    return int(datetime.fromisoformat(f"{DAY}T{hms}").replace(tzinfo=ET).timestamp())


def main():
    global DAY
    log = open(sys.argv[1], encoding="utf-8", errors="replace").read().splitlines()
    DAY = sys.argv[2]
    windows = [(ts(sys.argv[i]), ts(sys.argv[i + 1])) for i in range(3, len(sys.argv), 2)]
    now = int(datetime.now(timezone.utc).timestamp())
    # The scan warms from UTC midnight 53 days back (the terminal's window for 15m bars);
    # the warm lines below check that this gives the same bars.
    day0 = datetime.fromisoformat(DAY).replace(tzinfo=timezone.utc) - timedelta(days=53)
    h15 = history("15m", int(day0.timestamp()), now, SYMS)
    h1 = history("1m", ts("09:00:00"), now, SYMS)
    pat = re.compile(r"^\[(\S+ \S+)\] \[INFO\] \[ScanMonitor\] FIRE .*? / (\w+): CLOSE > ([\d.]+)")
    warm = re.compile(r"warm (\w+): (\d+) bars .*? \.\. (\S+ \S+), .*last 500 closes sum ([\d.]+), last close ([\d.]+)")
    for wi, (t0, t1) in enumerate(windows):
        print(f"== run {wi + 1}: {datetime.fromtimestamp(t0, ET):%H:%M:%S} .. {datetime.fromtimestamp(t1, ET):%H:%M:%S}")
        fires = []
        for ln in log:
            m = re.match(r"^\[(\S+ [\d:.]+)", ln)
            if not m:
                continue
            t = datetime.fromisoformat(m.group(1).replace(" ", "T")).replace(tzinfo=ET).timestamp()
            if not (t0 <= t < t1):
                continue
            w = warm.search(ln)
            if w:
                sym, last_start = w.group(1), w.group(3)
                ls = int(datetime.fromisoformat(last_start.replace(" ", "T")).replace(tzinfo=ET).timestamp())
                bars = [b for b in h15[sym] if b[0] <= ls]
                s500 = sum(b[4] for b in bars[-500:])
                ok = abs(s500 - float(w.group(4))) < 0.005 and len(bars) == int(w.group(2))
                print(f"  warm {sym}: log {w.group(2)} bars sum {w.group(4)} | gateway {len(bars)} bars sum {s500:.4f}"
                      f" -> {'same' if ok else 'DIFFERENT'}")
            f = pat.match(ln)
            if f:
                fires.append((t, f.group(2), float(f.group(3))))
        for t, sym, target in fires:
            closed = [b for b in h15[sym] if b[0] + 900 <= t]
            c = ema_closed([b[4] for b in closed])
            mins = [b for b in h1[sym] if t - 120 <= b[0] <= t]
            lo, hi = min(b[3] for b in mins), max(b[2] for b in mins)
            # n = how many times the live price went into the EMA on top of the closed bars
            # (1: first tick only, 2: prev + last tick, 3: an extra closed bar at ~the same price).
            fits, implied = [], []
            for n in (1, 2, 3):
                k = (1 - A) ** n
                p = (target - k * c) / (1 - k)
                implied.append(f"n={n}: {p:.2f}")
                if lo - 0.01 <= p <= hi + 0.01 and p > target:
                    fits.append(str(n))
            print(f"  FIRE {datetime.fromtimestamp(t, ET):%H:%M:%S.%f}"[:-3] +
                  f" {sym:4} target {target:.2f} | closed-bar EMA {c:.4f}, trades {lo}..{hi} (last 2 min)"
                  f" | implied price {', '.join(implied)} -> "
                  f"{'fits n=' + '/'.join(fits) if fits else 'NO TRADE PRICE FITS'}")
        fired = {s for _, s, _ in fires}
        for sym in SYMS:
            crossed = []
            for b in h1[sym]:
                if not (t0 - 60 <= b[0] < t1):
                    continue
                closed = [x for x in h15[sym] if x[0] + 900 <= b[0]]
                c = ema_closed([x[4] for x in closed])
                e_hi = (1 - (1 - A) ** 2) * b[2] + (1 - A) ** 2 * c
                if b[2] > e_hi:
                    crossed.append(datetime.fromtimestamp(b[0], ET).strftime("%H:%M"))
            print(f"  {sym:4} fired={'yes' if sym in fired else 'no '} minutes with price above EMA: "
                  f"{', '.join(crossed) if crossed else 'none'}")


if __name__ == "__main__":
    main()
