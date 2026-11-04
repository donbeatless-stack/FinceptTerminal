#!/usr/bin/env bash
# Runs the benchmark matrix quoted in gateway/README.md. Usage, from gateway/:
#   bench/run_bench.sh [path/to/fmdg_loadgen]
set -euo pipefail
LG="${1:-build/win/fmdg_loadgen.exe}"
run() { echo "### $*"; "$LG" --embedded --duration 8 "$@" | grep -E "feed|deliveries|conflated|received|ingest ->|ordering"; echo; }

# 1. Latency vs fan-out at a light, steady load (10k ticks/s over 100 symbols)
run --clients 1  --rate 10000 --gateway-io-threads 4 --io-threads 4
run --clients 1  --rate 10000 --gateway-io-threads 4 --io-threads 4 --busy-poll
run --clients 10 --rate 10000 --gateway-io-threads 4 --io-threads 4
run --clients 50 --rate 10000 --gateway-io-threads 4 --io-threads 4
# 2. Overload: 50 clients x 100k ticks/s = 5M deliveries/s, by network threads
for t in 2 4 8; do run --clients 50 --rate 100000 --gateway-io-threads $t --io-threads $t; done
# 3. Slow-consumer isolation: 5 of 50 clients capped at ~80 KB/s
run --clients 50 --slow 5 --rate 10000 --gateway-io-threads 4 --io-threads 4
run --clients 50 --slow 5 --rate 10000 --gateway-io-threads 4 --io-threads 4 --send-buffer 16384
# 4. Distributor ceiling: unpaced feed, 1000 symbols
run --clients 1  --symbols 1000 --rate 0 --gateway-io-threads 4 --io-threads 4
run --clients 10 --symbols 1000 --rate 0 --gateway-io-threads 4 --io-threads 4
