#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Benchmark matrix against a local snserver; prints a Markdown table.
#   scripts/bench.sh [build-dir] [duration-seconds]
#
# Client and server share the machine, so results measure the whole loop
# (both ends plus loopback), not the server alone.
set -euo pipefail

BUILD=${1:-build}
DUR=${2:-3}
WORKERS=${WORKERS:-2}
CLIENT_THREADS=${CLIENT_THREADS:-2}
log=$(mktemp)
trap 'kill "$pid" 2>/dev/null || true; rm -f "$log"' EXIT

"$BUILD/snserver" --host 127.0.0.1 --tcp-port 0 --udp-port 0 \
	--workers "$WORKERS" --max-conns 20000 >"$log" 2>&1 &
pid=$!
for _ in $(seq 100); do grep -q '^listening' "$log" && break; sleep 0.05; done
read -r tcp udp < <(sed -n 's/^listening tcp=\([0-9]*\) udp=\([0-9]*\).*/\1 \2/p' "$log")

echo "host: $(nproc) CPUs, $(uname -sr); server workers=$WORKERS, client threads=$CLIENT_THREADS, ${DUR}s per row"
echo
echo "| proto | conns | size | depth | req/s | MB/s | p50 µs | p99 µs | p99.9 µs |"
echo "|---|---|---|---|---|---|---|---|---|"
run() {
	local line
	# Options given later win, so a row may override --threads.
	line=$("$BUILD/snbench" --threads "$CLIENT_THREADS" "$@" \
		--duration "$DUR" --warmup 1 | grep '^RESULT')
	# shellcheck disable=SC2086
	eval "$(sed 's/^RESULT //' <<<"$line" | tr ' ' '\n' | sed 's/^/r_/')"
	printf '| %s | %s | %s | %s | %s | %s | %s | %s | %s |\n' "$r_proto" \
		"$r_conns" "$r_size" "$r_depth" "$r_rps" "$r_mbps" "$r_p50_us" \
		"$r_p99_us" "$r_p999_us"
}
run --port "$tcp" --conns 1 --size 64 --depth 1 --threads 1
run --port "$tcp" --conns 64 --size 64 --depth 1
run --port "$tcp" --conns 64 --size 64 --depth 16
run --port "$tcp" --conns 64 --size 4096 --depth 4
run --port "$tcp" --conns 64 --size 65536 --depth 2
run --port "$tcp" --conns 1000 --size 128 --depth 1
run --port "$udp" --udp --conns 64 --size 64

kill -TERM "$pid"
wait "$pid" || true
