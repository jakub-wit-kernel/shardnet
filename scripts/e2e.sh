#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# End-to-end check: start snserver on free ports, drive it with snbench over
# TCP (pipelined) and UDP, then stop it with SIGTERM and require a clean exit.
#   scripts/e2e.sh path/to/snserver path/to/snbench
set -euo pipefail

SERVER=$1
BENCH=$2
DURATION=${E2E_DURATION:-2}
log=$(mktemp)
trap 'kill "$pid" 2>/dev/null || true; rm -f "$log"' EXIT

"$SERVER" --host 127.0.0.1 --tcp-port 0 --udp-port 0 --workers 2 >"$log" 2>&1 &
pid=$!

for _ in $(seq 100); do
	grep -q '^listening' "$log" && break
	sleep 0.05
done
read -r tcp udp < <(sed -n 's/^listening tcp=\([0-9]*\) udp=\([0-9]*\).*/\1 \2/p' "$log")
[[ -n ${tcp:-} ]] || { echo "server did not start:"; cat "$log"; exit 1; }
echo "server pid $pid tcp=$tcp udp=$udp"

"$BENCH" --port "$tcp" --threads 2 --conns 32 --size 128 --depth 8 \
	--duration "$DURATION" --warmup 0.5
"$BENCH" --port "$udp" --udp --threads 2 --conns 16 --size 64 \
	--duration "$DURATION" --warmup 0.5

kill -TERM "$pid"
status=0
wait "$pid" || status=$?
cat "$log"
grep -q 'stopped' "$log" || { echo "server did not stop cleanly"; exit 1; }
exit "$status"
