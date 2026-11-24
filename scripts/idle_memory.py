#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Measure server memory per idle TCP connection.

Starts snserver, records its resident set size (VmRSS), opens N idle
connections, waits for the server to accept them all, and reports the RSS
growth divided by N. Then sends one message on every connection and
measures again, to show buffers being released once traffic stops.

    scripts/idle_memory.py build/snserver [N]
"""
import re
import socket
import subprocess
import sys
import time


def rss_kib(pid):
    with open(f"/proc/{pid}/status") as f:
        return int(re.search(r"VmRSS:\s+(\d+)", f.read()).group(1))


def main():
    server = sys.argv[1]
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 5000
    proc = subprocess.Popen(
        [server, "--host", "127.0.0.1", "--tcp-port", "0", "--udp-port", "-1",
         "--workers", "2", "--max-conns", str(n)],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    try:
        port = int(re.search(r"tcp=(\d+)", proc.stdout.readline()).group(1))
        time.sleep(0.3)
        base = rss_kib(proc.pid)

        socks = []
        for _ in range(n):
            s = socket.create_connection(("127.0.0.1", port))
            socks.append(s)
        time.sleep(1.0)  # let both workers accept the backlog
        idle = rss_kib(proc.pid)

        for s in socks:  # one round trip each, then idle again
            s.sendall(b"x" * 100)
        for s in socks:
            got = b""
            while len(got) < 100:
                got += s.recv(100 - len(got))
        time.sleep(0.5)
        after = rss_kib(proc.pid)

        per_conn = (idle - base) * 1024 / n
        print(f"connections:            {n}")
        print(f"server RSS at start:    {base} KiB")
        print(f"RSS with {n} idle:  {idle} KiB  "
              f"(+{idle - base} KiB, {per_conn:.0f} bytes/connection)")
        print(f"RSS after 1 echo each:  {after} KiB  "
              f"({(after - base) * 1024 / n:.0f} bytes/connection)")
        print(f"RESULT idle_bytes_per_conn={per_conn:.0f}")
        for s in socks:
            s.close()
    finally:
        proc.terminate()
        proc.wait(timeout=10)


if __name__ == "__main__":
    main()
