# shardnet: multi-reactor TCP/UDP server engine for Linux, in C

[![CI](https://github.com/jakub-wit-kernel/shardnet/actions/workflows/ci.yml/badge.svg)](https://github.com/jakub-wit-kernel/shardnet/actions/workflows/ci.yml)
![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue)

shardnet is a small, dependency-free C11 library for building
high-concurrency network servers on Linux. It comes with an echo server
(`snserver`) and a pipelined load generator (`snbench`) that reports latency
percentiles.

The design is **share-nothing**. Each worker thread owns an epoll loop and
its own `SO_REUSEPORT` TCP and UDP sockets, and the kernel spreads
connections across workers. A connection lives on one thread for its whole
life, so the hot path takes no locks.

## Highlights

- **Non-blocking I/O on epoll**, level-triggered, with fairness limits: one
  read per readiness event, bounded accept and `recvmmsg` batches.
- **TCP and UDP.** UDP reads up to 32 datagrams per syscall with `recvmmsg`.
- **Connection management:**
  - O(1) pooled connection objects
  - an admission limit
  - idle timeouts via an O(1) LRU list
  - peer half-close with output flush
  - close-after-send
- **Backpressure.** Reading pauses when unsent output passes a threshold and
  resumes at half (hysteresis), so a peer that never reads can't grow server
  memory without bound.
- **Memory-lean.** Reads go to a shared per-worker scratch buffer; only
  unconsumed input and unsent output are buffered, and buffers are freed
  when they drain. Measured cost: **about 106 bytes of user-space memory per
  idle connection**.
- **Graceful shutdown.** Stop accepting, flush, close, and force-close at a
  deadline.
- **Robustness details:**
  - deferred frees, so a connection closed earlier in an `epoll_wait` batch
    is never touched again
  - `EMFILE` shedding with a reserved spare fd
  - `MSG_NOSIGNAL` throughout
  - all signals blocked in workers
- **Observability:** per-worker atomic counters and a leveled, thread-tagged
  logger.

## Quick start

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
(cd build && ctest --output-on-failure)        # unit + integration + end-to-end

./build/snserver --tcp-port 7000 --udp-port 7000 --workers 4 --stats-sec 5 &
./build/snbench --port 7000 --conns 64 --size 128 --depth 8 --duration 5
./build/snbench --port 7000 --udp --conns 32 --size 64
kill %1                                         # SIGTERM: graceful drain
```

Sanitizer builds: `-DSN_SANITIZE=address,undefined` or
`-DSN_SANITIZE=thread`.

## Using the library

```c
#include <stdio.h>
#include "shardnet/shardnet.h"

/* Line-oriented upper-casing server: shows partial consumption. */
static ssize_t on_data(sn_conn *c, const uint8_t *d, size_t len, void *ud)
{
	size_t used = 0;
	(void)ud;
	for (size_t i = 0; i < len; i++) {
		if (d[i] != '\n')
			continue;
		char line[256];
		size_t n = i + 1 - used < sizeof(line) ? i + 1 - used : sizeof(line);
		for (size_t k = 0; k < n; k++)
			line[k] = (char)(d[used + k] >= 'a' && d[used + k] <= 'z'
					 ? d[used + k] - 32 : d[used + k]);
		sn_conn_send(c, line, n);
		used = i + 1;
	}
	return (ssize_t)used;	/* a partial line stays buffered */
}

int main(void)
{
	sn_handlers h = { .on_data = on_data };
	sn_config cfg;
	sn_server *s;

	sn_config_init(&cfg);
	cfg.tcp_port = 7001;
	cfg.idle_timeout_ms = 30000;
	if (sn_server_create(&cfg, &h, NULL, &s) || sn_server_start(s))
		return 1;
	printf("listening on %d\n", sn_server_tcp_port(s));
	getchar();			/* press Enter to stop */
	sn_server_stop(s, 2000);
	sn_server_destroy(s);
	return 0;
}
```

Every callback for a connection runs on that connection's worker thread.
`sn_conn_*` functions must be called from inside a callback. See
[include/shardnet/shardnet.h](include/shardnet/shardnet.h).

## Measured results

**Memory** (`scripts/idle_memory.py`, 5,000 idle connections): server RSS
grew 520 KiB, which is **106 bytes per connection**. It did not grow after
every connection echoed a message and went idle again. Kernel socket memory
is not included.

**Throughput and latency** (`scripts/bench.sh`): 2-vCPU development sandbox,
Linux 6.18, 2 server workers, with client and server **on the same 2 CPUs**.
Tail latencies here are dominated by CPU contention between the two
processes. CI publishes the same table from a GitHub-hosted runner in each
run's job summary.

| proto | conns | size | depth | req/s | MB/s | p50 µs | p99 µs |
|---|---|---|---|---|---|---|---|
| tcp | 1 | 64 | 1 | 28,269 | 1.8 | 34 | 67 |
| tcp | 64 | 64 | 1 | 208,805 | 13.4 | 85 | 4,391 |
| tcp | 64 | 64 | 16 | 437,090 | 28.0 | 1,262 | 8,782 |
| tcp | 64 | 4096 | 4 | 300,287 | 1,230 | 291 | 5,833 |
| tcp | 64 | 65536 | 2 | 58,614 | 3,841 | 1,360 | 8,520 |
| tcp | 1000 | 128 | 1 | 210,069 | 26.9 | 4,522 | 11,403 |
| udp | 64 | 64 | 1 | 281,637 | 18.0 | 65 | 4,162 |

`snbench` is a closed-loop generator, so its percentiles are subject to
coordinated omission. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) §9.

## Testing

| Suite | What it covers |
|---|---|
| `test_buf`, `test_pool`, `test_hist` | buffer compaction and growth, pool exhaustion and reuse, histogram bucket math and percentile error < 1/32 against exact sorting |
| `test_server` (16 cases) | echo of 8 MiB with partial writes; 256 concurrent connections spread over workers; backpressure bound; idle timeout; connection limit; half-close flush; partial input consumption; input limit; close-after-reply; graceful and deadline-forced shutdown; UDP echo and oversize drop; config validation; port conflict |
| `e2e_bench` | real `snserver` process + `snbench` over TCP and UDP, then a SIGTERM drain |

All suites run under **ASan+UBSan**, **ThreadSanitizer** and **valgrind**
with gcc and clang, plus **clang-tidy**, in CI.

## Layout

```
include/shardnet/   public API: shardnet.h, hist.h, log.h
src/                server.c (reactor), buf, pool, hist, log
apps/               snserver (echo server), snbench (load generator)
tests/              unit and integration tests (TAP output)
scripts/            e2e.sh, bench.sh, idle_memory.py
docs/               ARCHITECTURE.md
```

## License

Apache-2.0. See [LICENSE](LICENSE).
