# shardnet architecture

## 1. Requirements

**Functional**

| ID | Requirement |
|----|-------------|
| F1 | Serve TCP and UDP on one or more worker threads, with a callback API (`on_open`, `on_data`, `on_close`, `on_datagram`). |
| F2 | Non-blocking sockets throughout. No callback may block the loop on I/O. |
| F3 | Stream semantics for TCP. `on_data` may consume only part of the input; the rest is kept and presented again with new data appended. |
| F4 | Connection management: admission limit, idle timeout, local close after flush, peer half-close handling. |
| F5 | Graceful shutdown: stop accepting, flush pending output, close, then force-close after a deadline. |
| F6 | Observable: per-worker counters and leveled logging. |
| F7 | A load generator that reports throughput and latency percentiles. |

**Non-functional**

| ID | Requirement |
|----|-------------|
| N1 | No locks on the per-connection hot path. |
| N2 | Bounded memory per connection under any peer behaviour, slow or malicious included. |
| N3 | Idle connections should cost close to nothing in user space. |
| N4 | Fairness: one busy connection cannot starve the others on its worker. |
| N5 | Clean under ASan, UBSan, TSan and valgrind. |

## 2. Threading model: share-nothing shards

```
                    ┌──────────── kernel: SO_REUSEPORT group (hash of 4-tuple) ─────────────┐
                    ▼                           ▼                                          ▼
 ┌─ worker 0 ─────────────────┐ ┌─ worker 1 ─────────────────┐            ┌─ worker N-1 ──────────────┐
 │ epoll fd                   │ │ epoll fd                   │            │ ...                       │
 │ listen fd (TCP, reuseport) │ │ listen fd                  │    ...     │                           │
 │ udp fd    (reuseport)      │ │ udp fd                     │            │                           │
 │ eventfd   (stop signal)    │ │ eventfd                    │            │                           │
 │ conn pool + idle LRU list  │ │ conn pool + idle LRU list  │            │                           │
 │ 64 KiB read scratch        │ │ 64 KiB read scratch        │            │                           │
 └────────────────────────────┘ └────────────────────────────┘            └───────────────────────────┘
```

Every worker has its own listening socket, all bound to the same port with
`SO_REUSEPORT`. The kernel hashes each new connection, and each datagram
flow, to one socket in the group. A connection is accepted, read, written
and closed by one thread for its whole life. That gives us:

- **No locks** on connection state, buffers, the pool or the timer list
  (N1).
- **No thundering herd** on accept. A single shared listening socket with N
  waiters wakes everyone; with `SO_REUSEPORT` each worker waits only on its
  own queue.
- **Cache locality.** A connection's data stays on one core.

The cost is that load balancing is hash-based, not load-aware, so a few
heavy connections can land on the same worker. The test suite checks that
256 connections spread over at least 2 of 4 workers. In practice all 4
received connections.

The only cross-thread data is the per-worker counters. Workers write them
with relaxed atomic adds, and `sn_server_stats()` sums relaxed loads. Those
are statistics, not synchronization, so relaxed ordering is enough.

## 3. Event loop

Each worker loops on `epoll_wait`, **level-triggered**:

1. Wait for events, with a timeout set by the nearest idle expiry or the
   drain deadline, capped at 1 s.
2. Dispatch on the handle kind stored in `epoll_event.data.ptr`:
   - listen socket: accept up to 64 connections
   - UDP socket: up to 4 × `recvmmsg` of 32 datagrams
   - eventfd: begin the drain
   - connection: flush output, then read
3. Sweep the idle list, and free the connections closed during this batch.
4. If draining: exit when no connections remain, or force-close at the
   deadline.

**Why level-triggered?** Edge-triggered mode forces a read until `EAGAIN`.
That conflicts with fairness, where we want one read per wakeup, and with
backpressure, where we deliberately stop reading. Pausing an
edge-triggered socket means remembering to re-read it manually later.
Level-triggered mode lets the interest set express intent directly. The
interest set is recomputed after every state change, and `epoll_ctl` is
only called when it actually changes:

| State | Interest |
|---|---|
| open, not paused, peer has not sent FIN | `EPOLLIN \| EPOLLRDHUP` |
| output pending | `+ EPOLLOUT` |
| paused, closing, or peer FIN received | `EPOLLOUT` only, if output is pending |

`EPOLLRDHUP` is dropped together with `EPOLLIN`. Otherwise, once a peer sent
FIN while we were paused, level-triggered epoll would report it in a busy
loop.

**Fairness (N4).** One `recv` of at most `read_chunk` bytes per readiness
event, and at most 64 accepts or 128 datagrams per event. Anything left over
is reported again on the next `epoll_wait`, after the other ready sockets
have had their turn.

### Use-after-free inside one event batch

`epoll_wait` can return events A and B. Handling A may close connection X,
and B may also refer to X. Worse, an `accept()` in between could reuse X's
fd number for a new connection. shardnet therefore never frees a
connection during a batch. `conn_destroy()` closes the fd, marks the object
`CONN_DEAD` and pushes it onto a per-worker dead list. Later events for it
see `CONN_DEAD` and are skipped. The list is returned to the pool only after
the batch. Because events carry object pointers rather than fd numbers, fd
reuse cannot misroute them.

## 4. Memory design (N2, N3)

- **Connection pool.** Each worker preallocates `max_conns_per_worker`
  connection objects in one `calloc` block, with an index-based free list.
  Alloc and free are O(1) with no `malloc` on accept. The pool size is also
  the admission limit, and pages are only touched as slots are used. LIFO
  reuse keeps recently freed objects hot in the cache.
- **Shared read scratch.** Every `recv` goes into one per-worker 64 KiB
  buffer. If `on_data` consumes everything, which is the common case,
  nothing is copied or allocated. Only unconsumed bytes are copied into the
  connection's own input buffer.
- **Write-through output.** `sn_conn_send` calls `send()` directly when
  nothing is queued. Only the part the kernel refuses is buffered.
- **Release on drain.** Input and output buffers are freed as soon as they
  empty.

The measured result (`scripts/idle_memory.py`) is about **106 bytes of
server RSS per idle connection**: 5,000 idle connections added 520 KiB, the
size of the connection objects themselves. After every connection had
echoed a message and gone idle again, RSS was unchanged. This figure counts
user-space memory only; the kernel's socket structures and buffers come on
top.

**Bounds under hostile peers.**

| Threat | Bound |
|---|---|
| Peer sends but never reads | Output above `max_pending_out` pauses reading (backpressure). Reading resumes below half that (hysteresis). Pending output ≤ `max_pending_out + read_chunk`. The test measured 72 KiB with a 64 KiB threshold, against 9 MB pushed by the client. |
| Peer sends input the handler never consumes | Input above `max_pending_in` closes the connection. |
| Too many connections | Above `max_conns_per_worker`, new connections are accepted and closed immediately (counted as `rejected`). Accepting first, rather than leaving them queued, stops level-triggered epoll from spinning on the listening socket. |
| Out of file descriptors (`EMFILE`) | Same spinning hazard. A spare fd to `/dev/null` is released, the pending connection is accepted and closed, and the spare is re-acquired. |
| Idle connection | Closed after `idle_timeout_ms`. |

## 5. Idle timeouts in O(1)

Each worker keeps its connections in an intrusive doubly-linked list in
order of last activity. Any read or send moves the connection to the tail
in O(1). The head is therefore always the connection that expires first:

- the epoll timeout is computed from the head
- the sweep pops from the head until it reaches one that has not expired

All connections share one timeout, so this list replaces a heap or a timer
wheel with no loss of precision.

## 6. Connection lifecycle

```
accept ──► OPEN ──── sn_conn_close() ────────► CLOSING ──(output flushed)──┐
            │  └──── peer FIN (peer_eof) ──(output flushed)────────────────┤
            │                                                              ▼
            └──── error / HUP / idle / input limit / drain deadline ─────► DEAD ──(batch end)──► pool
```

- **Peer half-close.** On FIN, reading stops, but queued output is still
  delivered before closing. The `half_close_flushes_pending_output` test
  sends 3 MiB, half-closes, and requires every echoed byte to arrive before
  EOF.
- **Local close** (`sn_conn_close`) also flushes before closing, so a
  handler can send `BYE` and close in one step.
- **`on_close`** runs exactly once. It can run re-entrantly when
  `sn_conn_send` fails inside `on_data`. The object stays valid until the
  batch ends, so this is safe.

## 7. Graceful shutdown

`sn_server_stop(s, drain_ms)` writes to every worker's `eventfd`, then joins
the threads. On the wake-up, each worker:

1. Closes its listening and UDP sockets. Connections still waiting in this
   worker's accept queue are reset.
2. Calls `sn_conn_close()` on every connection. Idle ones close at once;
   ones with pending output switch to `CLOSING` and flush.
3. Exits when no connections remain, or at the deadline, when it
   force-closes the rest.

The tests check both ends. Six idle clients made `stop` return almost
immediately. A client that never reads made `stop(200)` return after 201 ms,
with all connections closed.

`snserver` blocks `SIGINT` and `SIGTERM` in every thread, and its main
thread receives them synchronously with `sigwait`. Workers inherit a fully
blocked signal mask, so no signal handler can ever interrupt the loop.

## 8. UDP

Each worker owns a `SO_REUSEPORT` UDP socket. It reads up to 32 datagrams
per `recvmmsg` call into one preallocated slab (32 × `udp_max_datagram`).
Datagrams flagged `MSG_TRUNC` are counted in `udp_dropped` and never passed
to the handler. Replies go out with `sendto` from the same worker, and a
failure there also counts as a drop. UDP has no flow control; dropping is
the protocol's own answer to overload.

## 9. Load generator (`snbench`)

- **Closed loop.** Each connection keeps `depth` requests in flight, which
  is pipelining. Send times sit in a per-connection ring; replies are
  matched by byte count, because echo responses arrive in order.
- **No allocation while measuring.** Latencies go into a log-linear
  histogram (`sn_hist`): exact below 32 ns, 32 sub-buckets per power of two
  above, so relative error stays under about 3%. Per-thread histograms are
  merged at the end.
- **Warmup.** The warmup period is excluded from the results.
- **Coordinated omission.** Closed-loop generators slow down when the
  server slows down, so they under-report tail latency compared with an
  open-loop (fixed-rate) generator. Reported percentiles are for this
  closed-loop load. An open-loop mode is listed under future work.

## 10. Limitations and future work

- **IPv4 only in `snbench`.** The library binds IPv6 addresses too.
- **No TLS.** It would sit naturally in `on_data`/`sn_conn_send`, but is
  out of scope.
- **No `writev`/zero-copy send.** `MSG_ZEROCOPY` and `io_uring` are
  natural next steps for large payloads.
- **Hash-based balancing.** `SO_REUSEPORT` with an attached BPF program
  could steer connections by worker load.
- **Open-loop load mode** for `snbench`, to measure without coordinated
  omission.
