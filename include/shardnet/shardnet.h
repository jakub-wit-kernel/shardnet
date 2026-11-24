/* SPDX-License-Identifier: Apache-2.0 */
/*
 * shardnet - a multi-reactor TCP/UDP server engine for Linux.
 *
 * Model: N worker threads ("shards"). Each worker owns an epoll instance, its
 * own SO_REUSEPORT listening TCP socket and UDP socket, and every connection
 * it accepts. The kernel spreads new connections and datagrams across the
 * workers. A connection never migrates, so per-connection state needs no
 * locks; all callbacks for a connection run on its worker thread.
 *
 * Threading rules:
 *   - sn_conn_* and sn_udp_send must be called from the worker thread that
 *     invoked the callback (i.e. from inside a callback).
 *   - sn_server_start/stop/stats/destroy are called from a control thread.
 */
#ifndef SHARDNET_H
#define SHARDNET_H

#include <stddef.h>
#include <stdint.h>
#include <sys/socket.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct sn_server sn_server;
typedef struct sn_conn sn_conn;

typedef struct sn_handlers {
	/* A connection was accepted. Optional. */
	void (*on_open)(sn_conn *c, void *ud);
	/*
	 * Bytes arrived. @data holds every byte not yet consumed, oldest first.
	 * Return how many bytes were consumed (0..len); the rest is kept and
	 * presented again, with new data appended, on the next call. Return -1
	 * to close the connection immediately. Required for TCP.
	 */
	ssize_t (*on_data)(sn_conn *c, const uint8_t *data, size_t len,
			   void *ud);
	/*
	 * The connection is about to be closed and freed. Optional. This may
	 * run re-entrantly, inside an sn_conn_send() or sn_conn_close() called
	 * from on_data; the sn_conn stays valid until on_data returns.
	 */
	void (*on_close)(sn_conn *c, void *ud);
	/* A UDP datagram arrived on @worker. Required if udp_port is set. */
	void (*on_datagram)(sn_server *s, int worker,
			    const struct sockaddr *from, socklen_t fromlen,
			    const uint8_t *data, size_t len, void *ud);
} sn_handlers;

typedef struct sn_config {
	const char *host;		/* bind address, default "0.0.0.0" */
	int tcp_port;			/* -1 disables TCP, 0 picks a free port */
	int udp_port;			/* -1 disables UDP, 0 picks a free port */
	int workers;			/* default: number of online CPUs */
	int max_conns_per_worker;	/* default 10000 */
	size_t read_chunk;		/* bytes per read() call, default 64 KiB */
	size_t max_pending_out;		/* backpressure threshold, default 1 MiB */
	size_t max_pending_in;		/* unconsumed input limit, default 1 MiB */
	size_t udp_max_datagram;	/* larger datagrams are dropped, default 2048 */
	int idle_timeout_ms;		/* 0 disables, default 0 */
	int backlog;			/* listen backlog, default 1024 */
} sn_config;

typedef struct sn_stats {
	uint64_t accepted;	/* connections accepted */
	uint64_t rejected;	/* closed at accept: worker at max_conns */
	uint64_t closed;	/* connections closed, any reason */
	uint64_t active;	/* currently open */
	uint64_t timeouts;	/* closed by idle timeout */
	uint64_t bytes_in;
	uint64_t bytes_out;
	uint64_t read_pauses;	/* times backpressure stopped reading */
	uint64_t pending_out;	/* bytes queued for sending right now */
	uint64_t udp_in;	/* datagrams received */
	uint64_t udp_out;	/* datagrams sent */
	uint64_t udp_dropped;	/* truncated (too large) or failed sends */
} sn_stats;

/* Fill @cfg with defaults. */
void sn_config_init(sn_config *cfg);

/*
 * Create sockets and workers (not yet running). Returns 0 or -errno.
 * Ports are bound here, so sn_server_tcp_port() is valid on return.
 */
int sn_server_create(const sn_config *cfg, const sn_handlers *h, void *ud,
		     sn_server **out);
int sn_server_start(sn_server *s);

/*
 * Graceful stop: workers stop accepting, stop reading, flush pending output
 * and close every connection. After @drain_ms, remaining connections are
 * closed forcibly. Blocks until all workers have exited. Idempotent.
 */
void sn_server_stop(sn_server *s, int drain_ms);
void sn_server_destroy(sn_server *s);

int sn_server_tcp_port(const sn_server *s);
int sn_server_udp_port(const sn_server *s);
int sn_server_workers(const sn_server *s);

/* Aggregate counters over all workers (or one worker if @worker >= 0). */
void sn_server_stats(sn_server *s, int worker, sn_stats *out);

/*
 * Queue bytes for sending. Writes directly to the socket when nothing is
 * already queued; buffers only what the kernel does not take. Returns 0 or
 * -errno (-EPIPE if the connection is closing).
 */
int sn_conn_send(sn_conn *c, const void *data, size_t len);
/* Close after all queued output has been sent. */
void sn_conn_close(sn_conn *c);
int sn_conn_fd(const sn_conn *c);
int sn_conn_worker(const sn_conn *c);
uint64_t sn_conn_id(const sn_conn *c);
void *sn_conn_get_user(const sn_conn *c);
void sn_conn_set_user(sn_conn *c, void *user);

int sn_udp_send(sn_server *s, int worker, const struct sockaddr *to,
		socklen_t tolen, const void *data, size_t len);

#ifdef __cplusplus
}
#endif

#endif /* SHARDNET_H */
