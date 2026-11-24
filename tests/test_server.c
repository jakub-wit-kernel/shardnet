// SPDX-License-Identifier: Apache-2.0
/*
 * Integration tests: each case starts a real multi-worker server in this
 * process and talks to it over loopback sockets from test/client threads.
 */
#define _GNU_SOURCE
#define KT_IMPLEMENTATION
#include "kt.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <stdint.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "shardnet/log.h"
#include "shardnet/shardnet.h"

/* ---- server-side test handlers --------------------------------------- */

enum mode { MODE_ECHO, MODE_LINE, MODE_QUIT };

struct tctx {
	enum mode mode;
	int opens, closes;	/* updated atomically from worker threads */
};

static void h_open(sn_conn *c, void *ud)
{
	(void)c;
	__atomic_fetch_add(&((struct tctx *)ud)->opens, 1, __ATOMIC_RELAXED);
}

static void h_close(sn_conn *c, void *ud)
{
	(void)c;
	__atomic_fetch_add(&((struct tctx *)ud)->closes, 1, __ATOMIC_RELAXED);
}

static ssize_t h_data(sn_conn *c, const uint8_t *d, size_t len, void *ud)
{
	struct tctx *t = ud;

	switch (t->mode) {
	case MODE_ECHO:
		return sn_conn_send(c, d, len) == 0 ? (ssize_t)len : -1;
	case MODE_LINE: {
		/* Reply per complete line; leave a partial line unconsumed. */
		size_t used = 0;

		for (size_t i = 0; i < len; i++) {
			if (d[i] != '\n')
				continue;
			sn_conn_send(c, "ok:", 3);
			sn_conn_send(c, d + used, i + 1 - used);
			used = i + 1;
		}
		return (ssize_t)used;
	}
	case MODE_QUIT:
		if (len >= 4 && memcmp(d, "QUIT", 4) == 0) {
			sn_conn_send(c, "BYE\n", 4);
			sn_conn_close(c);
			return (ssize_t)len;
		}
		return sn_conn_send(c, d, len) == 0 ? (ssize_t)len : -1;
	}
	return -1;
}

static void h_dgram(sn_server *s, int worker, const struct sockaddr *from,
		    socklen_t fromlen, const uint8_t *d, size_t len, void *ud)
{
	(void)ud;
	sn_udp_send(s, worker, from, fromlen, d, len);
}

static const sn_handlers handlers = {
	.on_open = h_open, .on_data = h_data, .on_close = h_close,
	.on_datagram = h_dgram,
};

/* ---- helpers --------------------------------------------------------- */

static double now_ms(void)
{
	struct timespec ts;

	clock_gettime(CLOCK_MONOTONIC, &ts);
	return ts.tv_sec * 1e3 + ts.tv_nsec / 1e6;
}

static void sleep_ms(int ms)
{
	struct timespec ts = { ms / 1000, (long)(ms % 1000) * 1000000L };

	while (nanosleep(&ts, &ts) == -1 && errno == EINTR)
		;
}

static void base_config(sn_config *cfg, int workers)
{
	sn_config_init(cfg);
	cfg->host = "127.0.0.1";
	cfg->tcp_port = 0;
	cfg->workers = workers;
	cfg->max_conns_per_worker = 1024;
}

static sn_server *start(const sn_config *cfg, struct tctx *t)
{
	sn_server *s = NULL;
	int r = sn_server_create(cfg, &handlers, t, &s);

	if (r)
		KT_FAIL("sn_server_create: %s", strerror(-r));
	KT_EQ(sn_server_start(s), 0);
	return s;
}

static int tcp_connect(int port)
{
	struct sockaddr_in a = { .sin_family = AF_INET,
				 .sin_port = htons((uint16_t)port) };
	struct timeval tv = { 5, 0 };
	int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);

	inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
	if (fd < 0 || connect(fd, (struct sockaddr *)&a, sizeof(a)) < 0)
		KT_FAIL("connect: %s", strerror(errno));
	setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
	return fd;
}

static uint8_t pattern(size_t i)
{
	return (uint8_t)(i * 2654435761u >> 13);
}

/*
 * Send @len bytes of pattern() while concurrently reading the echo, so a
 * full pipe never deadlocks. Optionally half-close and wait for EOF.
 * Returns 0 on success, or a negative code describing the failure.
 */
static int exchange(int fd, size_t len, int half_close, int timeout_ms)
{
	uint8_t buf[65536];
	size_t sent = 0, got = 0;
	double deadline = now_ms() + timeout_ms;
	int fl = fcntl(fd, F_GETFL);

	fcntl(fd, F_SETFL, fl | O_NONBLOCK);
	while (got < len || half_close) {
		struct pollfd p = { .fd = fd, .events = POLLIN };

		if (sent < len)
			p.events |= POLLOUT;
		if (now_ms() > deadline)
			return -1;			/* timeout */
		if (poll(&p, 1, 100) < 0)
			return -2;
		if ((p.revents & POLLOUT) && sent < len) {
			size_t n = len - sent < sizeof(buf) ? len - sent :
							      sizeof(buf);

			for (size_t i = 0; i < n; i++)
				buf[i] = pattern(sent + i);
			ssize_t w = send(fd, buf, n, MSG_NOSIGNAL);

			if (w < 0 && errno != EAGAIN)
				return -3;
			if (w > 0)
				sent += (size_t)w;
			if (sent == len && half_close)
				shutdown(fd, SHUT_WR);
		}
		if (p.revents & (POLLIN | POLLHUP)) {
			ssize_t r = recv(fd, buf, sizeof(buf), 0);

			if (r == 0) {
				fcntl(fd, F_SETFL, fl);
				return got == len ? 0 : -4;	/* EOF */
			}
			if (r < 0 && errno != EAGAIN)
				return -5;
			for (ssize_t i = 0; i < r; i++)
				if (buf[i] != pattern(got + (size_t)i))
					return -6;		/* corrupt */
			if (r > 0)
				got += (size_t)r;
			if (got > len)
				return -7;
		}
	}
	fcntl(fd, F_SETFL, fl);
	return 0;
}

/* Expect the server to close: recv returns 0 or a reset within timeout. */
static int closed_by_peer(int fd)
{
	char c;
	ssize_t r = recv(fd, &c, 1, 0);

	return r == 0 || (r < 0 && errno == ECONNRESET);
}

static sn_stats stats_of(sn_server *s)
{
	sn_stats st;

	sn_server_stats(s, -1, &st);
	return st;
}

/* Poll the server's counters until @field reaches @want (or time out). */
#define WAIT_STAT(s, field, cmp, want)					\
	do {								\
		double end_ = now_ms() + 5000;				\
		while (!(stats_of(s).field cmp (want)) && now_ms() < end_) \
			sleep_ms(5);					\
		KT_ASSERT(stats_of(s).field cmp (want));		\
	} while (0)

/* ---- TCP ------------------------------------------------------------- */

KT_TEST(echo_small_message)
{
	struct tctx t = { MODE_ECHO, 0, 0 };
	sn_config cfg;
	char buf[16];

	base_config(&cfg, 2);
	sn_server *s = start(&cfg, &t);
	int fd = tcp_connect(sn_server_tcp_port(s));

	KT_EQ(send(fd, "hello", 5, 0), 5);
	KT_EQ(recv(fd, buf, sizeof(buf), 0), 5);
	KT_ASSERT(memcmp(buf, "hello", 5) == 0);
	close(fd);
	WAIT_STAT(s, active, ==, 0);
	KT_EQ(stats_of(s).accepted, 1);
	KT_EQ(t.opens, 1);
	sn_server_destroy(s);
	KT_EQ(t.closes, 1);
}

KT_TEST(echo_8mib_with_partial_writes)
{
	struct tctx t = { MODE_ECHO, 0, 0 };
	sn_config cfg;

	base_config(&cfg, 2);
	sn_server *s = start(&cfg, &t);
	int fd = tcp_connect(sn_server_tcp_port(s));

	KT_EQ(exchange(fd, 8u << 20, 0, 20000), 0);
	close(fd);
	WAIT_STAT(s, active, ==, 0);
	sn_stats st = stats_of(s);

	KT_EQ(st.bytes_in, 8u << 20);
	KT_EQ(st.bytes_out, 8u << 20);
	KT_EQ(st.pending_out, 0);
	sn_server_destroy(s);
}

struct client_job {
	int port, nconn;
	size_t bytes;
	int err;
};

static void *client_thread(void *arg)
{
	struct client_job *j = arg;
	int fds[64];

	for (int i = 0; i < j->nconn; i++) {
		struct sockaddr_in a = { .sin_family = AF_INET,
					 .sin_port = htons((uint16_t)j->port) };

		inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
		fds[i] = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
		if (connect(fds[i], (struct sockaddr *)&a, sizeof(a)) < 0) {
			j->err = -100;
			return NULL;
		}
	}
	for (int i = 0; i < j->nconn && !j->err; i++)
		j->err = exchange(fds[i], j->bytes, 0, 20000);
	for (int i = 0; i < j->nconn; i++)
		close(fds[i]);
	return NULL;
}

KT_TEST(many_concurrent_connections_spread_over_workers)
{
	enum { THREADS = 8, PER = 32 };
	struct tctx t = { MODE_ECHO, 0, 0 };
	struct client_job jobs[THREADS];
	pthread_t th[THREADS];
	sn_config cfg;
	int busy_workers = 0;

	base_config(&cfg, 4);
	sn_server *s = start(&cfg, &t);

	for (int i = 0; i < THREADS; i++) {
		jobs[i] = (struct client_job){ sn_server_tcp_port(s), PER,
					       64 * 1024, 0 };
		pthread_create(&th[i], NULL, client_thread, &jobs[i]);
	}
	for (int i = 0; i < THREADS; i++)
		pthread_join(th[i], NULL);
	for (int i = 0; i < THREADS; i++)
		if (jobs[i].err)
			KT_FAIL("client thread %d: error %d", i, jobs[i].err);

	WAIT_STAT(s, active, ==, 0);
	KT_EQ(stats_of(s).accepted, THREADS * PER);
	for (int w = 0; w < 4; w++) {
		sn_stats ws;

		sn_server_stats(s, w, &ws);
		busy_workers += ws.accepted > 0;
	}
	printf("  # %d of 4 workers accepted connections\n", busy_workers);
	KT_ASSERT(busy_workers >= 2);	/* SO_REUSEPORT spreads the load */
	sn_server_destroy(s);
}

KT_TEST(backpressure_pauses_reading_and_bounds_memory)
{
	struct tctx t = { MODE_ECHO, 0, 0 };
	sn_config cfg;
	int small = 16 * 1024;
	size_t written = 0;
	uint8_t buf[16384];

	base_config(&cfg, 1);
	cfg.max_pending_out = 64 * 1024;
	cfg.read_chunk = 16 * 1024;
	sn_server *s = start(&cfg, &t);

	int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
	struct sockaddr_in a = { .sin_family = AF_INET,
				 .sin_port = htons((uint16_t)sn_server_tcp_port(s)) };

	inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
	setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &small, sizeof(small));
	KT_OK(connect(fd, (struct sockaddr *)&a, sizeof(a)));
	fcntl(fd, F_SETFL, O_NONBLOCK);

	/* Write without ever reading until everything is wedged. */
	for (double end = now_ms() + 500; now_ms() < end;) {
		for (size_t i = 0; i < sizeof(buf); i++)
			buf[i] = pattern(written + i);
		ssize_t w = send(fd, buf, sizeof(buf), MSG_NOSIGNAL);

		if (w > 0)
			written += (size_t)w;
		else
			sleep_ms(10);
	}
	sn_stats st = stats_of(s);

	printf("  # wrote %zu bytes; server pending_out %llu, pauses %llu\n",
	       written, (unsigned long long)st.pending_out,
	       (unsigned long long)st.read_pauses);
	KT_ASSERT(st.read_pauses >= 1);
	/* Pending output is bounded by the threshold plus one read. */
	KT_ASSERT(st.pending_out <= cfg.max_pending_out + cfg.read_chunk);

	/* Now drain everything and verify it arrived intact and in order. */
	size_t got = 0;

	for (double end = now_ms() + 10000; got < written && now_ms() < end;) {
		ssize_t r = recv(fd, buf, sizeof(buf), 0);

		if (r <= 0) {
			sleep_ms(1);
			continue;
		}
		for (ssize_t i = 0; i < r; i++)
			if (buf[i] != pattern(got + (size_t)i))
				KT_FAIL("corrupt byte at offset %zu", got + (size_t)i);
		got += (size_t)r;
	}
	KT_EQ(got, written);
	close(fd);
	WAIT_STAT(s, pending_out, ==, 0);
	sn_server_destroy(s);
}

KT_TEST(idle_timeout_closes_only_idle_connections)
{
	struct tctx t = { MODE_ECHO, 0, 0 };
	sn_config cfg;
	char buf[8];

	base_config(&cfg, 1);
	cfg.idle_timeout_ms = 200;
	sn_server *s = start(&cfg, &t);
	int idle = tcp_connect(sn_server_tcp_port(s));
	int busy = tcp_connect(sn_server_tcp_port(s));

	for (int i = 0; i < 12; i++) {		/* ~600 ms of activity */
		KT_EQ(send(busy, "x", 1, 0), 1);
		KT_EQ(recv(busy, buf, 1, 0), 1);
		sleep_ms(50);
	}
	KT_ASSERT(closed_by_peer(idle));
	KT_EQ(send(busy, "y", 1, 0), 1);	/* busy one is still alive */
	KT_EQ(recv(busy, buf, 1, 0), 1);
	KT_EQ(stats_of(s).timeouts, 1);
	close(idle);
	close(busy);
	sn_server_destroy(s);
}

KT_TEST(connection_limit_rejects_excess)
{
	struct tctx t = { MODE_ECHO, 0, 0 };
	sn_config cfg;
	char buf[8];

	base_config(&cfg, 1);
	cfg.max_conns_per_worker = 2;
	sn_server *s = start(&cfg, &t);
	int a = tcp_connect(sn_server_tcp_port(s));
	int b = tcp_connect(sn_server_tcp_port(s));

	WAIT_STAT(s, active, ==, 2);
	int c = tcp_connect(sn_server_tcp_port(s));

	KT_ASSERT(closed_by_peer(c));
	KT_EQ(stats_of(s).rejected, 1);
	KT_EQ(send(a, "a", 1, 0), 1);
	KT_EQ(recv(a, buf, 1, 0), 1);
	KT_EQ(send(b, "b", 1, 0), 1);
	KT_EQ(recv(b, buf, 1, 0), 1);
	close(a);
	close(b);
	close(c);
	sn_server_destroy(s);
}

KT_TEST(half_close_flushes_pending_output)
{
	struct tctx t = { MODE_ECHO, 0, 0 };
	sn_config cfg;

	base_config(&cfg, 1);
	sn_server *s = start(&cfg, &t);
	int fd = tcp_connect(sn_server_tcp_port(s));

	/* After our FIN, the server must still deliver every echoed byte. */
	KT_EQ(exchange(fd, 3u << 20, 1, 20000), 0);
	close(fd);
	WAIT_STAT(s, active, ==, 0);
	sn_server_destroy(s);
}

KT_TEST(partial_input_is_buffered_until_consumed)
{
	struct tctx t = { MODE_LINE, 0, 0 };
	sn_config cfg;
	char buf[64];
	size_t got = 0;

	base_config(&cfg, 1);
	sn_server *s = start(&cfg, &t);
	int fd = tcp_connect(sn_server_tcp_port(s));

	static const char *const parts[] = { "a", "bc", "\nde", "f", "\n" };

	for (size_t i = 0; i < KT_N(parts); i++) {
		KT_EQ(send(fd, parts[i], strlen(parts[i]), 0),
		      (ssize_t)strlen(parts[i]));
		sleep_ms(30);	/* make each part a separate read */
	}
	while (got < 14) {
		ssize_t r = recv(fd, buf + got, sizeof(buf) - got, 0);

		KT_ASSERT(r > 0);
		got += (size_t)r;
	}
	KT_EQ(got, 14);
	KT_ASSERT(memcmp(buf, "ok:abc\nok:def\n", 14) == 0);
	close(fd);
	sn_server_destroy(s);
}

KT_TEST(input_limit_closes_connection)
{
	struct tctx t = { MODE_LINE, 0, 0 };
	sn_config cfg;
	char junk[4096];

	base_config(&cfg, 1);
	cfg.max_pending_in = 8192;
	sn_server *s = start(&cfg, &t);
	int fd = tcp_connect(sn_server_tcp_port(s));

	memset(junk, 'z', sizeof(junk));	/* no newline: never consumed */
	for (int i = 0; i < 4; i++)
		send(fd, junk, sizeof(junk), MSG_NOSIGNAL);
	KT_ASSERT(closed_by_peer(fd));
	close(fd);
	WAIT_STAT(s, active, ==, 0);
	sn_server_destroy(s);
}

KT_TEST(handler_close_sends_reply_first)
{
	struct tctx t = { MODE_QUIT, 0, 0 };
	sn_config cfg;
	char buf[8];

	base_config(&cfg, 1);
	sn_server *s = start(&cfg, &t);
	int fd = tcp_connect(sn_server_tcp_port(s));

	KT_EQ(send(fd, "QUIT", 4, 0), 4);
	KT_EQ(recv(fd, buf, sizeof(buf), 0), 4);
	KT_ASSERT(memcmp(buf, "BYE\n", 4) == 0);
	KT_ASSERT(closed_by_peer(fd));
	close(fd);
	sn_server_destroy(s);
}

/* ---- shutdown -------------------------------------------------------- */

KT_TEST(graceful_stop_closes_idle_clients_promptly)
{
	struct tctx t = { MODE_ECHO, 0, 0 };
	sn_config cfg;
	int fds[6];

	base_config(&cfg, 3);
	sn_server *s = start(&cfg, &t);
	for (int i = 0; i < 6; i++)
		fds[i] = tcp_connect(sn_server_tcp_port(s));
	WAIT_STAT(s, active, ==, 6);

	double t0 = now_ms();

	sn_server_stop(s, 5000);
	KT_ASSERT(now_ms() - t0 < 1000);	/* nothing to drain */
	for (int i = 0; i < 6; i++) {
		KT_ASSERT(closed_by_peer(fds[i]));
		close(fds[i]);
	}
	KT_EQ(stats_of(s).active, 0);
	KT_EQ(t.closes, 6);
	sn_server_stop(s, 0);			/* idempotent */
	sn_server_destroy(s);
}

KT_TEST(drain_deadline_forces_stuck_connections_closed)
{
	struct tctx t = { MODE_ECHO, 0, 0 };
	sn_config cfg;
	uint8_t buf[16384] = { 0 };
	int small = 16 * 1024;

	base_config(&cfg, 1);
	cfg.max_pending_out = 32 * 1024;
	sn_server *s = start(&cfg, &t);
	int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
	struct sockaddr_in a = { .sin_family = AF_INET,
				 .sin_port = htons((uint16_t)sn_server_tcp_port(s)) };

	inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
	setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &small, sizeof(small));
	KT_OK(connect(fd, (struct sockaddr *)&a, sizeof(a)));
	fcntl(fd, F_SETFL, O_NONBLOCK);
	for (double end = now_ms() + 300; now_ms() < end;)
		if (send(fd, buf, sizeof(buf), MSG_NOSIGNAL) <= 0)
			sleep_ms(10);
	KT_ASSERT(stats_of(s).pending_out > 0);	/* client never reads */

	double t0 = now_ms();

	sn_server_stop(s, 200);
	double took = now_ms() - t0;

	printf("  # stop with stuck client took %.0f ms\n", took);
	KT_ASSERT(took >= 150 && took < 2000);
	KT_EQ(stats_of(s).active, 0);
	KT_EQ(stats_of(s).pending_out, 0);
	close(fd);
	sn_server_destroy(s);
}

/* ---- UDP ------------------------------------------------------------- */

static int udp_client(int port, struct sockaddr_in *dst)
{
	struct timeval tv = { 0, 300000 };
	int fd = socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0);

	memset(dst, 0, sizeof(*dst));
	dst->sin_family = AF_INET;
	dst->sin_port = htons((uint16_t)port);
	inet_pton(AF_INET, "127.0.0.1", &dst->sin_addr);
	setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
	return fd;
}

KT_TEST(udp_echo_roundtrips)
{
	struct tctx t = { MODE_ECHO, 0, 0 };
	struct sockaddr_in dst;
	sn_config cfg;
	char out[256], in[256];
	int ok = 0;

	base_config(&cfg, 2);
	cfg.tcp_port = -1;
	cfg.udp_port = 0;
	sn_server *s = start(&cfg, &t);
	int fd = udp_client(sn_server_udp_port(s), &dst);

	for (int i = 0; i < 500; i++) {
		int n = snprintf(out, sizeof(out), "datagram-%d", i);

		KT_EQ(sendto(fd, out, (size_t)n, 0, (struct sockaddr *)&dst,
			     sizeof(dst)), n);
		ssize_t r = recv(fd, in, sizeof(in), 0);

		ok += r == n && memcmp(in, out, (size_t)n) == 0;
	}
	KT_EQ(ok, 500);
	KT_EQ(stats_of(s).udp_in, 500);
	KT_EQ(stats_of(s).udp_out, 500);
	close(fd);
	sn_server_destroy(s);
}

KT_TEST(udp_oversize_datagram_dropped)
{
	struct tctx t = { MODE_ECHO, 0, 0 };
	struct sockaddr_in dst;
	sn_config cfg;
	char big[1000] = { 0 }, in[1024];

	base_config(&cfg, 1);
	cfg.tcp_port = -1;
	cfg.udp_port = 0;
	cfg.udp_max_datagram = 512;
	sn_server *s = start(&cfg, &t);
	int fd = udp_client(sn_server_udp_port(s), &dst);

	sendto(fd, big, sizeof(big), 0, (struct sockaddr *)&dst, sizeof(dst));
	KT_ERRNO(recv(fd, in, sizeof(in), 0), EAGAIN);
	WAIT_STAT(s, udp_dropped, ==, 1);
	sendto(fd, "small", 5, 0, (struct sockaddr *)&dst, sizeof(dst));
	KT_EQ(recv(fd, in, sizeof(in), 0), 5);
	close(fd);
	sn_server_destroy(s);
}

/* ---- API validation -------------------------------------------------- */

KT_TEST(create_rejects_invalid_config)
{
	sn_config cfg;
	sn_server *s;

	base_config(&cfg, 0);
	KT_EQ(sn_server_create(&cfg, &handlers, NULL, &s), -EINVAL);
	base_config(&cfg, 1);
	cfg.tcp_port = -1;
	cfg.udp_port = -1;
	KT_EQ(sn_server_create(&cfg, &handlers, NULL, &s), -EINVAL);
	base_config(&cfg, 1);
	cfg.host = "not-an-ip";
	KT_EQ(sn_server_create(&cfg, &handlers, NULL, &s), -EINVAL);
	KT_ASSERT(s == NULL);
}

KT_TEST(port_conflict_reports_error)
{
	struct tctx t = { MODE_ECHO, 0, 0 };
	sn_config cfg;
	sn_server *s2 = NULL;

	base_config(&cfg, 1);
	sn_server *s = start(&cfg, &t);
	/* A plain socket (no SO_REUSEPORT) holding the port blocks binding. */
	int fd = socket(AF_INET, SOCK_STREAM, 0);
	struct sockaddr_in a = { .sin_family = AF_INET };

	inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
	KT_OK(bind(fd, (struct sockaddr *)&a, sizeof(a)));
	socklen_t al = sizeof(a);

	getsockname(fd, (struct sockaddr *)&a, &al);
	listen(fd, 1);
	cfg.tcp_port = ntohs(a.sin_port);
	KT_EQ(sn_server_create(&cfg, &handlers, &t, &s2), -EADDRINUSE);
	close(fd);
	sn_server_destroy(s);
}

static const struct kt_case cases[] = {
	KT_CASE(echo_small_message),
	KT_CASE(echo_8mib_with_partial_writes),
	KT_CASE(many_concurrent_connections_spread_over_workers),
	KT_CASE(backpressure_pauses_reading_and_bounds_memory),
	KT_CASE(idle_timeout_closes_only_idle_connections),
	KT_CASE(connection_limit_rejects_excess),
	KT_CASE(half_close_flushes_pending_output),
	KT_CASE(partial_input_is_buffered_until_consumed),
	KT_CASE(input_limit_closes_connection),
	KT_CASE(handler_close_sends_reply_first),
	KT_CASE(graceful_stop_closes_idle_clients_promptly),
	KT_CASE(drain_deadline_forces_stuck_connections_closed),
	KT_CASE(udp_echo_roundtrips),
	KT_CASE(udp_oversize_datagram_dropped),
	KT_CASE(create_rejects_invalid_config),
	KT_CASE(port_conflict_reports_error),
};

int main(int argc, char **argv)
{
	if (!getenv("SN_LOG_LEVEL"))
		sn_log_set_level(SN_LOG_WARN);
	return kt_run(cases, KT_N(cases), argc, argv);
}
