// SPDX-License-Identifier: Apache-2.0
/*
 * snbench - closed-loop load generator for echo servers.
 *
 * TCP: --conns connections spread over --threads threads, each thread an
 * epoll loop. Every connection keeps --depth requests of --size bytes in
 * flight (pipelining) and measures each request's round trip.
 *
 * UDP: each connection is a connected UDP socket with one datagram in
 * flight; a reply not seen within 200 ms counts as lost.
 *
 * The last line is machine-readable:
 *   RESULT proto=tcp reqs=... rps=... mbps=... p50_us=... p99_us=... ...
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "shardnet/hist.h"

#define RING		256	/* timestamp slots per connection */
#define MAX_DEPTH	(RING - 1)
#define UDP_LOSS_NS	200000000ull

enum phase { WARMUP, MEASURE, DONE };

struct opts {
	const char *host;
	int port, threads, conns, depth, udp;
	size_t size;
	double duration, warmup;
};

struct bconn {
	int fd;
	uint64_t ts[RING];	/* send time of each in-flight request */
	unsigned head, tail;	/* head: oldest unanswered; tail: next */
	size_t wr_off;		/* bytes of the current request written */
	size_t rd_off;		/* bytes of the oldest response received */
	bool want_out;
	bool dead;
};

struct bthread {
	const struct opts *o;
	int first, n;		/* connection index range */
	pthread_t th;
	sn_hist hist;
	uint64_t done, errors, lost, bytes;
	int setup_err;
};

static int g_phase = WARMUP;	/* accessed atomically */

static uint64_t now_ns(void)
{
	struct timespec ts;

	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static int measuring(void)
{
	return __atomic_load_n(&g_phase, __ATOMIC_RELAXED) == MEASURE;
}

static int fcntl_nonblock(int fd)
{
	int fl = fcntl(fd, F_GETFL);

	if (fl < 0 || fcntl(fd, F_SETFL, fl | O_NONBLOCK) < 0) {
		close(fd);
		return -1;
	}
	return fd;
}

static int open_conn(const struct opts *o)
{
	struct sockaddr_in a = { .sin_family = AF_INET,
				 .sin_port = htons((uint16_t)o->port) };
	int one = 1;
	int fd = socket(AF_INET, (o->udp ? SOCK_DGRAM : SOCK_STREAM) |
				 SOCK_CLOEXEC, 0);

	if (fd < 0 || inet_pton(AF_INET, o->host, &a.sin_addr) != 1 ||
	    connect(fd, (struct sockaddr *)&a, sizeof(a)) < 0) {
		if (fd >= 0)
			close(fd);
		return -1;
	}
	if (!o->udp)
		setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
	/* Non-blocking only after connect so setup stays simple. */
	return fcntl_nonblock(fd);
}

/* ---- TCP ------------------------------------------------------------- */

static void tcp_fill(struct bconn *c, const struct opts *o, const uint8_t *req)
{
	while (!c->dead && c->tail - c->head < (unsigned)o->depth) {
		ssize_t n;

		if (c->wr_off == 0)
			c->ts[c->tail % RING] = now_ns();
		n = send(c->fd, req + c->wr_off, o->size - c->wr_off,
			 MSG_NOSIGNAL);
		if (n < 0) {
			if (errno == EAGAIN)
				c->want_out = true;
			else
				c->dead = true;
			return;
		}
		c->wr_off += (size_t)n;
		if (c->wr_off == o->size) {
			c->wr_off = 0;
			c->tail++;
		}
	}
	c->want_out = false;
}

static void tcp_read(struct bconn *c, struct bthread *t, uint8_t *buf,
		     size_t bufsz)
{
	ssize_t n = recv(c->fd, buf, bufsz, 0);
	uint64_t now;

	if (n <= 0) {
		if (n == 0 || errno != EAGAIN)
			c->dead = true;
		return;
	}
	now = now_ns();
	if (measuring())
		t->bytes += (size_t)n;
	c->rd_off += (size_t)n;
	while (c->rd_off >= t->o->size && c->head != c->tail) {
		c->rd_off -= t->o->size;
		if (measuring()) {
			sn_hist_record(&t->hist, now - c->ts[c->head % RING]);
			t->done++;
		}
		c->head++;
	}
}

static void update_interest(int ep, struct bconn *c)
{
	struct epoll_event ev = {
		.events = EPOLLIN | (c->want_out ? EPOLLOUT : 0),
		.data.ptr = c,
	};

	epoll_ctl(ep, EPOLL_CTL_MOD, c->fd, &ev);
}

/* ---- UDP ------------------------------------------------------------- */

static void udp_send(struct bconn *c, const struct opts *o, uint8_t *req)
{
	uint64_t ts = now_ns();

	memcpy(req, &ts, sizeof(ts));
	c->ts[0] = ts;
	if (send(c->fd, req, o->size, 0) < 0 && errno != EAGAIN)
		c->dead = true;
	c->tail = 1;	/* one in flight */
}

static void udp_read(struct bconn *c, struct bthread *t, uint8_t *buf,
		     size_t bufsz, uint8_t *req)
{
	for (;;) {
		ssize_t n = recv(c->fd, buf, bufsz, MSG_DONTWAIT);
		uint64_t sent;

		if (n < (ssize_t)sizeof(sent))
			return;
		memcpy(&sent, buf, sizeof(sent));
		if (sent != c->ts[0])
			continue;	/* late reply to a request we gave up on */
		if (measuring()) {
			sn_hist_record(&t->hist, now_ns() - sent);
			t->done++;
			t->bytes += (size_t)n;
		}
		udp_send(c, t->o, req);
	}
}

/* ---- thread ---------------------------------------------------------- */

static void *bench_thread(void *arg)
{
	struct bthread *t = arg;
	const struct opts *o = t->o;
	size_t bufsz = o->size * (size_t)o->depth + 65536;
	uint8_t *buf = malloc(bufsz), *req = malloc(o->size);
	struct bconn *cs = calloc((size_t)t->n, sizeof(*cs));
	struct epoll_event evs[256];
	int ep = epoll_create1(EPOLL_CLOEXEC);

	sn_hist_init(&t->hist);
	if (!buf || !req || !cs || ep < 0) {
		t->setup_err = ENOMEM;
		goto out;
	}
	memset(req, 'r', o->size);
	for (int i = 0; i < t->n; i++) {
		struct epoll_event ev = { .events = EPOLLIN, .data.ptr = &cs[i] };

		cs[i].fd = open_conn(o);
		if (cs[i].fd < 0) {
			t->setup_err = errno ? errno : ECONNREFUSED;
			goto out;
		}
		epoll_ctl(ep, EPOLL_CTL_ADD, cs[i].fd, &ev);
		if (o->udp)
			udp_send(&cs[i], o, req);
		else
			tcp_fill(&cs[i], o, req);
		if (cs[i].want_out)
			update_interest(ep, &cs[i]);
	}

	while (__atomic_load_n(&g_phase, __ATOMIC_RELAXED) != DONE) {
		int n = epoll_wait(ep, evs, 256, o->udp ? 10 : 100);

		for (int i = 0; i < n; i++) {
			struct bconn *c = evs[i].data.ptr;
			bool was_out = c->want_out;

			if (c->dead)
				continue;
			if (o->udp) {
				udp_read(c, t, buf, bufsz, req);
				continue;
			}
			if (evs[i].events & (EPOLLIN | EPOLLERR | EPOLLHUP))
				tcp_read(c, t, buf, bufsz);
			tcp_fill(c, o, req);
			if (c->dead) {
				t->errors++;
				epoll_ctl(ep, EPOLL_CTL_DEL, c->fd, NULL);
			} else if (was_out != c->want_out) {
				update_interest(ep, c);
			}
		}
		if (o->udp) {
			uint64_t now = now_ns();

			for (int i = 0; i < t->n; i++)
				if (now - cs[i].ts[0] > UDP_LOSS_NS) {
					if (measuring())
						t->lost++;
					udp_send(&cs[i], o, req);
				}
		}
	}
out:
	for (int i = 0; cs && i < t->n; i++)
		if (cs[i].fd > 0)
			close(cs[i].fd);
	if (ep >= 0)
		close(ep);
	free(cs);
	free(buf);
	free(req);
	return NULL;
}

/* ---- main ------------------------------------------------------------ */

static void usage(const char *argv0)
{
	fprintf(stderr,
		"usage: %s [--host IP] [--port N] [--udp] [--threads N]\n"
		"          [--conns N] [--size BYTES] [--depth N]\n"
		"          [--duration SEC] [--warmup SEC]\n", argv0);
	exit(2);
}

static void sleep_s(double s)
{
	struct timespec ts = { (time_t)s, (long)((s - (double)(time_t)s) * 1e9) };

	while (nanosleep(&ts, &ts) == -1 && errno == EINTR)
		;
}

int main(int argc, char **argv)
{
	static const struct option lo[] = {
		{ "host", required_argument, NULL, 'h' },
		{ "port", required_argument, NULL, 'p' },
		{ "udp", no_argument, NULL, 'U' },
		{ "threads", required_argument, NULL, 't' },
		{ "conns", required_argument, NULL, 'c' },
		{ "size", required_argument, NULL, 's' },
		{ "depth", required_argument, NULL, 'd' },
		{ "duration", required_argument, NULL, 'D' },
		{ "warmup", required_argument, NULL, 'W' },
		{ "help", no_argument, NULL, '?' },
		{ 0 }
	};
	struct opts o = { "127.0.0.1", 7000, 2, 32, 1, 0, 64, 5.0, 1.0 };
	struct bthread *ts;
	sn_hist all;
	uint64_t done = 0, errors = 0, lost = 0, bytes = 0;
	int opt;

	while ((opt = getopt_long(argc, argv, "", lo, NULL)) != -1) {
		switch (opt) {
		case 'h': o.host = optarg; break;
		case 'p': o.port = atoi(optarg); break;
		case 'U': o.udp = 1; break;
		case 't': o.threads = atoi(optarg); break;
		case 'c': o.conns = atoi(optarg); break;
		case 's': o.size = (size_t)atol(optarg); break;
		case 'd': o.depth = atoi(optarg); break;
		case 'D': o.duration = atof(optarg); break;
		case 'W': o.warmup = atof(optarg); break;
		default: usage(argv[0]);
		}
	}
	if (o.threads < 1 || o.conns < o.threads || o.size < 8 ||
	    o.depth < 1 || o.depth > MAX_DEPTH || o.duration <= 0 ||
	    (o.udp && o.size > 65000))
		usage(argv[0]);
	if (o.udp)
		o.depth = 1;
	signal(SIGPIPE, SIG_IGN);

	ts = calloc((size_t)o.threads, sizeof(*ts));
	for (int i = 0, first = 0; i < o.threads; i++) {
		int n = o.conns / o.threads + (i < o.conns % o.threads);

		ts[i] = (struct bthread){ .o = &o, .first = first, .n = n };
		first += n;
		pthread_create(&ts[i].th, NULL, bench_thread, &ts[i]);
	}
	sleep_s(o.warmup);
	__atomic_store_n(&g_phase, MEASURE, __ATOMIC_RELAXED);
	uint64_t t0 = now_ns();

	sleep_s(o.duration);
	__atomic_store_n(&g_phase, DONE, __ATOMIC_RELAXED);
	double secs = (double)(now_ns() - t0) / 1e9;

	sn_hist_init(&all);
	for (int i = 0; i < o.threads; i++) {
		pthread_join(ts[i].th, NULL);
		if (ts[i].setup_err) {
			fprintf(stderr, "thread %d: setup failed: %s\n", i,
				strerror(ts[i].setup_err));
			return 1;
		}
		sn_hist_merge(&all, &ts[i].hist);
		done += ts[i].done;
		errors += ts[i].errors;
		lost += ts[i].lost;
		bytes += ts[i].bytes;
	}

	printf("%s %s:%d  threads=%d conns=%d size=%zu depth=%d  %.1fs\n",
	       o.udp ? "udp" : "tcp", o.host, o.port, o.threads, o.conns,
	       o.size, o.depth, secs);
	printf("  requests   %llu  (%.0f req/s, %.1f MB/s received)\n",
	       (unsigned long long)done, (double)done / secs,
	       (double)bytes / secs / 1e6);
	printf("  latency us p50 %.1f  p90 %.1f  p99 %.1f  p99.9 %.1f  max %.1f  mean %.1f\n",
	       sn_hist_percentile(&all, 50) / 1e3,
	       sn_hist_percentile(&all, 90) / 1e3,
	       sn_hist_percentile(&all, 99) / 1e3,
	       sn_hist_percentile(&all, 99.9) / 1e3,
	       (double)(all.total ? all.max : 0) / 1e3,
	       sn_hist_mean(&all) / 1e3);
	printf("  errors %llu  lost %llu\n", (unsigned long long)errors,
	       (unsigned long long)lost);
	printf("RESULT proto=%s conns=%d size=%zu depth=%d reqs=%llu rps=%.0f "
	       "mbps=%.1f p50_us=%.1f p99_us=%.1f p999_us=%.1f errors=%llu "
	       "lost=%llu\n",
	       o.udp ? "udp" : "tcp", o.conns, o.size, o.depth,
	       (unsigned long long)done, (double)done / secs,
	       (double)bytes / secs / 1e6,
	       sn_hist_percentile(&all, 50) / 1e3,
	       sn_hist_percentile(&all, 99) / 1e3,
	       sn_hist_percentile(&all, 99.9) / 1e3,
	       (unsigned long long)errors, (unsigned long long)lost);
	free(ts);
	return errors || done == 0 ? 1 : 0;
}
