// SPDX-License-Identifier: Apache-2.0
/*
 * shardnet server: one epoll reactor per worker thread.
 * See docs/ARCHITECTURE.md for the design rationale.
 */
#define _GNU_SOURCE
#include "shardnet/shardnet.h"
#include "shardnet/log.h"

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <time.h>
#include <unistd.h>

#include "buf.h"
#include "pool.h"

#define MAX_EVENTS	256
#define ACCEPT_BATCH	64	/* accepts per readiness event (fairness) */
#define UDP_BATCH	32	/* datagrams per recvmmsg() */
#define UDP_ROUNDS	4	/* recvmmsg() calls per readiness event */
#define TICK_MS		1000	/* longest epoll_wait when timers exist */

enum handle_kind { H_LISTEN = 1, H_UDP, H_WAKE, H_CONN };

/* First member of anything registered with epoll; data.ptr points here. */
struct handle {
	enum handle_kind kind;
};

enum conn_state {
	CONN_OPEN,	/* normal operation */
	CONN_CLOSING,	/* close requested: no reads, flush output then close */
	CONN_DEAD,	/* closed; memory parked until the end of this batch */
};

struct sn_conn {
	struct handle h;		/* must be first */
	int fd;
	enum conn_state state;
	uint32_t events;		/* current epoll interest set */
	bool read_paused;		/* backpressure: output over limit */
	bool peer_eof;			/* peer sent FIN */
	uint64_t id;
	int64_t last_active_ms;
	struct worker *w;
	sn_buf in;			/* bytes on_data has not consumed */
	sn_buf out;			/* bytes the kernel has not accepted */
	sn_conn *lru_prev, *lru_next;	/* activity order, oldest first */
	sn_conn *dead_next;
	void *user;
};

struct worker_stats {
	uint64_t accepted, rejected, closed, active, timeouts;
	uint64_t bytes_in, bytes_out, read_pauses, pending_out;
	uint64_t udp_in, udp_out, udp_dropped;
};

struct worker {
	sn_server *s;
	int idx;
	pthread_t thread;
	bool thread_started;
	int epfd, lfd, ufd, wakefd, spare_fd;
	struct handle h_listen, h_udp, h_wake;
	sn_pool pool;
	sn_conn *lru_head, *lru_tail;
	sn_conn *dead;
	uint8_t *scratch;		/* read_chunk bytes, reused for every read */
	uint8_t *udp_bufs;		/* UDP_BATCH * udp_max_datagram */
	bool draining;
	int64_t drain_deadline_ms;
	uint64_t next_id;
	struct worker_stats st;		/* written by this worker, read by any */
};

struct sn_server {
	sn_config cfg;
	sn_handlers h;
	void *ud;
	int nworkers;
	struct worker *w;
	int tcp_port, udp_port;
	bool started, stopped;
	int drain_ms;
	pthread_mutex_t ctl_lock;
};

/* ---- small helpers ---------------------------------------------------- */

#define STAT_ADD(w, field, n) \
	__atomic_fetch_add(&(w)->st.field, (uint64_t)(n), __ATOMIC_RELAXED)
#define STAT_SUB(w, field, n) \
	__atomic_fetch_sub(&(w)->st.field, (uint64_t)(n), __ATOMIC_RELAXED)

static int64_t now_ms(void)
{
	struct timespec ts;

	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static void lru_unlink(struct worker *w, sn_conn *c)
{
	if (c->lru_prev)
		c->lru_prev->lru_next = c->lru_next;
	else
		w->lru_head = c->lru_next;
	if (c->lru_next)
		c->lru_next->lru_prev = c->lru_prev;
	else
		w->lru_tail = c->lru_prev;
	c->lru_prev = c->lru_next = NULL;
}

static void lru_append(struct worker *w, sn_conn *c)
{
	c->lru_prev = w->lru_tail;
	c->lru_next = NULL;
	if (w->lru_tail)
		w->lru_tail->lru_next = c;
	else
		w->lru_head = c;
	w->lru_tail = c;
}

/* Mark activity: O(1) move to the tail of the idle list. */
static void conn_touch(sn_conn *c, int64_t now)
{
	struct worker *w = c->w;

	c->last_active_ms = now;
	if (w->lru_tail != c) {
		lru_unlink(w, c);
		lru_append(w, c);
	}
}

/* ---- connection lifecycle -------------------------------------------- */

static void conn_destroy(sn_conn *c, const char *why)
{
	struct worker *w = c->w;

	if (c->state == CONN_DEAD)
		return;
	if (w->s->h.on_close)
		w->s->h.on_close(c, w->s->ud);
	sn_debug("conn %llx closed (%s)", (unsigned long long)c->id, why);
	/* close() drops the fd from the epoll set. */
	close(c->fd);
	c->fd = -1;
	c->state = CONN_DEAD;
	lru_unlink(w, c);
	STAT_SUB(w, pending_out, c->out.len);
	sn_buf_free(&c->in);
	sn_buf_free(&c->out);
	STAT_ADD(w, closed, 1);
	STAT_SUB(w, active, 1);
	/*
	 * Do not return the object to the pool yet: later events in the same
	 * epoll_wait() batch may still point at it. reap_dead() frees it once
	 * the batch is done, so stale events see CONN_DEAD and are skipped.
	 */
	c->dead_next = w->dead;
	w->dead = c;
}

static void reap_dead(struct worker *w)
{
	while (w->dead) {
		sn_conn *c = w->dead;

		w->dead = c->dead_next;
		sn_pool_free(&w->pool, c);
	}
}

/* Bring the epoll interest set in line with the connection state. */
static void conn_update_events(sn_conn *c)
{
	uint32_t want = 0;

	if (c->state == CONN_DEAD)
		return;
	/*
	 * Level-triggered. EPOLLRDHUP goes with EPOLLIN: while reading is
	 * paused we must not ask for it, or a peer FIN would wake us forever.
	 */
	if (c->state == CONN_OPEN && !c->read_paused && !c->peer_eof)
		want |= EPOLLIN | EPOLLRDHUP;
	if (c->out.len)
		want |= EPOLLOUT;
	if (want == c->events)
		return;

	struct epoll_event ev = { .events = want, .data.ptr = &c->h };

	if (epoll_ctl(c->w->epfd, EPOLL_CTL_MOD, c->fd, &ev) < 0) {
		sn_err("epoll_ctl MOD conn %llx: %s",
		       (unsigned long long)c->id, strerror(errno));
		conn_destroy(c, "epoll_ctl");
		return;
	}
	c->events = want;
}

/* Close now if nothing more can happen on this connection. */
static void conn_maybe_finish(sn_conn *c)
{
	if (c->state == CONN_DEAD || c->out.len)
		return;
	if (c->state == CONN_CLOSING || c->peer_eof)
		conn_destroy(c, c->peer_eof ? "peer eof" : "local close");
}

static void conn_flush(sn_conn *c)
{
	struct worker *w = c->w;
	size_t max_out = w->s->cfg.max_pending_out;

	while (c->out.len) {
		ssize_t n = send(c->fd, sn_buf_head(&c->out), c->out.len,
				 MSG_NOSIGNAL | MSG_DONTWAIT);

		if (n < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK)
				break;
			if (errno == EINTR)
				continue;
			conn_destroy(c, "send error");
			return;
		}
		sn_buf_consume(&c->out, (size_t)n);
		STAT_ADD(w, bytes_out, n);
		STAT_SUB(w, pending_out, n);
	}
	/* Hysteresis: resume reading once half the backlog has drained. */
	if (c->read_paused && c->out.len <= max_out / 2)
		c->read_paused = false;
	if (c->out.len == 0)
		sn_buf_free(&c->out);	/* idle connections hold no buffers */
	conn_maybe_finish(c);
	conn_update_events(c);
}

static void conn_read(sn_conn *c, int64_t now)
{
	struct worker *w = c->w;
	const sn_config *cfg = &w->s->cfg;
	const uint8_t *data;
	size_t len;
	ssize_t n, used;

	/* One read per readiness event so busy peers cannot starve others. */
	n = recv(c->fd, w->scratch, cfg->read_chunk, MSG_DONTWAIT);
	if (n == 0) {
		c->peer_eof = true;
		conn_maybe_finish(c);
		conn_update_events(c);
		return;
	}
	if (n < 0) {
		if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)
			conn_destroy(c, "recv error");
		return;
	}
	STAT_ADD(w, bytes_in, n);
	conn_touch(c, now);

	/* Fast path: nothing buffered, hand the scratch buffer over directly. */
	if (c->in.len == 0) {
		data = w->scratch;
		len = (size_t)n;
	} else {
		if (sn_buf_append(&c->in, w->scratch, (size_t)n) < 0) {
			conn_destroy(c, "out of memory");
			return;
		}
		data = sn_buf_head(&c->in);
		len = c->in.len;
	}

	used = w->s->h.on_data(c, data, len, w->s->ud);
	if (c->state == CONN_DEAD)
		return;
	if (used < 0 || (size_t)used > len) {
		if (used > 0)
			sn_err("on_data consumed %zd of %zu bytes", used, len);
		conn_destroy(c, "handler");
		return;
	}

	if (data == w->scratch) {
		if ((size_t)used < len &&
		    sn_buf_append(&c->in, data + used, len - (size_t)used) < 0) {
			conn_destroy(c, "out of memory");
			return;
		}
	} else {
		sn_buf_consume(&c->in, (size_t)used);
	}
	if (c->in.len == 0)
		sn_buf_free(&c->in);
	else if (c->in.len > cfg->max_pending_in) {
		sn_warn("conn %llx: %zu unconsumed input bytes, closing",
			(unsigned long long)c->id, c->in.len);
		conn_destroy(c, "input limit");
		return;
	}
	conn_update_events(c);
}

static void conn_event(sn_conn *c, uint32_t ev, int64_t now)
{
	if (c->state == CONN_DEAD)
		return;		/* stale event: closed earlier in this batch */
	if (ev & (EPOLLERR | EPOLLHUP)) {
		conn_destroy(c, ev & EPOLLERR ? "socket error" : "hangup");
		return;
	}
	if (ev & EPOLLOUT) {
		conn_flush(c);
		if (c->state == CONN_DEAD)
			return;
	}
	if ((ev & (EPOLLIN | EPOLLRDHUP)) && (c->events & EPOLLIN))
		conn_read(c, now);
}

/* ---- public connection API (worker thread only) ---------------------- */

int sn_conn_send(sn_conn *c, const void *data, size_t len)
{
	struct worker *w = c->w;
	size_t sent = 0;

	if (c->state != CONN_OPEN)
		return -EPIPE;
	if (len == 0)
		return 0;

	if (c->out.len == 0) {
		ssize_t n = send(c->fd, data, len, MSG_NOSIGNAL | MSG_DONTWAIT);

		if (n < 0) {
			int err = errno;

			if (err != EAGAIN && err != EWOULDBLOCK && err != EINTR) {
				conn_destroy(c, "send error");
				return -err;
			}
			n = 0;
		}
		sent = (size_t)n;
		STAT_ADD(w, bytes_out, n);
		conn_touch(c, now_ms());
	}
	if (sent < len) {
		if (sn_buf_append(&c->out, (const uint8_t *)data + sent,
				  len - sent) < 0) {
			conn_destroy(c, "out of memory");
			return -ENOMEM;
		}
		STAT_ADD(w, pending_out, len - sent);
		if (!c->read_paused && c->out.len > w->s->cfg.max_pending_out) {
			c->read_paused = true;
			STAT_ADD(w, read_pauses, 1);
		}
		conn_update_events(c);
	}
	return 0;
}

void sn_conn_close(sn_conn *c)
{
	if (c->state != CONN_OPEN)
		return;
	c->state = CONN_CLOSING;
	conn_maybe_finish(c);
	conn_update_events(c);
}

int sn_conn_fd(const sn_conn *c) { return c->fd; }
int sn_conn_worker(const sn_conn *c) { return c->w->idx; }
uint64_t sn_conn_id(const sn_conn *c) { return c->id; }
void *sn_conn_get_user(const sn_conn *c) { return c->user; }
void sn_conn_set_user(sn_conn *c, void *user) { c->user = user; }

/* ---- accepting --------------------------------------------------------- */

/*
 * Out of file descriptors: the pending connection stays in the accept queue
 * and level-triggered epoll would report it forever. Free the spare fd,
 * accept and immediately close the connection, then re-reserve the spare.
 */
static void shed_on_emfile(struct worker *w)
{
	if (w->spare_fd < 0)
		return;
	close(w->spare_fd);
	int fd = accept4(w->lfd, NULL, NULL, SOCK_CLOEXEC);

	if (fd >= 0) {
		close(fd);
		STAT_ADD(w, rejected, 1);
	}
	w->spare_fd = open("/dev/null", O_RDONLY | O_CLOEXEC);
}

static void accept_ready(struct worker *w, int64_t now)
{
	const sn_server *s = w->s;

	for (int i = 0; i < ACCEPT_BATCH; i++) {
		int fd = accept4(w->lfd, NULL, NULL,
				 SOCK_NONBLOCK | SOCK_CLOEXEC);
		sn_conn *c;

		if (fd < 0) {
			if (errno == EINTR || errno == ECONNABORTED)
				continue;
			if (errno == EMFILE || errno == ENFILE) {
				sn_warn("accept: %s; shedding connection",
					strerror(errno));
				shed_on_emfile(w);
			} else if (errno != EAGAIN && errno != EWOULDBLOCK) {
				sn_err("accept: %s", strerror(errno));
			}
			return;
		}

		c = w->draining ? NULL : sn_pool_alloc(&w->pool);
		if (!c) {
			close(fd);
			STAT_ADD(w, rejected, 1);
			continue;
		}

		int one = 1;

		setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
		c->h.kind = H_CONN;
		c->fd = fd;
		c->state = CONN_OPEN;
		c->w = w;
		c->id = ((uint64_t)w->idx << 48) | ++w->next_id;
		c->last_active_ms = now;
		c->events = EPOLLIN | EPOLLRDHUP;

		struct epoll_event ev = { .events = c->events,
					  .data.ptr = &c->h };

		if (epoll_ctl(w->epfd, EPOLL_CTL_ADD, fd, &ev) < 0) {
			sn_err("epoll_ctl ADD: %s", strerror(errno));
			close(fd);
			sn_pool_free(&w->pool, c);
			continue;
		}
		lru_append(w, c);
		STAT_ADD(w, accepted, 1);
		STAT_ADD(w, active, 1);
		sn_debug("conn %llx open fd=%d", (unsigned long long)c->id, fd);
		if (s->h.on_open)
			s->h.on_open(c, s->ud);
	}
}

/* ---- UDP --------------------------------------------------------------- */

static void udp_ready(struct worker *w)
{
	const sn_server *s = w->s;
	size_t slot = s->cfg.udp_max_datagram;
	struct mmsghdr msgs[UDP_BATCH];
	struct iovec iov[UDP_BATCH];
	struct sockaddr_storage from[UDP_BATCH];

	for (int round = 0; round < UDP_ROUNDS; round++) {
		int n;

		for (int i = 0; i < UDP_BATCH; i++) {
			iov[i].iov_base = w->udp_bufs + (size_t)i * slot;
			iov[i].iov_len = slot;
			memset(&msgs[i].msg_hdr, 0, sizeof(msgs[i].msg_hdr));
			msgs[i].msg_hdr.msg_iov = &iov[i];
			msgs[i].msg_hdr.msg_iovlen = 1;
			msgs[i].msg_hdr.msg_name = &from[i];
			msgs[i].msg_hdr.msg_namelen = sizeof(from[i]);
		}
		n = recvmmsg(w->ufd, msgs, UDP_BATCH, MSG_DONTWAIT, NULL);
		if (n < 0) {
			if (errno != EAGAIN && errno != EWOULDBLOCK &&
			    errno != EINTR)
				sn_err("recvmmsg: %s", strerror(errno));
			return;
		}
		for (int i = 0; i < n; i++) {
			if (msgs[i].msg_hdr.msg_flags & MSG_TRUNC) {
				STAT_ADD(w, udp_dropped, 1);
				continue;
			}
			STAT_ADD(w, udp_in, 1);
			s->h.on_datagram(w->s, w->idx,
					 (struct sockaddr *)&from[i],
					 msgs[i].msg_hdr.msg_namelen,
					 iov[i].iov_base, msgs[i].msg_len,
					 s->ud);
		}
		if (n < UDP_BATCH)
			return;
	}
}

int sn_udp_send(sn_server *s, int worker, const struct sockaddr *to,
		socklen_t tolen, const void *data, size_t len)
{
	struct worker *w;

	if (worker < 0 || worker >= s->nworkers || s->w[worker].ufd < 0)
		return -EINVAL;
	w = &s->w[worker];
	if (sendto(w->ufd, data, len, MSG_DONTWAIT | MSG_NOSIGNAL, to,
		   tolen) < 0) {
		STAT_ADD(w, udp_dropped, 1);
		return -errno;
	}
	STAT_ADD(w, udp_out, 1);
	return 0;
}

/* ---- worker loop ------------------------------------------------------- */

static void begin_drain(struct worker *w, int64_t now)
{
	sn_conn *c, *next;

	w->draining = true;
	w->drain_deadline_ms = now + __atomic_load_n(&w->s->drain_ms,
						     __ATOMIC_RELAXED);
	/* Stop taking new work; peers in our accept queue get a reset. */
	if (w->lfd >= 0) {
		close(w->lfd);
		w->lfd = -1;
	}
	if (w->ufd >= 0) {
		close(w->ufd);
		w->ufd = -1;
	}
	for (c = w->lru_head; c; c = next) {
		next = c->lru_next;
		sn_conn_close(c);	/* flushes first if output is pending */
	}
	sn_info("draining: %u connections left", w->pool.in_use);
}

/* epoll_wait timeout: the nearest idle expiry or drain deadline, capped. */
static int next_timeout_ms(const struct worker *w, int64_t now)
{
	int idle = w->s->cfg.idle_timeout_ms;
	bool have = false;
	int64_t t = 0;

	if (idle > 0 && w->lru_head) {
		t = w->lru_head->last_active_ms + idle - now;
		have = true;
	}
	if (w->draining && (!have || w->drain_deadline_ms - now < t)) {
		t = w->drain_deadline_ms - now;
		have = true;
	}
	if (!have)
		return -1;	/* nothing timed: sleep until an event */
	if (t < 0)
		t = 0;
	return (int)(t > TICK_MS ? TICK_MS : t);
}

static void sweep_idle(struct worker *w, int64_t now)
{
	int idle = w->s->cfg.idle_timeout_ms;

	while (w->lru_head && now - w->lru_head->last_active_ms >= idle) {
		STAT_ADD(w, timeouts, 1);
		conn_destroy(w->lru_head, "idle timeout");
	}
}

static void *worker_main(void *arg)
{
	struct worker *w = arg;
	struct epoll_event evs[MAX_EVENTS];
	char tag[16];

	snprintf(tag, sizeof(tag), "w%d", w->idx);
	sn_log_set_thread_tag(tag);
	pthread_setname_np(pthread_self(), tag);
	sn_debug("worker started");

	for (;;) {
		int64_t now = now_ms();
		int n = epoll_wait(w->epfd, evs, MAX_EVENTS,
				   next_timeout_ms(w, now));

		if (n < 0) {
			if (errno == EINTR)
				continue;
			sn_err("epoll_wait: %s", strerror(errno));
			break;
		}
		now = now_ms();
		for (int i = 0; i < n; i++) {
			struct handle *h = evs[i].data.ptr;

			switch (h->kind) {
			case H_LISTEN:
				if (w->lfd >= 0)
					accept_ready(w, now);
				break;
			case H_UDP:
				if (w->ufd >= 0)
					udp_ready(w);
				break;
			case H_WAKE: {
				uint64_t v;

				(void)!read(w->wakefd, &v, sizeof(v));
				if (!w->draining)
					begin_drain(w, now);
				break;
			}
			case H_CONN:
				conn_event((sn_conn *)h, evs[i].events, now);
				break;
			}
		}
		if (w->s->cfg.idle_timeout_ms > 0)
			sweep_idle(w, now_ms());
		reap_dead(w);

		if (w->draining) {
			if (w->pool.in_use == 0)
				break;
			if (now_ms() >= w->drain_deadline_ms) {
				sn_warn("drain timeout: forcing %u connections closed",
					w->pool.in_use);
				while (w->lru_head)
					conn_destroy(w->lru_head, "drain timeout");
				reap_dead(w);
				break;
			}
		}
	}
	sn_debug("worker exiting");
	return NULL;
}

/* ---- setup and teardown ------------------------------------------------ */

void sn_config_init(sn_config *cfg)
{
	long cpus = sysconf(_SC_NPROCESSORS_ONLN);

	memset(cfg, 0, sizeof(*cfg));
	cfg->host = "0.0.0.0";
	cfg->tcp_port = 0;
	cfg->udp_port = -1;
	cfg->workers = cpus > 0 ? (int)cpus : 1;
	cfg->max_conns_per_worker = 10000;
	cfg->read_chunk = 64 * 1024;
	cfg->max_pending_out = 1024 * 1024;
	cfg->max_pending_in = 1024 * 1024;
	cfg->udp_max_datagram = 2048;
	cfg->idle_timeout_ms = 0;
	cfg->backlog = 1024;
}

/* Create a bound SO_REUSEPORT socket. *port 0 is replaced by the real port. */
static int bind_socket(const char *host, int type, int *port, int backlog)
{
	struct addrinfo hints = { .ai_flags = AI_PASSIVE | AI_NUMERICHOST |
					      AI_NUMERICSERV,
				  .ai_socktype = type };
	struct addrinfo *ai = NULL;
	struct sockaddr_storage ss;
	socklen_t sslen = sizeof(ss);
	char portstr[8];
	int fd, one = 1, err;

	snprintf(portstr, sizeof(portstr), "%d", *port);
	err = getaddrinfo(host, portstr, &hints, &ai);
	if (err) {
		sn_err("bad bind address %s: %s", host, gai_strerror(err));
		return -EINVAL;
	}
	fd = socket(ai->ai_family, type | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
	if (fd < 0)
		goto fail;
	if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0 ||
	    setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one)) < 0 ||
	    bind(fd, ai->ai_addr, ai->ai_addrlen) < 0 ||
	    (type == SOCK_STREAM && listen(fd, backlog) < 0) ||
	    getsockname(fd, (struct sockaddr *)&ss, &sslen) < 0)
		goto fail;
	freeaddrinfo(ai);
	*port = ntohs(ss.ss_family == AF_INET6 ?
		      ((struct sockaddr_in6 *)&ss)->sin6_port :
		      ((struct sockaddr_in *)&ss)->sin_port);
	return fd;
fail:
	err = -errno;
	sn_err("%s socket on %s:%s: %s", type == SOCK_STREAM ? "tcp" : "udp",
	       host, portstr, strerror(errno));
	if (fd >= 0)
		close(fd);
	freeaddrinfo(ai);
	return err;
}

static int epoll_add(int epfd, int fd, struct handle *h)
{
	struct epoll_event ev = { .events = EPOLLIN, .data.ptr = h };

	return epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev) < 0 ? -errno : 0;
}

static int worker_init(sn_server *s, struct worker *w, int idx)
{
	const sn_config *cfg = &s->cfg;
	int ret;

	w->s = s;
	w->idx = idx;
	w->h_listen.kind = H_LISTEN;
	w->h_udp.kind = H_UDP;
	w->h_wake.kind = H_WAKE;

	w->epfd = epoll_create1(EPOLL_CLOEXEC);
	w->wakefd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
	w->spare_fd = open("/dev/null", O_RDONLY | O_CLOEXEC);
	if (w->epfd < 0 || w->wakefd < 0)
		return -errno;
	if ((ret = epoll_add(w->epfd, w->wakefd, &w->h_wake)))
		return ret;

	if (cfg->tcp_port >= 0) {
		w->lfd = bind_socket(cfg->host, SOCK_STREAM, &s->tcp_port,
				     cfg->backlog);
		if (w->lfd < 0)
			return w->lfd;
		if ((ret = epoll_add(w->epfd, w->lfd, &w->h_listen)))
			return ret;
	}
	if (cfg->udp_port >= 0) {
		w->ufd = bind_socket(cfg->host, SOCK_DGRAM, &s->udp_port, 0);
		if (w->ufd < 0)
			return w->ufd;
		if ((ret = epoll_add(w->epfd, w->ufd, &w->h_udp)))
			return ret;
		w->udp_bufs = malloc(UDP_BATCH * cfg->udp_max_datagram);
		if (!w->udp_bufs)
			return -ENOMEM;
	}

	ret = sn_pool_init(&w->pool, sizeof(sn_conn),
			   (uint32_t)cfg->max_conns_per_worker);
	if (ret)
		return ret;
	w->scratch = malloc(cfg->read_chunk);
	return w->scratch ? 0 : -ENOMEM;
}

static void worker_free(struct worker *w)
{
	while (w->lru_head) {
		sn_conn *c = w->lru_head;

		lru_unlink(w, c);
		if (c->fd >= 0)
			close(c->fd);
		sn_buf_free(&c->in);
		sn_buf_free(&c->out);
	}
	int fds[] = { w->epfd, w->lfd, w->ufd, w->wakefd, w->spare_fd };

	for (size_t i = 0; i < sizeof(fds) / sizeof(fds[0]); i++)
		if (fds[i] >= 0)
			close(fds[i]);
	sn_pool_destroy(&w->pool);
	free(w->scratch);
	free(w->udp_bufs);
}

int sn_server_create(const sn_config *cfg, const sn_handlers *h, void *ud,
		     sn_server **out)
{
	sn_server *s;
	int ret = 0;

	*out = NULL;
	if (!cfg || !h || cfg->workers < 1 || cfg->workers > 1024 ||
	    cfg->max_conns_per_worker < 1 || cfg->read_chunk == 0 ||
	    cfg->max_pending_out == 0 || cfg->idle_timeout_ms < 0 ||
	    cfg->tcp_port > 65535 || cfg->udp_port > 65535 ||
	    (cfg->tcp_port < 0 && cfg->udp_port < 0) ||
	    (cfg->tcp_port >= 0 && !h->on_data) ||
	    (cfg->udp_port >= 0 && (!h->on_datagram ||
				    cfg->udp_max_datagram == 0)))
		return -EINVAL;

	s = calloc(1, sizeof(*s));
	if (!s)
		return -ENOMEM;
	s->cfg = *cfg;
	if (!s->cfg.host)
		s->cfg.host = "0.0.0.0";
	s->h = *h;
	s->ud = ud;
	s->nworkers = cfg->workers;
	s->tcp_port = cfg->tcp_port;
	s->udp_port = cfg->udp_port;
	pthread_mutex_init(&s->ctl_lock, NULL);
	s->w = calloc((size_t)s->nworkers, sizeof(*s->w));
	if (!s->w) {
		free(s);
		return -ENOMEM;
	}
	for (int i = 0; i < s->nworkers; i++)
		s->w[i].epfd = s->w[i].lfd = s->w[i].ufd = s->w[i].wakefd =
			s->w[i].spare_fd = -1;

	/* Worker 0 binds first; with port 0 it picks the port the rest share. */
	for (int i = 0; i < s->nworkers && !ret; i++)
		ret = worker_init(s, &s->w[i], i);
	if (ret) {
		sn_server_destroy(s);
		return ret;
	}
	*out = s;
	return 0;
}

int sn_server_start(sn_server *s)
{
	sigset_t all, old;
	int ret = 0;

	pthread_mutex_lock(&s->ctl_lock);
	if (s->started) {
		pthread_mutex_unlock(&s->ctl_lock);
		return -EALREADY;
	}
	/* Workers block all signals; the application handles them. */
	sigfillset(&all);
	pthread_sigmask(SIG_BLOCK, &all, &old);
	for (int i = 0; i < s->nworkers; i++) {
		ret = -pthread_create(&s->w[i].thread, NULL, worker_main,
				      &s->w[i]);
		if (ret)
			break;
		s->w[i].thread_started = true;
	}
	pthread_sigmask(SIG_SETMASK, &old, NULL);
	s->started = true;
	pthread_mutex_unlock(&s->ctl_lock);
	if (ret) {
		sn_server_stop(s, 0);
		return ret;
	}
	sn_info("started %d workers, tcp port %d, udp port %d", s->nworkers,
		s->tcp_port, s->udp_port);
	return 0;
}

void sn_server_stop(sn_server *s, int drain_ms)
{
	uint64_t one = 1;

	pthread_mutex_lock(&s->ctl_lock);
	if (s->stopped || !s->started) {
		s->stopped = true;
		pthread_mutex_unlock(&s->ctl_lock);
		return;
	}
	s->stopped = true;
	__atomic_store_n(&s->drain_ms, drain_ms < 0 ? 0 : drain_ms,
			 __ATOMIC_RELAXED);
	for (int i = 0; i < s->nworkers; i++)
		if (s->w[i].thread_started)
			(void)!write(s->w[i].wakefd, &one, sizeof(one));
	for (int i = 0; i < s->nworkers; i++)
		if (s->w[i].thread_started)
			pthread_join(s->w[i].thread, NULL);
	pthread_mutex_unlock(&s->ctl_lock);
	sn_info("stopped");
}

void sn_server_destroy(sn_server *s)
{
	if (!s)
		return;
	sn_server_stop(s, 0);
	for (int i = 0; i < s->nworkers; i++)
		worker_free(&s->w[i]);
	pthread_mutex_destroy(&s->ctl_lock);
	free(s->w);
	free(s);
}

int sn_server_tcp_port(const sn_server *s) { return s->tcp_port; }
int sn_server_udp_port(const sn_server *s) { return s->udp_port; }
int sn_server_workers(const sn_server *s) { return s->nworkers; }

void sn_server_stats(sn_server *s, int worker, sn_stats *out)
{
	memset(out, 0, sizeof(*out));
	for (int i = 0; i < s->nworkers; i++) {
		const struct worker_stats *st = &s->w[i].st;

		if (worker >= 0 && i != worker)
			continue;
#define SUM(f) out->f += __atomic_load_n(&st->f, __ATOMIC_RELAXED)
		SUM(accepted); SUM(rejected); SUM(closed); SUM(active);
		SUM(timeouts); SUM(bytes_in); SUM(bytes_out);
		SUM(read_pauses); SUM(pending_out); SUM(udp_in);
		SUM(udp_out); SUM(udp_dropped);
#undef SUM
	}
}
