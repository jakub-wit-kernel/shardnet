// SPDX-License-Identifier: Apache-2.0
/*
 * snserver - TCP + UDP echo server built on shardnet.
 *
 *   snserver [--host ADDR] [--tcp-port N] [--udp-port N] [--workers N]
 *            [--max-conns N] [--idle-ms N] [--stats-sec N] [--drain-ms N]
 *
 * Prints "listening tcp=<port> udp=<port>" once ready (for scripts), then
 * runs until SIGINT/SIGTERM, when it drains connections and exits.
 */
#define _GNU_SOURCE
#include <errno.h>
#include <getopt.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "cli.h"
#include "shardnet/log.h"
#include "shardnet/shardnet.h"

static ssize_t on_data(sn_conn *c, const uint8_t *data, size_t len, void *ud)
{
	(void)ud;
	return sn_conn_send(c, data, len) == 0 ? (ssize_t)len : -1;
}

static void on_datagram(sn_server *s, int worker, const struct sockaddr *from,
			socklen_t fromlen, const uint8_t *data, size_t len,
			void *ud)
{
	(void)ud;
	sn_udp_send(s, worker, from, fromlen, data, len);
}

static void print_stats(sn_server *s)
{
	sn_stats st;

	sn_server_stats(s, -1, &st);
	sn_info("stats: active=%llu accepted=%llu rejected=%llu timeouts=%llu "
		"in=%llu out=%llu pending_out=%llu pauses=%llu udp_in=%llu "
		"udp_out=%llu udp_dropped=%llu",
		(unsigned long long)st.active, (unsigned long long)st.accepted,
		(unsigned long long)st.rejected,
		(unsigned long long)st.timeouts,
		(unsigned long long)st.bytes_in,
		(unsigned long long)st.bytes_out,
		(unsigned long long)st.pending_out,
		(unsigned long long)st.read_pauses,
		(unsigned long long)st.udp_in, (unsigned long long)st.udp_out,
		(unsigned long long)st.udp_dropped);
}

static void usage(const char *argv0)
{
	fprintf(stderr,
		"usage: %s [--host ADDR] [--tcp-port N|-1] [--udp-port N|-1]\n"
		"          [--workers N] [--max-conns N] [--idle-ms N]\n"
		"          [--stats-sec N] [--drain-ms N]\n", argv0);
	exit(2);
}

int main(int argc, char **argv)
{
	static const struct option opts[] = {
		{ "host", required_argument, NULL, 'h' },
		{ "tcp-port", required_argument, NULL, 't' },
		{ "udp-port", required_argument, NULL, 'u' },
		{ "workers", required_argument, NULL, 'w' },
		{ "max-conns", required_argument, NULL, 'm' },
		{ "idle-ms", required_argument, NULL, 'i' },
		{ "stats-sec", required_argument, NULL, 's' },
		{ "drain-ms", required_argument, NULL, 'd' },
		{ "help", no_argument, NULL, '?' },
		{ 0 }
	};
	const sn_handlers h = { .on_data = on_data, .on_datagram = on_datagram };
	sn_config cfg;
	sn_server *s;
	sigset_t sigs;
	int opt, stats_sec = 0, drain_ms = 5000, r, sig;

	sn_config_init(&cfg);
	cfg.tcp_port = 7000;
	cfg.udp_port = 7000;
	while ((opt = getopt_long(argc, argv, "", opts, NULL)) != -1) {
		switch (opt) {
		case 'h': cfg.host = optarg; break;
		case 't': cfg.tcp_port = (int)cli_long("tcp-port", optarg, -1, 65535); break;
		case 'u': cfg.udp_port = (int)cli_long("udp-port", optarg, -1, 65535); break;
		case 'w': cfg.workers = (int)cli_long("workers", optarg, 1, 1024); break;
		case 'm': cfg.max_conns_per_worker = (int)cli_long("max-conns", optarg, 1, 1000000); break;
		case 'i': cfg.idle_timeout_ms = (int)cli_long("idle-ms", optarg, 0, 86400000); break;
		case 's': stats_sec = (int)cli_long("stats-sec", optarg, 0, 86400); break;
		case 'd': drain_ms = (int)cli_long("drain-ms", optarg, 0, 3600000); break;
		default: usage(argv[0]);
		}
	}

	/* Handle SIGINT/SIGTERM synchronously in this thread. */
	sigemptyset(&sigs);
	sigaddset(&sigs, SIGINT);
	sigaddset(&sigs, SIGTERM);
	pthread_sigmask(SIG_BLOCK, &sigs, NULL);
	signal(SIGPIPE, SIG_IGN);

	r = sn_server_create(&cfg, &h, NULL, &s);
	if (r) {
		sn_err("cannot create server: %s", strerror(-r));
		return 1;
	}
	r = sn_server_start(s);
	if (r) {
		sn_err("cannot start server: %s", strerror(-r));
		sn_server_destroy(s);
		return 1;
	}
	printf("listening tcp=%d udp=%d workers=%d\n", sn_server_tcp_port(s),
	       sn_server_udp_port(s), sn_server_workers(s));
	fflush(stdout);

	for (;;) {
		if (stats_sec > 0) {
			struct timespec ts = { stats_sec, 0 };

			sig = sigtimedwait(&sigs, NULL, &ts);
			if (sig < 0) {
				if (errno == EAGAIN)
					print_stats(s);
				continue;
			}
		} else if (sigwait(&sigs, &sig) != 0) {
			continue;
		}
		break;
	}
	sn_info("received %s, draining (up to %d ms)", strsignal(sig),
		drain_ms);
	sn_server_stop(s, drain_ms);
	print_stats(s);
	sn_server_destroy(s);
	return 0;
}
