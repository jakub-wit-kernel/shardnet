// SPDX-License-Identifier: Apache-2.0
#include "shardnet/log.h"

#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <unistd.h>

static int log_level = -1;	/* -1: not yet read from the environment */
static pthread_once_t log_once = PTHREAD_ONCE_INIT;
static _Thread_local char thread_tag[16] = "main";

static void log_init_from_env(void)
{
	const char *env = getenv("SN_LOG_LEVEL");
	int level = SN_LOG_INFO;

	if (env) {
		if (!strcasecmp(env, "error"))
			level = SN_LOG_ERROR;
		else if (!strcasecmp(env, "warn"))
			level = SN_LOG_WARN;
		else if (!strcasecmp(env, "debug"))
			level = SN_LOG_DEBUG;
	}
	/* Only fill in a default; an explicit set_level wins. */
	int expected = -1;

	__atomic_compare_exchange_n(&log_level, &expected, level, 0,
				    __ATOMIC_RELAXED, __ATOMIC_RELAXED);
}

void sn_log_set_level(enum sn_log_level level)
{
	pthread_once(&log_once, log_init_from_env);
	__atomic_store_n(&log_level, (int)level, __ATOMIC_RELAXED);
}

enum sn_log_level sn_log_get_level(void)
{
	pthread_once(&log_once, log_init_from_env);
	return (enum sn_log_level)__atomic_load_n(&log_level, __ATOMIC_RELAXED);
}

void sn_log_set_thread_tag(const char *tag)
{
	snprintf(thread_tag, sizeof(thread_tag), "%s", tag);
}

void sn_log_write(enum sn_log_level level, const char *fmt, ...)
{
	static const char *const names[] = { "ERROR", "WARN", "INFO", "DEBUG" };
	char line[1024];
	struct timespec ts;
	struct tm tm;
	va_list ap;
	int n;

	clock_gettime(CLOCK_REALTIME, &ts);
	gmtime_r(&ts.tv_sec, &tm);
	n = snprintf(line, sizeof(line),
		     "%04d-%02d-%02dT%02d:%02d:%02d.%03ldZ %-5s [%s] ",
		     tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour,
		     tm.tm_min, tm.tm_sec, ts.tv_nsec / 1000000,
		     names[level], thread_tag);
	va_start(ap, fmt);
	n += vsnprintf(line + n, sizeof(line) - (size_t)n - 1, fmt, ap);
	va_end(ap);
	if (n > (int)sizeof(line) - 2)
		n = (int)sizeof(line) - 2;
	line[n++] = '\n';
	/* One write per line keeps concurrent log lines intact. */
	(void)!write(STDERR_FILENO, line, (size_t)n);
}
