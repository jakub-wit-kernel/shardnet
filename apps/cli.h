/* SPDX-License-Identifier: Apache-2.0 */
/* Strict command-line number parsing: reject junk and out-of-range values. */
#ifndef SN_CLI_H
#define SN_CLI_H

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>

static inline long cli_long(const char *opt, const char *s, long min, long max)
{
	char *end;
	long v;

	errno = 0;
	v = strtol(s, &end, 10);
	if (errno || end == s || *end || v < min || v > max) {
		fprintf(stderr, "--%s: expected an integer in [%ld, %ld], got '%s'\n",
			opt, min, max, s);
		exit(2);
	}
	return v;
}

static inline double cli_double(const char *opt, const char *s, double min,
				double max)
{
	char *end;
	double v;

	errno = 0;
	v = strtod(s, &end);
	if (errno || end == s || *end || !(v >= min && v <= max)) {
		fprintf(stderr, "--%s: expected a number in [%g, %g], got '%s'\n",
			opt, min, max, s);
		exit(2);
	}
	return v;
}

#endif /* SN_CLI_H */
