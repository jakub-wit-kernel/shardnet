/* SPDX-License-Identifier: Apache-2.0 */
/*
 * kt.h - a deliberately small test harness shared by the unit and
 * integration suites. Output is TAP version 13 so CI logs stay readable and
 * any TAP consumer can parse it.
 *
 * Usage:
 *   KT_TEST(name) { KT_ASSERT(x == 1); KT_EQ(a, b); }
 *   static const struct kt_case cases[] = { KT_CASE(name), ... };
 *   int main(int argc, char **argv) { return kt_run(cases, KT_N(cases), argc, argv); }
 *
 * A filter argument (substring match on the test name) runs a subset.
 */
#ifndef KT_H
#define KT_H

#include <errno.h>
#include <setjmp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct kt_case {
	const char *name;
	void (*fn)(void);
};

#define KT_N(a)		(sizeof(a) / sizeof((a)[0]))
#define KT_TEST(name)	static void kt_##name(void)
#define KT_CASE(name)	{ #name, kt_##name }

extern jmp_buf kt_jmp;
extern int kt_skipped;

#define KT_FAIL(...)							\
	do {								\
		printf("  # FAIL %s:%d: ", __FILE__, __LINE__);		\
		printf(__VA_ARGS__);					\
		printf("\n");						\
		longjmp(kt_jmp, 1);					\
	} while (0)

#define KT_ASSERT(cond)							\
	do {								\
		if (!(cond))						\
			KT_FAIL("assertion failed: %s", #cond);		\
	} while (0)

#define KT_EQ(a, b)							\
	do {								\
		long long kt_a_ = (long long)(a);			\
		long long kt_b_ = (long long)(b);			\
		if (kt_a_ != kt_b_)					\
			KT_FAIL("%s == %s (%lld != %lld)", #a, #b,	\
				kt_a_, kt_b_);				\
	} while (0)

/* Assert a syscall-style call returns -1 with the given errno. */
#define KT_ERRNO(call, err)						\
	do {								\
		errno = 0;						\
		long kt_r_ = (long)(call);				\
		int kt_e_ = errno;					\
		if (kt_r_ != -1 || kt_e_ != (err)) {			\
			/* strerror() may reuse one buffer: copy first. */ \
			char kt_want_[64];				\
			snprintf(kt_want_, sizeof(kt_want_), "%s",	\
				 strerror(err));			\
			KT_FAIL("%s: expected -1/%s, got %ld/%s",	\
				#call, kt_want_, kt_r_,			\
				strerror(kt_e_));			\
		}							\
	} while (0)

/* Assert a syscall-style call does not return -1. */
#define KT_OK(call)							\
	do {								\
		errno = 0;						\
		if ((long)(call) == -1)					\
			KT_FAIL("%s: %s", #call, strerror(errno));	\
	} while (0)

#define KT_SKIP(...)							\
	do {								\
		printf("  # SKIP ");					\
		printf(__VA_ARGS__);					\
		printf("\n");						\
		kt_skipped = 1;						\
		longjmp(kt_jmp, 2);					\
	} while (0)

#ifdef KT_IMPLEMENTATION
jmp_buf kt_jmp;
int kt_skipped;

/* Returns 0 = pass, 1 = fail, 2 = skip. setjmp() is isolated here so no
 * caller locals are live across longjmp(). */
static int kt_run_one(void (*fn)(void))
{
	int rc = setjmp(kt_jmp);

	if (rc == 0)
		fn();
	return rc;
}

static int kt_run(const struct kt_case *cases, size_t n, int argc,
		  char **argv)
{
	const char *filter = argc > 1 ? argv[1] : NULL;
	size_t i, ran = 0, failed = 0, skipped = 0, planned = 0;

	for (i = 0; i < n; i++)
		if (!filter || strstr(cases[i].name, filter))
			planned++;

	printf("TAP version 13\n1..%zu\n", planned);
	for (i = 0; i < n; i++) {
		int rc;

		if (filter && !strstr(cases[i].name, filter))
			continue;
		ran++;
		kt_skipped = 0;
		fflush(stdout);
		rc = kt_run_one(cases[i].fn);
		if (rc == 1) {
			failed++;
			printf("not ok %zu - %s\n", ran, cases[i].name);
		} else if (rc == 2) {
			skipped++;
			printf("ok %zu - %s # SKIP\n", ran, cases[i].name);
		} else {
			printf("ok %zu - %s\n", ran, cases[i].name);
		}
		fflush(stdout);
	}
	printf("# %zu run, %zu passed, %zu failed, %zu skipped\n", ran,
	       ran - failed - skipped, failed, skipped);
	return failed ? EXIT_FAILURE : EXIT_SUCCESS;
}
#endif /* KT_IMPLEMENTATION */

#endif /* KT_H */
