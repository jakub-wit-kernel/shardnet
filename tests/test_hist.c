// SPDX-License-Identifier: Apache-2.0
#define KT_IMPLEMENTATION
#include "kt.h"

#include <math.h>
#include <stdint.h>

#include "shardnet/hist.h"

KT_TEST(small_values_are_exact)
{
	for (uint64_t v = 0; v < SN_HIST_SUB; v++) {
		KT_EQ(sn_hist_index(v), v);
		KT_EQ(sn_hist_bucket_low((unsigned)v), v);
		KT_EQ(sn_hist_bucket_high((unsigned)v), v);
	}
}

/* Every value must land in a bucket whose [low, high] contains it. */
KT_TEST(buckets_contain_their_values)
{
	uint64_t v = 1;

	for (int i = 0; i < 200000; i++) {
		unsigned idx = sn_hist_index(v);

		KT_ASSERT(idx < SN_HIST_BUCKETS);
		KT_ASSERT(sn_hist_bucket_low(idx) <= v);
		KT_ASSERT(v <= sn_hist_bucket_high(idx));
		v = v * 1.0001 + 1 + (uint64_t)i;
		if (v > (UINT64_MAX >> 2))
			v = (uint64_t)i;
	}
	KT_ASSERT(sn_hist_index(UINT64_MAX) == SN_HIST_BUCKETS - 1);
}

/* Bucket ranges tile the number line with no gaps or overlaps. */
KT_TEST(buckets_are_contiguous)
{
	for (unsigned i = 1; i < SN_HIST_BUCKETS; i++)
		KT_EQ(sn_hist_bucket_low(i), sn_hist_bucket_high(i - 1) + 1);
}

static int cmp_u64(const void *a, const void *b)
{
	uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;

	return (x > y) - (x < y);
}

KT_TEST(percentiles_within_relative_error)
{
	enum { N = 100000 };
	static uint64_t vals[N];
	static sn_hist h;
	static const double ps[] = { 1, 10, 50, 90, 99, 99.9 };
	uint64_t x = 88172645463325252ull;

	sn_hist_init(&h);
	for (int i = 0; i < N; i++) {
		/* Log-uniform spread over ~1 us .. ~10 s in nanoseconds. */
		x ^= x << 13;
		x ^= x >> 7;
		x ^= x << 17;
		vals[i] = (uint64_t)pow(10.0, 3.0 + (double)(x % 7000000) / 1e6);
		sn_hist_record(&h, vals[i]);
	}
	qsort(vals, N, sizeof(vals[0]), cmp_u64);
	for (size_t i = 0; i < KT_N(ps); i++) {
		uint64_t exact = vals[(size_t)(ps[i] / 100.0 * N) - 1];
		uint64_t est = sn_hist_percentile(&h, ps[i]);
		double err = fabs((double)est - (double)exact) / (double)exact;

		if (err > 1.0 / SN_HIST_SUB)
			KT_FAIL("p%.1f: exact %llu est %llu err %.4f", ps[i],
				(unsigned long long)exact,
				(unsigned long long)est, err);
	}
	KT_EQ(sn_hist_percentile(&h, 100), vals[N - 1]);
	KT_EQ(h.min, vals[0]);
}

KT_TEST(merge_equals_combined)
{
	static sn_hist a, b, all;

	sn_hist_init(&a);
	sn_hist_init(&b);
	sn_hist_init(&all);
	for (uint64_t v = 1; v < 50000; v += 7) {
		sn_hist_record(v % 2 ? &a : &b, v);
		sn_hist_record(&all, v);
	}
	sn_hist_merge(&a, &b);
	KT_EQ(a.total, all.total);
	KT_EQ(a.min, all.min);
	KT_EQ(a.max, all.max);
	KT_ASSERT(memcmp(a.counts, all.counts, sizeof(a.counts)) == 0);
	KT_EQ(sn_hist_percentile(&a, 99), sn_hist_percentile(&all, 99));
}

KT_TEST(empty_histogram)
{
	static sn_hist h;

	sn_hist_init(&h);
	KT_EQ(sn_hist_percentile(&h, 50), 0);
	KT_ASSERT(sn_hist_mean(&h) == 0.0);
}

static const struct kt_case cases[] = {
	KT_CASE(small_values_are_exact),
	KT_CASE(buckets_contain_their_values),
	KT_CASE(buckets_are_contiguous),
	KT_CASE(percentiles_within_relative_error),
	KT_CASE(merge_equals_combined),
	KT_CASE(empty_histogram),
};

int main(int argc, char **argv)
{
	return kt_run(cases, KT_N(cases), argc, argv);
}
