// SPDX-License-Identifier: Apache-2.0
#include "shardnet/hist.h"

#include <string.h>

unsigned sn_hist_index(uint64_t v)
{
	unsigned e;

	if (v < SN_HIST_SUB)
		return (unsigned)v;
	e = 63u - (unsigned)__builtin_clzll(v);		/* e >= SUB_BITS */
	return (e - SN_HIST_SUB_BITS + 1) * SN_HIST_SUB +
	       (unsigned)((v >> (e - SN_HIST_SUB_BITS)) & (SN_HIST_SUB - 1));
}

uint64_t sn_hist_bucket_low(unsigned idx)
{
	unsigned e, sub;

	if (idx < SN_HIST_SUB)
		return idx;
	e = idx / SN_HIST_SUB + SN_HIST_SUB_BITS - 1;
	sub = idx % SN_HIST_SUB;
	return ((uint64_t)SN_HIST_SUB + sub) << (e - SN_HIST_SUB_BITS);
}

uint64_t sn_hist_bucket_high(unsigned idx)
{
	unsigned e;

	if (idx < SN_HIST_SUB)
		return idx;
	e = idx / SN_HIST_SUB + SN_HIST_SUB_BITS - 1;
	return sn_hist_bucket_low(idx) + ((uint64_t)1 << (e - SN_HIST_SUB_BITS)) - 1;
}

void sn_hist_init(sn_hist *h)
{
	memset(h, 0, sizeof(*h));
	h->min = UINT64_MAX;
}

void sn_hist_record(sn_hist *h, uint64_t v)
{
	h->counts[sn_hist_index(v)]++;
	h->total++;
	h->sum += (double)v;
	if (v < h->min)
		h->min = v;
	if (v > h->max)
		h->max = v;
}

void sn_hist_merge(sn_hist *dst, const sn_hist *src)
{
	for (unsigned i = 0; i < SN_HIST_BUCKETS; i++)
		dst->counts[i] += src->counts[i];
	dst->total += src->total;
	dst->sum += src->sum;
	if (src->min < dst->min)
		dst->min = src->min;
	if (src->max > dst->max)
		dst->max = src->max;
}

uint64_t sn_hist_percentile(const sn_hist *h, double p)
{
	uint64_t rank, seen = 0;

	if (h->total == 0)
		return 0;
	if (p >= 100.0)
		return h->max;
	rank = (uint64_t)(p / 100.0 * (double)h->total);
	if (rank == 0)
		rank = 1;
	for (unsigned i = 0; i < SN_HIST_BUCKETS; i++) {
		seen += h->counts[i];
		if (seen >= rank) {
			/* Midpoint of the bucket, clamped to observed range. */
			uint64_t lo = sn_hist_bucket_low(i);
			uint64_t v = lo + (sn_hist_bucket_high(i) - lo) / 2;

			if (v < h->min)
				v = h->min;
			if (v > h->max)
				v = h->max;
			return v;
		}
	}
	return h->max;
}

double sn_hist_mean(const sn_hist *h)
{
	return h->total ? h->sum / (double)h->total : 0.0;
}
