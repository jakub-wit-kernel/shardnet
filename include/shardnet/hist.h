/* SPDX-License-Identifier: Apache-2.0 */
/*
 * sn_hist - fixed-size log-linear histogram for latency recording.
 *
 * Values below 32 are exact; above that, each power-of-two range is split
 * into 32 equal sub-buckets, bounding relative error to ~3%. Recording is
 * O(1) with no allocation; histograms from different threads can be merged.
 */
#ifndef SHARDNET_HIST_H
#define SHARDNET_HIST_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define SN_HIST_SUB_BITS	5
#define SN_HIST_SUB		(1u << SN_HIST_SUB_BITS)
#define SN_HIST_BUCKETS		((64 - SN_HIST_SUB_BITS + 1) * SN_HIST_SUB)

typedef struct sn_hist {
	uint64_t counts[SN_HIST_BUCKETS];
	uint64_t total;
	uint64_t min;
	uint64_t max;
	double sum;
} sn_hist;

void sn_hist_init(sn_hist *h);
void sn_hist_record(sn_hist *h, uint64_t value);
void sn_hist_merge(sn_hist *dst, const sn_hist *src);
/* Value at percentile @p (0 < p <= 100); 0 if empty. */
uint64_t sn_hist_percentile(const sn_hist *h, double p);
double sn_hist_mean(const sn_hist *h);

/* Exposed for tests. */
unsigned sn_hist_index(uint64_t value);
uint64_t sn_hist_bucket_low(unsigned idx);
uint64_t sn_hist_bucket_high(unsigned idx);

#ifdef __cplusplus
}
#endif

#endif /* SHARDNET_HIST_H */
