// SPDX-License-Identifier: Apache-2.0
#define KT_IMPLEMENTATION
#include "kt.h"

#include <stdint.h>

#include "buf.h"

KT_TEST(append_and_consume)
{
	sn_buf b = { 0 };

	KT_EQ(sn_buf_append(&b, "hello", 5), 0);
	KT_EQ(b.len, 5);
	KT_ASSERT(memcmp(sn_buf_head(&b), "hello", 5) == 0);
	sn_buf_consume(&b, 2);
	KT_EQ(b.len, 3);
	KT_ASSERT(memcmp(sn_buf_head(&b), "llo", 3) == 0);
	sn_buf_consume(&b, 3);
	KT_EQ(b.len, 0);
	KT_EQ(b.off, 0);	/* reset to the start when drained */
	sn_buf_free(&b);
	KT_ASSERT(b.data == NULL);
}

KT_TEST(compacts_before_growing)
{
	sn_buf b = { 0 };
	uint8_t chunk[SN_BUF_MIN_CAP];

	memset(chunk, 'x', sizeof(chunk));
	KT_EQ(sn_buf_append(&b, chunk, sizeof(chunk) - 100), 0);
	KT_EQ(b.cap, SN_BUF_MIN_CAP);
	sn_buf_consume(&b, 1000);
	/* 1000 bytes free at the front; this fits after compaction. */
	KT_EQ(sn_buf_append(&b, chunk, 900), 0);
	KT_EQ(b.cap, SN_BUF_MIN_CAP);
	KT_EQ(b.off, 0);
	KT_EQ(b.len, SN_BUF_MIN_CAP - 100 - 1000 + 900);
	sn_buf_free(&b);
}

KT_TEST(grows_and_preserves_content)
{
	sn_buf b = { 0 };
	uint8_t *ref = malloc(1 << 20);
	size_t total = 0, consumed = 0;

	KT_ASSERT(ref);
	for (size_t i = 0; i < (1 << 20); i++)
		ref[i] = (uint8_t)(i * 7 + (i >> 9));
	/* Interleave appends of varying size with partial consumes. */
	while (total < (1 << 20)) {
		size_t n = 1 + (total * 31) % 9000;

		if (total + n > (1 << 20))
			n = (1 << 20) - total;
		KT_EQ(sn_buf_append(&b, ref + total, n), 0);
		total += n;
		size_t c = b.len / 3;

		KT_ASSERT(memcmp(sn_buf_head(&b), ref + consumed, b.len) == 0);
		sn_buf_consume(&b, c);
		consumed += c;
	}
	KT_EQ(b.len, total - consumed);
	KT_ASSERT(memcmp(sn_buf_head(&b), ref + consumed, b.len) == 0);
	sn_buf_free(&b);
	free(ref);
}

KT_TEST(zero_length_append_is_noop)
{
	sn_buf b = { 0 };

	KT_EQ(sn_buf_append(&b, "", 0), 0);
	KT_ASSERT(b.data == NULL);
	KT_EQ(b.len, 0);
}

static const struct kt_case cases[] = {
	KT_CASE(append_and_consume),
	KT_CASE(compacts_before_growing),
	KT_CASE(grows_and_preserves_content),
	KT_CASE(zero_length_append_is_noop),
};

int main(int argc, char **argv)
{
	return kt_run(cases, KT_N(cases), argc, argv);
}
