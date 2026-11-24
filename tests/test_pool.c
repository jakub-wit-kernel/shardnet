// SPDX-License-Identifier: Apache-2.0
#define KT_IMPLEMENTATION
#include "kt.h"

#include <stdint.h>

#include "pool.h"

struct obj {
	uint64_t a;
	char tag[13];
};

KT_TEST(alloc_until_exhausted_then_reuse)
{
	sn_pool p;
	struct obj *o[8];

	KT_EQ(sn_pool_init(&p, sizeof(struct obj), 8), 0);
	KT_EQ(p.size % 16, 0);
	for (int i = 0; i < 8; i++) {
		o[i] = sn_pool_alloc(&p);
		KT_ASSERT(o[i]);
		KT_EQ(o[i]->a, 0);	/* zeroed */
		o[i]->a = (uint64_t)i + 100;
	}
	KT_EQ(p.in_use, 8);
	KT_ASSERT(sn_pool_alloc(&p) == NULL);

	sn_pool_free(&p, o[3]);
	KT_EQ(p.in_use, 7);
	struct obj *again = sn_pool_alloc(&p);

	KT_ASSERT(again == o[3]);	/* LIFO reuse: hot in cache */
	KT_EQ(again->a, 0);
	/* Neighbours untouched. */
	KT_EQ(o[2]->a, 102);
	KT_EQ(o[4]->a, 104);
	sn_pool_destroy(&p);
}

KT_TEST(objects_are_distinct_and_aligned)
{
	sn_pool p;
	void *seen[100];

	KT_EQ(sn_pool_init(&p, 24, 100), 0);
	for (int i = 0; i < 100; i++) {
		seen[i] = sn_pool_alloc(&p);
		KT_ASSERT(((uintptr_t)seen[i] & 15) == 0);
		for (int j = 0; j < i; j++)
			KT_ASSERT(seen[i] != seen[j]);
	}
	for (int i = 0; i < 100; i += 2)
		sn_pool_free(&p, seen[i]);
	KT_EQ(p.in_use, 50);
	for (int i = 0; i < 50; i++)
		KT_ASSERT(sn_pool_alloc(&p));
	KT_ASSERT(sn_pool_alloc(&p) == NULL);
	sn_pool_destroy(&p);
}

KT_TEST(init_rejects_bad_args)
{
	sn_pool p;

	KT_EQ(sn_pool_init(&p, 0, 10), -EINVAL);
	KT_EQ(sn_pool_init(&p, 8, 0), -EINVAL);
}

static const struct kt_case cases[] = {
	KT_CASE(alloc_until_exhausted_then_reuse),
	KT_CASE(objects_are_distinct_and_aligned),
	KT_CASE(init_rejects_bad_args),
};

int main(int argc, char **argv)
{
	return kt_run(cases, KT_N(cases), argc, argv);
}
