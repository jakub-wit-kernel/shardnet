/* SPDX-License-Identifier: Apache-2.0 */
/*
 * sn_pool - fixed-capacity object pool with O(1) alloc/free.
 *
 * All objects are allocated up front in one block, so the hot path never
 * calls malloc, objects are cache-friendly, and capacity doubles as an
 * admission limit. Not thread-safe: each worker owns its pool.
 */
#ifndef SN_POOL_H
#define SN_POOL_H

#include <stddef.h>
#include <stdint.h>

typedef struct sn_pool {
	uint8_t *mem;		/* count * size bytes, zeroed */
	uint32_t *next;		/* free-list links by index */
	size_t size;		/* object size */
	uint32_t count;		/* capacity */
	uint32_t free_head;	/* first free index, or count when exhausted */
	uint32_t in_use;
} sn_pool;

int sn_pool_init(sn_pool *p, size_t obj_size, uint32_t count);
void sn_pool_destroy(sn_pool *p);
/* Zeroed object, or NULL when the pool is exhausted. */
void *sn_pool_alloc(sn_pool *p);
void sn_pool_free(sn_pool *p, void *obj);

#endif /* SN_POOL_H */
