// SPDX-License-Identifier: Apache-2.0
#include "pool.h"

#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

int sn_pool_init(sn_pool *p, size_t obj_size, uint32_t count)
{
	memset(p, 0, sizeof(*p));
	if (obj_size == 0 || count == 0 || count == UINT32_MAX)
		return -EINVAL;
	/* Round up so every object stays suitably aligned. */
	obj_size = (obj_size + 15) & ~(size_t)15;
	if (obj_size > SIZE_MAX / count)
		return -ENOMEM;
	p->mem = calloc(count, obj_size);
	p->next = malloc(sizeof(*p->next) * count);
	if (!p->mem || !p->next) {
		sn_pool_destroy(p);
		return -ENOMEM;
	}
	for (uint32_t i = 0; i < count; i++)
		p->next[i] = i + 1;
	p->size = obj_size;
	p->count = count;
	p->free_head = 0;
	return 0;
}

void sn_pool_destroy(sn_pool *p)
{
	free(p->mem);
	free(p->next);
	memset(p, 0, sizeof(*p));
}

void *sn_pool_alloc(sn_pool *p)
{
	uint32_t i = p->free_head;
	uint8_t *obj;

	if (i >= p->count)
		return NULL;
	p->free_head = p->next[i];
	p->in_use++;
	obj = p->mem + (size_t)i * p->size;
	memset(obj, 0, p->size);
	return obj;
}

void sn_pool_free(sn_pool *p, void *obj)
{
	size_t byte = (size_t)((uint8_t *)obj - p->mem);
	uint32_t i = (uint32_t)(byte / p->size);

	assert(byte % p->size == 0 && i < p->count && p->in_use > 0);
	p->next[i] = p->free_head;
	p->free_head = i;
	p->in_use--;
}
