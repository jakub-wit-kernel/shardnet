// SPDX-License-Identifier: Apache-2.0
#include "buf.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

int sn_buf_append(sn_buf *b, const void *src, size_t n)
{
	if (n == 0)
		return 0;
	if (n > SIZE_MAX - b->len)
		return -ENOMEM;

	/* Not enough room at the tail? Compact first, grow only if needed. */
	if (b->off + b->len + n > b->cap) {
		if (b->len + n <= b->cap) {
			memmove(b->data, b->data + b->off, b->len);
			b->off = 0;
		} else {
			size_t cap = b->cap ? b->cap : SN_BUF_MIN_CAP;
			uint8_t *p;

			while (cap < b->len + n) {
				if (cap > SIZE_MAX / 2)
					return -ENOMEM;
				cap *= 2;
			}
			p = malloc(cap);
			if (!p)
				return -ENOMEM;
			if (b->len)
				memcpy(p, b->data + b->off, b->len);
			free(b->data);
			b->data = p;
			b->cap = cap;
			b->off = 0;
		}
	}
	memcpy(b->data + b->off + b->len, src, n);
	b->len += n;
	return 0;
}

void sn_buf_consume(sn_buf *b, size_t n)
{
	if (n >= b->len) {
		b->off = 0;
		b->len = 0;
		return;
	}
	b->off += n;
	b->len -= n;
}

void sn_buf_free(sn_buf *b)
{
	free(b->data);
	b->data = NULL;
	b->off = 0;
	b->len = 0;
	b->cap = 0;
}
