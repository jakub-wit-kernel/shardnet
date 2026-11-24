/* SPDX-License-Identifier: Apache-2.0 */
/*
 * sn_buf - growable byte FIFO used for per-connection pending input/output.
 *
 * Data lives in [off, off + len) of a heap block of size cap. Consuming
 * advances off (O(1)); appending compacts or grows only when needed. A
 * drained buffer can release its memory so idle connections cost nothing.
 */
#ifndef SN_BUF_H
#define SN_BUF_H

#include <stddef.h>
#include <stdint.h>

typedef struct sn_buf {
	uint8_t *data;
	size_t off;
	size_t len;
	size_t cap;
} sn_buf;

#define SN_BUF_MIN_CAP 4096

static inline const uint8_t *sn_buf_head(const sn_buf *b)
{
	return b->data + b->off;
}

/* Append @n bytes. Returns 0 or -ENOMEM (buffer unchanged on failure). */
int sn_buf_append(sn_buf *b, const void *src, size_t n);
/* Drop @n bytes from the front (n <= len). */
void sn_buf_consume(sn_buf *b, size_t n);
/* Free the backing memory. The buffer may be reused afterwards. */
void sn_buf_free(sn_buf *b);

#endif /* SN_BUF_H */
