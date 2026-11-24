/* SPDX-License-Identifier: Apache-2.0 */
/*
 * Minimal leveled logger: UTC timestamp, level, thread tag, message, one
 * write(2) per line so lines from different threads never interleave.
 * The level comes from sn_log_set_level() or the SN_LOG_LEVEL environment
 * variable (error, warn, info, debug); default info.
 */
#ifndef SHARDNET_LOG_H
#define SHARDNET_LOG_H

#ifdef __cplusplus
extern "C" {
#endif

enum sn_log_level { SN_LOG_ERROR, SN_LOG_WARN, SN_LOG_INFO, SN_LOG_DEBUG };

void sn_log_set_level(enum sn_log_level level);
enum sn_log_level sn_log_get_level(void);
/* Tag shown for the calling thread, e.g. "w3". Copied; max 15 chars. */
void sn_log_set_thread_tag(const char *tag);
void sn_log_write(enum sn_log_level level, const char *fmt, ...)
	__attribute__((format(printf, 2, 3)));

#define SN_LOG(level, ...)						\
	do {								\
		if ((level) <= sn_log_get_level())			\
			sn_log_write((level), __VA_ARGS__);		\
	} while (0)

#define sn_err(...)	SN_LOG(SN_LOG_ERROR, __VA_ARGS__)
#define sn_warn(...)	SN_LOG(SN_LOG_WARN, __VA_ARGS__)
#define sn_info(...)	SN_LOG(SN_LOG_INFO, __VA_ARGS__)
#define sn_debug(...)	SN_LOG(SN_LOG_DEBUG, __VA_ARGS__)

#ifdef __cplusplus
}
#endif

#endif /* SHARDNET_LOG_H */
