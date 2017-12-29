/*
 * Description: 
 *     History: yang@haipo.me, 2017/12/14, create
 */

# ifndef UT_MONITOR_H
# define UT_MONITOR_H

# include <stdint.h>
# include "ut_rpc_clt.h"

int monitor_init(rpc_clt_cfg *cfg, const char *scope, const char *host);
void monitor_inc(const char *key, uint64_t val);
void monitor_set(const char *key, uint64_t val);

# endif

