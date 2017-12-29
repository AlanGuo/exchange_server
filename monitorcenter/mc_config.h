/*
 * Description: 
 *     History: yang@haipo.me, 2017/12/06, create
 */

# ifndef MC_CONFIG_H
# define MC_CONFIG_H

# include <math.h>
# include <stdio.h>
# include <error.h>
# include <errno.h>
# include <ctype.h>
# include <string.h>
# include <stdlib.h>
# include <unistd.h>
# include <assert.h>
# include <inttypes.h>

# include "nw_svr.h"
# include "nw_clt.h"
# include "nw_job.h"
# include "nw_timer.h"

# include "ut_log.h"
# include "ut_sds.h"
# include "ut_cli.h"
# include "ut_misc.h"
# include "ut_signal.h"
# include "ut_define.h"
# include "ut_config.h"
# include "ut_rpc_clt.h"
# include "ut_rpc_svr.h"
# include "ut_rpc_cmd.h"

# define MAX_SCOPE_LENGTH   16
# define MAX_KEY_LENGTH     64
# define MAX_HOST_LENGTH    64
# define MAX_KEEP_DAYS      7
# define MAX_QUERY_POINTS   10080

struct settings {
    bool                debug;
    process_cfg         process;
    log_cfg             log;
    alert_cfg           alert;
    rpc_svr_cfg         svr;
    redis_sentinel_cfg  redis;
};

extern struct settings settings;

int init_config(const char *path);

# endif

