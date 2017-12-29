/*
 * Description: 
 *     History: yang@haipo.me, 2017/12/14, create
 */

# include "ut_log.h"
# include "ut_monitor.h"
# include "ut_rpc_cmd.h"
# include <jansson.h>

static rpc_clt *clt;
static char *process_scope;
static char *process_host;

static void on_server_connect(nw_ses *ses, bool result)
{
    rpc_clt *clt = ses->privdata;
    if (result) {
        log_info("connect %s:%s success", clt->name, nw_sock_human_addr(&ses->peer_addr));
    } else {
        log_info("connect %s:%s fail", clt->name, nw_sock_human_addr(&ses->peer_addr));
    }
}

static void on_server_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    return;
}

int monitor_init(rpc_clt_cfg *cfg, const char *scope, const char *host)
{
    if (strlen(scope) == 0 || strlen(host) == 0)
        return -__LINE__;

    rpc_clt_type type;
    memset(&type, 0, sizeof(type));
    type.on_connect = on_server_connect;
    type.on_recv_pkg = on_server_recv_pkg;
    clt = rpc_clt_create(cfg, &type);
    if (clt == NULL)
        return -__LINE__;
    if (rpc_clt_start(clt) < 0)
        return -__LINE__;

    process_scope = strdup(scope);
    process_host  = strdup(host);

    return 0;
}

void monitor_inc(const char *key, uint64_t val)
{
    if (clt == NULL)
        return;

    json_t *params = json_array();
    json_array_append_new(params, json_string(process_scope));
    json_array_append_new(params, json_string(key));
    json_array_append_new(params, json_string(process_host));
    json_array_append_new(params, json_integer(val));

    rpc_pkg pkg;
    memset(&pkg, 0, sizeof(pkg));
    pkg.pkg_type  = RPC_PKG_TYPE_REQUEST;
    pkg.command   = CMD_MONITOR_INC;
    pkg.body      = json_dumps(params, 0);
    pkg.body_size = strlen(pkg.body);

    rpc_clt_send(clt, &pkg);
    free(pkg.body);
    json_decref(params);
}

void monitor_set(const char *key, uint64_t val)
{
    if (clt == NULL)
        return;

    json_t *params = json_array();
    json_array_append_new(params, json_string(process_scope));
    json_array_append_new(params, json_string(key));
    json_array_append_new(params, json_string(process_host));
    json_array_append_new(params, json_integer(val));

    rpc_pkg pkg;
    memset(&pkg, 0, sizeof(pkg));
    pkg.pkg_type  = RPC_PKG_TYPE_REQUEST;
    pkg.command   = CMD_MONITOR_SET;
    pkg.body      = json_dumps(params, 0);
    pkg.body_size = strlen(pkg.body);

    rpc_clt_send(clt, &pkg);
    free(pkg.body);
    json_decref(params);
}

