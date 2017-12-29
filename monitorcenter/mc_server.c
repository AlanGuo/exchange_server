/*
 * Description: 
 *     History: yang@haipo.me, 2017/12/06, create
 */

# include "mc_config.h"
# include "mc_server.h"

static rpc_svr *svr;
static redis_sentinel_t *redis;
static redisContext *redis_store;
static dict_t *monitor_set;
static dict_t *monitor_val;
static time_t last_aggregate_hosts;
static time_t last_aggregate_daily;
static nw_timer timer;

struct monitor_key {
    char key[100];
    time_t timestamp;
};

struct monitor_val {
    uint64_t val;
};

static bool is_good_scope(const char *value)
{
    const size_t len = strlen(value);
    if (len == 0 || len > MAX_SCOPE_LENGTH)
        return false;
    for (size_t i = 0; i < len; ++i) {
        if (!(isalnum(value[i]) || value[i] == '.' || value[i] == '-' || value[i] == '_'))
            return false;
    }

    return true;
}

static bool is_good_key(const char *value)
{
    const size_t len = strlen(value);
    if (len == 0 || len > MAX_KEY_LENGTH)
        return false;
    for (size_t i = 0; i < len; ++i) {
        if (!(isalnum(value[i]) || value[i] == '.' || value[i] == '-' || value[i] == '_'))
            return false;
    }

    return true;
}

static bool is_good_host(const char *value)
{
    const size_t len = strlen(value);
    if (len == 0 || len > MAX_HOST_LENGTH)
        return false;
    for (size_t i = 0; i < len; ++i) {
        if (!(isalnum(value[i]) || value[i] == '.' || value[i] == '-' || value[i] == '_'))
            return false;
    }

    return true;
}

static int reply_json(nw_ses *ses, rpc_pkg *pkg, const json_t *json)
{
    char *message_data;
    if (settings.debug) {
        message_data = json_dumps(json, JSON_INDENT(4));
    } else {
        message_data = json_dumps(json, 0);
    }
    if (message_data == NULL)
        return -__LINE__;
    log_trace("connection: %s send: %s", nw_sock_human_addr(&ses->peer_addr), message_data);

    rpc_pkg reply;
    memcpy(&reply, pkg, sizeof(reply));
    reply.pkg_type = RPC_PKG_TYPE_REPLY;
    reply.body = message_data;
    reply.body_size = strlen(message_data);
    rpc_send(ses, &reply);
    free(message_data);

    return 0;
}

static int reply_error(nw_ses *ses, rpc_pkg *pkg, int code, const char *message)
{
    json_t *error = json_object();
    json_object_set_new(error, "code", json_integer(code));
    json_object_set_new(error, "message", json_string(message));

    json_t *reply = json_object();
    json_object_set_new(reply, "error", error);
    json_object_set_new(reply, "result", json_null());
    json_object_set_new(reply, "id", json_integer(pkg->req_id));

    int ret = reply_json(ses, pkg, reply);
    json_decref(reply);

    return ret;
}

static int reply_error_invalid_argument(nw_ses *ses, rpc_pkg *pkg)
{
    return reply_error(ses, pkg, 1, "invalid argument");
}

static int reply_error_internal_error(nw_ses *ses, rpc_pkg *pkg)
{
    return reply_error(ses, pkg, 2, "internal error");
}

static int reply_result(nw_ses *ses, rpc_pkg *pkg, json_t *result)
{
    json_t *reply = json_object();
    json_object_set_new(reply, "error", json_null());
    json_object_set    (reply, "result", result);
    json_object_set_new(reply, "id", json_integer(pkg->req_id));

    int ret = reply_json(ses, pkg, reply);
    json_decref(reply);

    return ret;
}

static int reply_success(nw_ses *ses, rpc_pkg *pkg)
{
    json_t *result = json_object();
    json_object_set_new(result, "status", json_string("success"));

    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

void *redis_query(const char *format, ...)
{
    for (int i = 0; i < 2; ++i) {
        if (redis_store == NULL) {
            log_info("redis connection lost, try connect");
            redis_store = redis_sentinel_connect_master(redis);
            if (redis_store == NULL) {
                log_error("redis_sentinel_connect_master fail");
                break;
            }
        }

        va_list ap;
        va_start(ap, format);
        redisReply *reply = redisvCommand(redis_store, format, ap);
        va_end(ap);

        if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
            if (reply == NULL) {
                log_error("redisvCommand fail: %d, %s", redis_store->err, strerror(errno));
            } else {
                log_error("redisvCommand fail: %d, %s, %s", redis_store->err, strerror(errno), reply->str);
                freeReplyObject(reply);
            }
            redisFree(redis_store);
            redis_store = NULL;
            continue;
        }

        return reply;
    }

    return NULL;
}

static int update_key_list(char *full_key, const char *scope, const char *key, const char *host)
{
    dict_entry *entry = dict_find(monitor_set, full_key);
    if (entry) {
        return 0;
    }

    log_info("key: %s not exist", full_key);
    redisReply *reply;

    reply = redis_query("SADD m:scopes %s", scope);
    if (reply == NULL)
        return -__LINE__;
    freeReplyObject(reply);

    reply = redis_query("SADD m:%s:keys %s", scope, key);
    if (reply == NULL)
        return -__LINE__;
    freeReplyObject(reply);

    reply = redis_query("SADD m:%s:%s:hosts %s", scope, key, host);
    if (reply == NULL)
        return -__LINE__;
    freeReplyObject(reply);

    dict_add(monitor_set, full_key, NULL);

    return 0;
}

static int update_key_inc(struct monitor_key *mkey, uint64_t val)
{
    dict_entry *entry = dict_find(monitor_val, mkey);
    if (entry) {
        struct monitor_val *mval = entry->val;
        mval->val += val;
        return 0;
    }

    struct monitor_val mval = { .val = val };
    dict_add(monitor_val, mkey, &mval);

    return 0;
}

static int update_key_set(struct monitor_key *mkey, uint64_t val)
{
    dict_entry *entry = dict_find(monitor_val, mkey);
    if (entry) {
        struct monitor_val *mval = entry->val;
        mval->val = val;
        return 0;
    }

    struct monitor_val mval = { .val = val };
    dict_add(monitor_val, mkey, &mval);

    return 0;
}

static int on_cmd_monitor_inc(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 4)
        return reply_error_invalid_argument(ses, pkg);
    const char *scope = json_string_value(json_array_get(params, 0));
    if (!scope || !is_good_scope(scope))
        return reply_error_invalid_argument(ses, pkg);
    const char *key = json_string_value(json_array_get(params, 1));
    if (!key || !is_good_key(key))
        return reply_error_invalid_argument(ses, pkg);
    const char *host = json_string_value(json_array_get(params, 2));
    if (!host || !is_good_host(host))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t val = json_integer_value(json_array_get(params, 3));

    struct monitor_key mkey;
    memset(&mkey, 0, sizeof(mkey));
    snprintf(mkey.key, sizeof(mkey.key), "%s:%s:%s", scope, key, host);
    mkey.timestamp = time(NULL) / 60 * 60;

    int ret;
    ret = update_key_list(mkey.key, scope, key, host);
    if (ret < 0) {
        log_error("update_key_list fail: %d", ret);
        return reply_error_internal_error(ses, pkg);
    }
    ret = update_key_inc(&mkey, val);
    if (ret < 0) {
        log_error("update_key_inc fail: %d", ret);
        return reply_error_internal_error(ses, pkg);
    }

    return reply_success(ses, pkg);
}

static int on_cmd_monitor_set(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 4)
        return reply_error_invalid_argument(ses, pkg);
    const char *scope = json_string_value(json_array_get(params, 0));
    if (!scope || !is_good_scope(scope))
        return reply_error_invalid_argument(ses, pkg);
    const char *key = json_string_value(json_array_get(params, 1));
    if (!key || !is_good_key(key))
        return reply_error_invalid_argument(ses, pkg);
    const char *host = json_string_value(json_array_get(params, 2));
    if (!host || !is_good_host(host))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t val = json_integer_value(json_array_get(params, 3));

    struct monitor_key mkey;
    memset(&mkey, 0, sizeof(mkey));
    snprintf(mkey.key, sizeof(mkey.key), "%s:%s:%s", scope, key, host);
    mkey.timestamp = time(NULL) / 60 * 60;

    int ret;
    ret = update_key_list(mkey.key, scope, key, host);
    if (ret < 0) {
        log_error("update_key_list fail: %d", ret);
        return reply_error_internal_error(ses, pkg);
    }
    ret = update_key_set(&mkey, val);
    if (ret < 0) {
        log_error("update_key_inc fail: %d", ret);
        return reply_error_internal_error(ses, pkg);
    }

    return reply_success(ses, pkg);
}

static int on_cmd_monitor_list_scope(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    redisReply *reply = redis_query("SMEMBERS m:scopes");
    if (reply == NULL)
        return reply_error_internal_error(ses, pkg);
    json_t *result = json_array();
    for (size_t i = 0; i < reply->elements; i += 1) {
        json_array_append_new(result, json_string(reply->element[i]->str));
    }

    reply_result(ses, pkg, result);
    json_decref(result);
    freeReplyObject(reply);

    return 0;
}

static int on_cmd_monitor_list_key(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 1)
        return reply_error_invalid_argument(ses, pkg);
    const char *scope = json_string_value(json_array_get(params, 0));
    if (!scope || !is_good_scope(scope))
        return reply_error_invalid_argument(ses, pkg);

    redisReply *reply = redis_query("SMEMBERS m:%s:keys", scope);
    if (reply == NULL)
        return reply_error_internal_error(ses, pkg);
    json_t *result = json_array();
    for (size_t i = 0; i < reply->elements; i += 1) {
        json_array_append_new(result, json_string(reply->element[i]->str));
    }

    reply_result(ses, pkg, result);
    json_decref(result);
    freeReplyObject(reply);

    return 0;
}

static int on_cmd_monitor_list_host(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 2)
        return reply_error_invalid_argument(ses, pkg);
    const char *scope = json_string_value(json_array_get(params, 0));
    if (!scope || !is_good_scope(scope))
        return reply_error_invalid_argument(ses, pkg);
    const char *key = json_string_value(json_array_get(params, 1));
    if (!key || !is_good_key(key))
        return reply_error_invalid_argument(ses, pkg);

    redisReply *reply = redis_query("SMEMBERS m:%s:%s:hosts", scope, key);
    if (reply == NULL)
        return reply_error_internal_error(ses, pkg);
    json_t *result = json_array();
    for (size_t i = 0; i < reply->elements; i += 1) {
        json_array_append_new(result, json_string(reply->element[i]->str));
    }

    reply_result(ses, pkg, result);
    json_decref(result);
    freeReplyObject(reply);

    return 0;
}

static int on_cmd_monitor_query(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 4)
        return reply_error_invalid_argument(ses, pkg);
    const char *scope = json_string_value(json_array_get(params, 0));
    if (!scope || !is_good_scope(scope))
        return reply_error_invalid_argument(ses, pkg);
    const char *key = json_string_value(json_array_get(params, 1));
    if (!key || !is_good_key(key))
        return reply_error_invalid_argument(ses, pkg);
    const char *host = json_string_value(json_array_get(params, 2));
    if (!host || (strlen(host) > 0 && !is_good_host(host)))
        return reply_error_invalid_argument(ses, pkg);
    size_t points = json_integer_value(json_array_get(params, 3));
    if (points == 0 || points > MAX_QUERY_POINTS)
        return reply_error_invalid_argument(ses, pkg);

    sds cmd = sdsempty();
    time_t start = time(NULL) / 60 * 60 - 60 * points;
    cmd = sdscatprintf(cmd, "HMGET m:%s:%s:%s:m", scope, key, host);
    for (size_t i = 0; i < points; ++i) {
        cmd = sdscatprintf(cmd, " %ld", start + i * 60);
    }

    redisReply *reply = redis_query(cmd);
    sdsfree(cmd);
    if (reply == NULL)
        return reply_error_internal_error(ses, pkg);

    json_t *result = json_array();
    for (size_t i = 0; i < reply->elements; ++i) {
        time_t timestamp = start + i * 60;
        uint64_t value = 0;
        if (reply->element[i]->type == REDIS_REPLY_STRING) {
            value = strtoull(reply->element[i]->str, NULL, 0);
        }

        json_t *unit = json_array();
        json_array_append_new(unit, json_integer(timestamp));
        json_array_append_new(unit, json_integer(value));
        json_array_append_new(result, unit);
    }

    reply_result(ses, pkg, result);
    json_decref(result);
    freeReplyObject(reply);

    return 0;
}

static int on_cmd_monitor_daily(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 4)
        return reply_error_invalid_argument(ses, pkg);
    const char *scope = json_string_value(json_array_get(params, 0));
    if (!scope || !is_good_scope(scope))
        return reply_error_invalid_argument(ses, pkg);
    const char *key = json_string_value(json_array_get(params, 1));
    if (!key || !is_good_key(key))
        return reply_error_invalid_argument(ses, pkg);
    const char *host = json_string_value(json_array_get(params, 2));
    if (!host || (strlen(host) > 0 && !is_good_host(host)))
        return reply_error_invalid_argument(ses, pkg);
    size_t points = json_integer_value(json_array_get(params, 3));
    if (points == 0 || points > MAX_QUERY_POINTS)
        return reply_error_invalid_argument(ses, pkg);

    sds cmd = sdsempty();
    time_t now = time(NULL);
    time_t start = get_day_start(now) - points * 86400;
    cmd = sdscatprintf(cmd, "HMGET m:%s:%s:%s:d", scope, key, host);
    for (size_t i = 0; i < points; ++i) {
        cmd = sdscatprintf(cmd, " %ld", start + i * 86400);
    }

    redisReply *reply = redis_query(cmd);
    sdsfree(cmd);
    if (reply == NULL)
        return reply_error_internal_error(ses, pkg);

    json_t *result = json_array();
    for (size_t i = 0; i < reply->elements; ++i) {
        time_t timestamp = start + i * 86400;
        uint64_t value = 0;
        if (reply->element[i]->type == REDIS_REPLY_STRING) {
            value = strtoull(reply->element[i]->str, NULL, 0);
        }

        json_t *unit = json_array();
        json_array_append_new(unit, json_integer(timestamp));
        json_array_append_new(unit, json_integer(value));
        json_array_append_new(result, unit);
    }

    reply_result(ses, pkg, result);
    json_decref(result);
    freeReplyObject(reply);

    return 0;
}

static void svr_on_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    json_t *params = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (params == NULL || !json_is_array(params)) {
        goto decode_error;
    }
    sds params_str = sdsnewlen(pkg->body, pkg->body_size);

    int ret;
    switch (pkg->command) {
    case CMD_MONITOR_INC:
        log_debug("from: %s cmd monitor inc, squence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_monitor_inc(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_monitor_inc %s fail: %d", params_str, ret);
        }
        break;
    case CMD_MONITOR_SET:
        log_debug("from: %s cmd monitor set, squence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_monitor_set(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_monitor_set %s fail: %d", params_str, ret);
        }
        break;
    case CMD_MONITOR_LIST_SCOPE:
        log_debug("from: %s cmd monitor list scope, squence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_monitor_list_scope(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_monitor_list_scope %s fail: %d", params_str, ret);
        }
        break;
    case CMD_MONITOR_LIST_KEY:
        log_debug("from: %s cmd monitor list key, squence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_monitor_list_key(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_monitor_list_key %s fail: %d", params_str, ret);
        }
        break;
    case CMD_MONITOR_LIST_HOST:
        log_debug("from: %s cmd monitor list host, squence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_monitor_list_host(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_monitor_list_host %s fail: %d", params_str, ret);
        }
        break;
    case CMD_MONITOR_QUERY:
        log_debug("from: %s cmd monitor query minute, squence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_monitor_query(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_monitor_list_host %s fail: %d", params_str, ret);
        }
        break;
    case CMD_MONITOR_DAILY:
        log_debug("from: %s cmd monitor query daily, squence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_monitor_daily(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_monitor_list_host %s fail: %d", params_str, ret);
        }
        break;
    default:
        log_error("from: %s unknown command: %u", nw_sock_human_addr(&ses->peer_addr), pkg->command);
        break;
    }

    sdsfree(params_str);
    json_decref(params);
    return;

decode_error:
    if (params) {
        json_decref(params);
    }
    sds hex = hexdump(pkg->body, pkg->body_size);
    log_error("connection: %s, cmd: %u decode params fail, params data: \n%s", \
            nw_sock_human_addr(&ses->peer_addr), pkg->command, hex);
    sdsfree(hex);
    rpc_svr_close_clt(svr, ses);

    return;
}

static int flush_dict(time_t end)
{
    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(monitor_val);
    while ((entry = dict_next(iter)) != NULL) {
        struct monitor_key *k = entry->key;
        if (k->timestamp >= end)
            continue;
        struct monitor_val *v = entry->val;
        if (v->val != 0) {
            redisReply *reply = redis_query("HINCRBY m:%s:m %ld %"PRIu64, k->key, k->timestamp, v->val);
            if (reply == NULL) {
                dict_release_iterator(iter);
                return -__LINE__;
            }
            freeReplyObject(reply);
        }
        dict_delete(monitor_val, k);
    }
    dict_release_iterator(iter);

    return 0;
}

static int get_monitor_value(time_t timestamp, const char *scope, const char *key, const char *host, uint64_t *val)
{
    redisReply *reply = redis_query("HGET m:%s:%s:%s:m %ld", scope, key, host, timestamp);
    if (reply == NULL)
        return -__LINE__;
    if (reply->type == REDIS_REPLY_STRING) {
        *val = strtoull(reply->str, NULL, 0);
    } else {
        *val = 0;
    }
    freeReplyObject(reply);
    return 0;
}

static int aggregate_hosts_scope_key(time_t timestamp, const char *scope, const char *key)
{
    redisReply *reply = redis_query("SMEMBERS m:%s:%s:hosts", scope, key);
    if (reply == NULL)
        return -__LINE__;
    uint64_t total_val = 0;
    for (size_t i = 0; i < reply->elements; ++i) {
        uint64_t val;
        int ret = get_monitor_value(timestamp, scope, key, reply->element[i]->str, &val);
        if (ret < 0) {
            freeReplyObject(reply);
            return ret;
        }
        total_val += val;
    }
    freeReplyObject(reply);

    if (total_val) {
        reply = redis_query("HSET m:%s:%s::m %ld %"PRIu64, scope, key, timestamp, total_val);
        if (reply == NULL)
            return -__LINE__;
        freeReplyObject(reply);
    }

    return 0;
}

static int aggregate_hosts_scope(time_t timestamp, const char *scope)
{
    redisReply *reply = redis_query("SMEMBERS m:%s:keys", scope);
    if (reply == NULL)
        return -__LINE__;
    for (size_t i = 0; i < reply->elements; ++i) {
        int ret = aggregate_hosts_scope_key(timestamp, scope, reply->element[i]->str);
        if (ret < 0) {
            freeReplyObject(reply);
            return -__LINE__;
        }
    }
    freeReplyObject(reply);

    return 0;
}

static int aggregate_hosts(time_t timestamp)
{
    redisReply *reply = redis_query("SMEMBERS m:scopes");
    if (reply == NULL)
        return -__LINE__;
    for (size_t i = 0; i < reply->elements; ++i) {
        int ret = aggregate_hosts_scope(timestamp, reply->element[i]->str);
        if (ret < 0) {
            freeReplyObject(reply);
            return ret;
        }
    }
    freeReplyObject(reply);

    return 0;
}

static int clear_key(time_t timestamp, const char *scope, const char *key, const char *host)
{
    time_t end = timestamp - MAX_KEEP_DAYS * 86400;
    redisReply *reply = redis_query("HGETALL m:%s:%s:%s:m", scope, key, host);
    if (reply == NULL)
        return -__LINE__;

    sds cmd = sdsempty();
    cmd = sdscatprintf(cmd, "HDEL m:%s:%s:%s:m", scope, key, host);

    bool has_delete = false;
    for (size_t i = 0; i < reply->elements; i += 2) {
        time_t t = strtol(reply->element[i]->str, NULL, 0);
        if (t >= end) {
            continue;
        }
        cmd = sdscatprintf(cmd, " %ld", t);
        has_delete = true;
    }
    freeReplyObject(reply);

    if (has_delete) {
        reply = redis_query(cmd);
        if (reply == NULL) {
            sdsfree(cmd);
            return -__LINE__;
        }
    }
    sdsfree(cmd);

    return 0;
}

static int aggregate_daily_scope_key_host(time_t timestamp, const char *scope, const char *key, const char *host)
{
    sds cmd = sdsempty();
    cmd = sdscatprintf(cmd, "HMGET m:%s:%s:%s:m", scope, key, host);
    for (size_t i = 0; i < 60 * 24; ++i) {
        cmd = sdscatprintf(cmd, " %ld", timestamp + 60 * i);
    }

    uint64_t total_val = 0;
    redisReply *reply = redis_query(cmd);
    if (reply == NULL)
        return -__LINE__;
    for (int i = 0; i < reply->elements; i += 2) {
        if (reply->element[i + 1]->type == REDIS_REPLY_STRING) {
            total_val += strtoull(reply->element[i + 1]->str, NULL, 0);
        }
    }
    freeReplyObject(reply);

    if (total_val) {
        reply = redis_query("HSET m:%s:%s:%s:d %ld %"PRIu64, scope, key, host, timestamp, total_val);
        if (reply == NULL)
            return -__LINE__;
        freeReplyObject(reply);
    }

    ERR_RET(clear_key(timestamp, scope, key, host));

    return 0;
}

static int aggregate_daily_scope_key(time_t timestamp, const char *scope, const char *key)
{
    ERR_RET(aggregate_daily_scope_key_host(timestamp, scope, key, ""));

    redisReply *reply = redis_query("SMEMBERS m:%s:%s:hosts", scope, key);
    if (reply == NULL)
        return -__LINE__;
    for (size_t i = 0; i < reply->elements; ++i) {
        int ret = aggregate_daily_scope_key_host(timestamp, scope, key, reply->element[i]->str);
        if (ret < 0) {
            freeReplyObject(reply);
            return ret;
        }
    }
    freeReplyObject(reply);

    return 0;
}

static int aggregate_daily_scope(time_t timestamp, const char *scope)
{
    redisReply *reply = redis_query("SMEMBERS m:%s:keys", scope);
    if (reply == NULL)
        return -__LINE__;
    for (size_t i = 0; i < reply->elements; ++i) {
        int ret = aggregate_daily_scope_key(timestamp, scope, reply->element[i]->str);
        if (ret < 0) {
            freeReplyObject(reply);
            return -__LINE__;
        }
    }
    freeReplyObject(reply);

    return 0;
}

static int aggregate_daily(time_t timestamp)
{
    redisReply *reply = redis_query("SMEMBERS m:scopes");
    if (reply == NULL)
        return -__LINE__;
    for (size_t i = 0; i < reply->elements; ++i) {
        int ret = aggregate_daily_scope(timestamp, reply->element[i]->str);
        if (ret < 0) {
            freeReplyObject(reply);
            return -__LINE__;
        }
    }
    freeReplyObject(reply);

    return 0;
}

static int set_last_aggregate_time(const char *key, time_t val)
{
    redisReply *reply = redis_query("SET %s %ld", key, val);
    if (reply == NULL)
        return -__LINE__;
    freeReplyObject(reply);
    return 0;
}

static int check_aggregate(time_t now)
{
    bool m_update = false;
    time_t m_start = now / 60 * 60;
    while (last_aggregate_hosts < (m_start - 60)) {
        ERR_RET(aggregate_hosts(last_aggregate_hosts + 60));
        last_aggregate_hosts += 60;
        m_update = true;
    }
    if (m_update) {
        ERR_RET(set_last_aggregate_time("m:last_aggregate_hosts", last_aggregate_hosts));
    }

    bool d_update = false;
    time_t d_start = get_day_start(now);
    while (last_aggregate_daily < (d_start - 86400)) {
        ERR_RET(aggregate_daily(last_aggregate_daily + 86400));
        last_aggregate_daily += 86400;
        d_update = true;
    }
    if (d_update) {
        ERR_RET(set_last_aggregate_time("m:last_aggregate_daily", last_aggregate_daily));
    }

    return 0;
}

static int flush_data(time_t now)
{
    double begin = current_timestamp();
    ERR_RET(flush_dict(now / 60 * 60));
    ERR_RET(check_aggregate(now));
    double end = current_timestamp();
    log_info("flush data success, cost time: %f", end - begin);

    return 0;
}

static void on_timer(nw_timer *timer, void *privdata)
{
    static bool flush_error = false;
    static time_t flush_last = 0;
    static time_t flush_error_start = 0;

    time_t now = time(NULL);
    if (flush_error) {
        int ret = flush_data(now);
        if (ret < 0) {
            log_error("flush_data to redis fail: %d", ret);
            if ((now - flush_error_start) >= 60) {
                log_fatal("flush_data to redis fail last %ld seconds!", now - flush_error_start);
            }
        } else {
            flush_error = false;
            flush_error_start = 0;
        }
    } else if (now - flush_last >= 60) {
        flush_last = now / 60 * 60;
        int ret = flush_data(now);
        if (ret < 0) {
            log_error("flush_data to redis fail: %d", ret);
            flush_error = true;
            flush_error_start = now;
        }
    }
}

static void svr_on_new_connection(nw_ses *ses)
{
    log_trace("new connection: %s", nw_sock_human_addr(&ses->peer_addr));
}

static void svr_on_connection_close(nw_ses *ses)
{
    log_trace("connection: %s close", nw_sock_human_addr(&ses->peer_addr));
}

static uint32_t set_dict_hash_func(const void *key)
{
    return dict_generic_hash_function(key, strlen((char *)key));
}

static int set_dict_key_compare(const void *key1, const void *key2)
{
    return strcmp((char *)key1, (char *)key2);
}

static void *set_dict_key_dup(const void *key)
{
    return strdup((char *)key);
}

static void set_dict_key_free(void *key)
{
    free(key);
}

static uint32_t val_dict_hash_func(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct monitor_key));
}

static int val_dict_key_compare(const void *key1, const void *key2)
{
    return memcmp(key1, key2, sizeof(struct monitor_key));
}

static void *val_dict_key_dup(const void *key)
{
    struct monitor_key *obj = malloc(sizeof(struct monitor_key));
    memcpy(obj, key, sizeof(struct monitor_key));
    return obj;
}

static void val_dict_key_free(void *key)
{
    free(key);
}

static void *val_dict_val_dup(const void *val)
{
    struct monitor_val *obj = malloc(sizeof(struct monitor_val));
    memcpy(obj, val, sizeof(struct monitor_val));
    return obj;
}

static void val_dict_val_free(void *val)
{
    free(val);
}

static int init_dict(void)
{
    dict_types type;
    memset(&type, 0, sizeof(type));
    type.hash_function  = set_dict_hash_func;
    type.key_compare    = set_dict_key_compare;
    type.key_dup        = set_dict_key_dup;
    type.key_destructor = set_dict_key_free;

    monitor_set = dict_create(&type, 64);
    if (monitor_set == NULL)
        return -__LINE__;

    memset(&type, 0, sizeof(type));
    type.hash_function  = val_dict_hash_func;
    type.key_compare    = val_dict_key_compare;
    type.key_dup        = val_dict_key_dup;
    type.key_destructor = val_dict_key_free;
    type.val_dup        = val_dict_val_dup;
    type.val_destructor = val_dict_val_free;

    monitor_val = dict_create(&type, 64);
    if (monitor_val == NULL)
        return -__LINE__;

    return 0;
}

static int init_time(void)
{
    time_t now = time(NULL);
    redisReply *reply;

    reply = redis_query("GET k:last_aggregate_hosts");
    if (reply == NULL)
        return -__LINE__;
    if (reply->type == REDIS_REPLY_STRING) {
        last_aggregate_hosts = strtol(reply->str, NULL, 0);
    } else {
        last_aggregate_hosts = now / 60 * 60 - 60;
    }
    freeReplyObject(reply);

    reply = redis_query("GET k:last_aggregate_daily");
    if (reply == NULL)
        return -__LINE__;

    if (reply->type == REDIS_REPLY_STRING) {
        last_aggregate_daily = strtol(reply->str, NULL, 0);
    } else {
        last_aggregate_daily = get_day_start(now) - 86400;
    }
    freeReplyObject(reply);

    return 0;
}

int init_server(void)
{
    rpc_svr_type type;
    memset(&type, 0, sizeof(type));
    type.on_recv_pkg = svr_on_recv_pkg;
    type.on_new_connection = svr_on_new_connection;
    type.on_connection_close = svr_on_connection_close;

    svr = rpc_svr_create(&settings.svr, &type);
    if (svr == NULL)
        return -__LINE__;
    if (rpc_svr_start(svr) < 0)
        return -__LINE__;

    redis = redis_sentinel_create(&settings.redis);
    if (redis == NULL)
        return -__LINE__;
    redis_store = redis_sentinel_connect_master(redis);
    if (redis_store == NULL)
        return -__LINE__;

    ERR_RET(init_dict());
    ERR_RET(init_time());

    nw_timer_set(&timer, 1.0, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}

void writer_flush(void)
{
    flush_data(time(NULL));
}

