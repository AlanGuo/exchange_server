#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
from redis.sentinel import Sentinel

SENTINEL_LIST = [("127.0.0.1", 26381), ("127.0.0.1", 26382), ("127.0.0.1", 26383)]
SENTINEL_NAME = "mymaster"

def main(scope):
    sentinel = Sentinel(SENTINEL_LIST)
    client = sentinel.master_for(SENTINEL_NAME)
    keys = client.smembers("m:%s:keys" % scope)
    for key in keys:
        print key
        hosts = client.smembers("m:%s:%s:hosts" % (scope, key))
        for host in hosts:
            client.delete("m:%s:%s:%s:m" % (scope, key, host))
            client.delete("m:%s:%s:%s:d" % (scope, key, host))
        client.delete("m:%s:%s:m" % (scope, key))
        client.delete("m:%s:%s:d" % (scope, key))
        client.delete("m:%s:%s:hosts" % (scope, key))
    client.delete("m:%s:keys" % scope)
    client.srem("m:scopes", scope)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "usage: %s scope" % sys.argv[0]
        sys.exit(1)
    main(sys.argv[1])
