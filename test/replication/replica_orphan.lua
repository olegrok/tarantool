#!/usr/bin/env tarantool

local TIMEOUT = tonumber(arg[1])

box.cfg({
    listen              = os.getenv("LISTEN"),
    replication         = os.getenv("MASTER"),
    replication_connect_timeout = 0.5,
})

require('console').listen(os.getenv('ADMIN'))
