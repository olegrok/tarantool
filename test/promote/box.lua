#!/usr/bin/env tarantool
os = require('os')

box.cfg{ listen = os.getenv("LISTEN") }

CLUSTER = { 'box1', 'box2', 'box3', 'box4' }

require('console').listen(os.getenv('ADMIN'))
