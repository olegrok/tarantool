--
-- gh-3427: no sync after configuration update
--

env = require('test_run')
test_run = env.new()
engine = test_run:get_cfg('engine')

box.schema.user.grant('guest', 'read,write,execute', 'universe')

box.schema.user.grant('guest', 'replication')
test_run:cmd("create server replica with rpl_master=default, script='replication/replica_orphan.lua'")
test_run:cmd("start server replica")

repl = test_run:eval('replica', 'return box.cfg.listen')[1]
box.cfg{replication = repl}

test_run:cmd("switch replica")
test_run:cmd("switch default")

s = box.schema.space.create('test', {engine = engine});
index = s:create_index('primary')

-- change replica configuration
test_run:cmd("switch replica")
box.cfg{replication={}}

test_run:cmd("switch default")
-- insert values on the master while replica is unconfigured
a = 100000 while a > 0 do a = a-1 box.space.test:insert{a,'A'..a} end

test_run:cmd("switch replica")
box.cfg{replication = os.getenv("MASTER")}

test_run:cmd("switch default")
test_run:cmd("switch replica")

box.info.replication[1].upstream.lag > 1
test_run:cmd("switch default")

-- cleanup
test_run:cmd("stop server replica")
test_run:cmd("cleanup server replica")
box.space.test:drop()
box.schema.user.revoke('guest', 'replication')
box.schema.user.revoke('guest', 'read,write,execute', 'universe')

