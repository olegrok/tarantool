test_run = require('test_run').new()
test_run:create_cluster(CLUSTER, 'promote')
test_run:wait_fullmesh(CLUSTER)
--
-- Check the promote actually allows to switch the master.
--
_ = test_run:switch('box1')
-- Box1 is a master.
box.cfg.read_only

_ = test_run:switch('box2')
-- Box2 is a slave.
box.cfg.read_only
-- And can not do DDL/DML.
box.schema.create_space('test') -- Fail.

box.ctl.promote()
promote_info()
-- Now the slave has become a master.
box.cfg.read_only
-- And can do DDL/DML.
s = box.schema.create_space('test')
s:drop()

_ = test_run:switch('box1')
-- In turn, the old master is a slave now.
box.cfg.read_only
promote_info()
-- For him any DDL/DML is forbidden.
box.schema.create_space('test2')

-- Check a watcher state.
_ = test_run:switch('box3')
promote_info()

--
-- Clear the basic successfull test and try different errors.
--
_ = test_run:switch('box2')
box.ctl.promote_reset()
promotion_history()

prom = box.space._promotion

-- Invalid UUIDs.
prom:insert{1, 'invalid', 1, box.info.uuid, 1, 't'}
prom:insert{1, box.info.uuid, 1, 'invalid', 1, 't'}
-- Invalid ts.
prom:insert{1, box.info.uuid, 1, box.info.uuid, -1, 't'}
-- Invalid type.
prom:insert{1, box.info.uuid, 1, box.info.uuid, 1, 'invalid'}
-- Invalid type-specific options.
prom:insert{1, box.info.uuid, 1, box.info.uuid, 1, 'begin', {quorum = 1}}
prom:insert{1, box.info.uuid, 1, box.info.uuid, 1, 'begin', {quorum = 'invalid', timeout = 1}}

map = setmetatable({}, {__serialize = 'map'})
prom:insert{1, box.info.uuid, 1, box.info.uuid, 1, 'status', map}
prom:insert{1, box.info.uuid, 1, box.info.uuid, 1, 'status', {is_master = 'invalid'}}

prom:insert{1, box.info.uuid, 1, box.info.uuid, 1, 'error', map}
prom:insert{1, box.info.uuid, 1, box.info.uuid, 1, 'error', {code = 'code', message = 'msg'}}

prom:insert{1, box.info.uuid, 1, box.info.uuid, 1, 'sync', map}
prom:insert{1, box.info.uuid, 1, box.info.uuid, 1, 'success', map}

--
-- Test simple invalid scenarios.
--

-- Already master.
box.ctl.promote()
_ = test_run:switch('box1')
-- Small quorum.
box.ctl.promote({quorum = 2})
-- Two masters.
box.cfg{read_only = false}
_ = test_run:switch('box3')
promote_check_error()
promotion_history_find_masters()
box.cfg.read_only
_ = test_run:switch('box1')
box.cfg.read_only
_ = test_run:switch('box2')
box.cfg.read_only
_ = test_run:switch('box4')
box.cfg.read_only
-- Box.cfg.read_only became immutable when promote had been
-- called.
box.cfg{read_only = false}

--
-- Test recovery after failed promotion.
--
_ = test_run:cmd('restart server box2')
_ = test_run:cmd('restart server box3')
_ = test_run:switch('box2')
info = promote_info()
info.old_master_uuid == 'box1' or info.old_master_uuid == 'box2'
info.old_master_uuid = nil
info.comment = info.comment:match('two masters exist')
info
_ = test_run:switch('box3')
info = promote_info()
info.old_master_uuid == 'box1' or info.old_master_uuid == 'box2'
info.old_master_uuid = nil
info

--
-- Test timeout.
--
_ = test_run:switch('box1')
box.ctl.promote_reset()
box.cfg{read_only = true}
-- Now box2 is a single master.
_ = test_run:switch('box3')
promote_check_error({timeout = 0.00001})
promote_info()

--
-- Test the case when the cluster is not read-only, but a single
-- master is not available now. In such a case the promote()
-- should fail regardless of quorum.
--
_ = test_run:cmd('stop server box2')
box.ctl.promote_reset()
-- Quorum is 3 to test that the quorum must contain an old master.
promote_check_error({timeout = 0.5, quorum = 3})
promote_info()
_ = test_run:switch('box1')
_ = test_run:cmd('stop server box3')
_ = test_run:cmd('start server box2')
_ = test_run:switch('box2')
info = promote_info({'round_id', 'comment', 'phase', 'round_uuid'})
info.comment = info.comment:match('Timeout exceeded')
info

_ = test_run:cmd('start server box3')
_ = test_run:switch('box3')
promote_info({'round_id', 'comment', 'phase', 'round_uuid', 'role'})

--
-- Test promotion in a completely read-only cluster.
--
_ = test_run:switch('box2')
box.ctl.promote_reset()
box.cfg{read_only = true}
box.ctl.promote()
promote_info()

--
-- Test promotion reset of several rounds.
--
_ = test_run:switch('box3')
box.ctl.promote()
promote_info()
box.ctl.promote_reset()
promotion_history()

_ = test_run:switch('default')
test_run:drop_cluster(CLUSTER)
