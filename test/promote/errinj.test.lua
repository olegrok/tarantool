test_run = require('test_run').new()
test_run:create_cluster(CLUSTER, 'promote')
test_run:wait_fullmesh(CLUSTER)
--
-- Test the case when two different promotions are started at the
-- same time. Here the initiators are box2 and box3 while box1 is
-- an old master and box4 is a watcher.
--
_ = test_run:switch('box1')
errinj.set("ERRINJ_WAL_DELAY", true)

_ = test_run:switch('box2')
errinj.set("ERRINJ_WAL_DELAY", true)

_ = test_run:switch('box3')
errinj.set("ERRINJ_WAL_DELAY", true)

_ = test_run:switch('box2')
err = nil
ok = nil
_ = fiber.create(function() ok, err = promote_check_error() end)

_ = test_run:switch('box3')
err = nil
ok = nil
f = fiber.create(function() ok, err = promote_check_error() end)
while f:status() ~= 'suspended' do fiber.sleep(0.01) end
errinj.set("ERRINJ_WAL_DELAY", false)

_ = test_run:switch('box2')
errinj.set("ERRINJ_WAL_DELAY", false)
while not err do fiber.sleep(0.01) end
ok, err

_ = test_run:switch('box1')
errinj.set("ERRINJ_WAL_DELAY", false)
while promote_info().phase ~= 'error' do fiber.sleep(0.01) end
info = promote_info()
info.comment = info.comment:match('unexpected message')
info

_ = test_run:switch('box3')
while not err do fiber.sleep(0.01) end
ok, err

--
-- Test that after all a new promotion works.
--
box.ctl.promote()
promote_info()

--
-- Test the case when during a promotion round an initiator is
-- restarted after sending 'begin' and the round had been failed
-- on timeout. On recovery the initiator has to detect by 'begin'
-- that it was read only and make 'read_only' option be immutable
-- for a user despite the fact that 'status' is never sent by this
-- instance.
--
-- The test plan: disable watchers, start a promotion round, turn
-- the initiator off, wait until the round is failed due to
-- timeout, turn the initiator on. It should catch its own
-- begin + error and went to read only mode, even if box.cfg was
-- called with read_only = false.
--
_ = test_run:cmd('stop server box2')
_ = test_run:cmd('stop server box4')
-- Box1 is an initiator, box3 is an old master.
_ = test_run:switch('box1')
-- Do reset and snapshot to do not replay the previous round on
-- restart.
box.ctl.promote_reset()
box.snapshot()
_ = fiber.create(function() box.ctl.promote({timeout = 0.1}) end)
_ = test_run:switch('box3')
while box.space._promotion:count() == 0 do fiber.sleep(0.01) end
_ = test_run:cmd('stop server box1')
while box.ctl.promote_info().phase ~= 'error' do fiber.sleep(0.01) end
_ = test_run:cmd('start server box1')
_ = test_run:switch('box1')
promote_info()
box.cfg.read_only
_ = test_run:cmd('start server box2')
_ = test_run:cmd('start server box4')

_ = test_run:switch('default')
test_run:drop_cluster(CLUSTER)
