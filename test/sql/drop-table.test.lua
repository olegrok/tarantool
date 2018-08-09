test_run = require('test_run').new()
engine = test_run:get_cfg('engine')
box.sql.execute('pragma sql_default_engine=\''..engine..'\'')

-- box.cfg()

-- create space
box.sql.execute("CREATE TABLE zzzoobar (c1, c2 PRIMARY KEY, c3, c4)")

-- Debug
-- box.sql.execute("PRAGMA vdbe_debug=ON ; INSERT INTO zzzoobar VALUES (111, 222, 'c3', 444)")

box.sql.execute("CREATE INDEX zb ON zzzoobar(c1, c3)")

-- Dummy entry
box.sql.execute("INSERT INTO zzzoobar VALUES (111, 222, 'c3', 444)")

box.sql.execute("DROP TABLE zzzoobar")

-- Table does not exist anymore. Should error here.
box.sql.execute("INSERT INTO zzzoobar VALUES (111, 222, 'c3', 444)")

-- Cleanup
-- DROP TABLE should do the job

-- Debug
-- require("console").start()

-- gh-3592: net.box segmentation fault after "create table" with
-- autoincrement
test_run = require('test_run').new()
box.schema.user.create('tmp')
box.schema.user.grant('tmp', 'create', 'space')
box.schema.user.grant('tmp', 'write', 'space', '_space')
box.schema.user.grant('tmp', 'write', 'space', '_schema')
box.session.su('tmp')
-- Error - nothing should be created
box.sql.execute('create table t (id integer primary key, a integer)')
box.space.T
box.sql.execute('drop table t')
test_run:cmd('restart server default');
box.schema.user.drop('tmp')
