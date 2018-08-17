test_run = require('test_run').new()
engine = test_run:get_cfg('engine')
box.sql.execute('pragma sql_default_engine=\''..engine..'\'')

box.cfg{}

box.sql.execute("CREATE TABLE t1 (s1 INTEGER PRIMARY KEY AUTOINCREMENT, s2 INTEGER, CHECK (s1 <> 19));");
box.sql.execute("CREATE TABLE t2 (s1 INTEGER PRIMARY KEY AUTOINCREMENT, s2 INTEGER, CHECK (s1 <> 19 AND s1 <> 25));");
box.sql.execute("CREATE TABLE t3 (s1 INTEGER PRIMARY KEY AUTOINCREMENT, s2 INTEGER, CHECK (s1 < 10));");
box.sql.execute("CREATE TABLE t4 (s1 INTEGER PRIMARY KEY AUTOINCREMENT, s2 INTEGER, CHECK (s1 <> 19));");

box.sql.execute("insert into t1 values (18, null);")
box.sql.execute("insert into t1(s2) values (null);")

box.sql.execute("insert into t2 values (18, null);")
box.sql.execute("insert into t2(s2) values (null);")
box.sql.execute("insert into t2 values (24, null);")
box.sql.execute("insert into t2(s2) values (null);")

box.sql.execute("insert into t3 values (9, null)")
box.sql.execute("insert into t3(s2) values (null)")

box.sql.execute("insert into t4 values (18, null);")
box.sql.execute("insert into t4 values (null, null);")

box.sql.execute("DROP TABLE t1")
box.sql.execute("DROP TABLE t2")
box.sql.execute("DROP TABLE t3")
box.sql.execute("DROP TABLE t4")
