test_run = require('test_run').new()
engine = test_run:get_cfg('engine')
nb = require('net.box')

box.sql.execute("PRAGMA sql_default_engine='"..engine.."'")
box.sql.execute("CREATE TABLE te37 (s1 INT PRIMARY KEY);")
box.sql.execute("INSERT INTO te37 VALUES (1);")

box.schema.user.grant('guest','read,write,execute','universe')
c = nb.connect(box.cfg.listen)
c:execute("SELECT * FROM te37;")

box.schema.user.revoke('guest','read','universe')
c = nb.connect(box.cfg.listen)
c:execute("SELECT * FROM te37;")

-- Cleanup
box.sql.execute("DROP TABLE te37")
