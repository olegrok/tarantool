test_run = require('test_run').new()
fiber = require('fiber')
fio = require('fio')

test_run:cleanup_cluster()

-- Make each snapshot trigger garbage collection.
default_checkpoint_count = box.cfg.checkpoint_count
box.cfg{checkpoint_count = 1}

-- Temporary space for bumping lsn.
temp = box.schema.space.create('temp')
_ = temp:create_index('pk')

s = box.schema.space.create('test', {engine='vinyl'})
_ = s:create_index('pk', {run_count_per_level=1})

path = fio.pathjoin(box.cfg.vinyl_dir, tostring(s.id), tostring(s.index.pk.id))

function ls_data() return fio.glob(fio.pathjoin(path, '*')) end
function ls_vylog() return fio.glob(fio.pathjoin(box.cfg.vinyl_dir, '*.vylog')) end
function gc_info() return box.info.gc() end
function gc() temp:auto_increment{} box.snapshot() end

-- Check that run files are deleted by gc.
s:insert{1} box.snapshot() -- dump
s:insert{2} box.snapshot() -- dump + compaction
while s.index.pk:stat().run_count > 1 do fiber.sleep(0.01) end -- wait for compaction
gc()
files = ls_data()
#files == 2 or {files, gc_info()}

-- Check that gc keeps the current and previous log files.
files = ls_vylog()
#files == 2 or {files, gc_info()}

-- Check that files left from dropped indexes are deleted by gc.
s:drop()
gc()
files = ls_data()
#files == 0 or {files, gc_info()}

--
-- Check that vylog files are removed if vinyl is not used.
--

-- This purges records corresponding to dropped runs, but
-- dropped index records are still stored in vylog.
gc()
files = ls_vylog()
#files == 2 or {files, gc_info()}

-- All records should have been purged from the log by now
-- so we should only keep the previous log file.
gc()
files = ls_vylog()
#files == 1 or {files, gc_info()}

-- The previous log file should be removed by the next gc.
gc()
files = ls_vylog()
#files == 0 or {files, gc_info()}

temp:drop()

box.cfg{checkpoint_count = default_checkpoint_count}

--
-- Check that compacted run files that are not referenced
-- by any checkpoint are deleted immediately (gh-3407).
--
test_run:cmd("create server test with script='vinyl/low_quota.lua'")
test_run:cmd("start server test with args='1048576'")
test_run:cmd('switch test')

box.cfg{checkpoint_count = 2}

fio = require('fio')
fiber = require('fiber')

s = box.schema.space.create('test', {engine = 'vinyl'})
_ = s:create_index('pk', {run_count_per_level = 3})

function count_runs() return #fio.glob(fio.pathjoin(box.cfg.vinyl_dir, s.id, s.index.pk.id, '*.run')) end

_ = s:replace{1}
box.snapshot()
_ = s:replace{2}
box.snapshot()

count_runs() -- 2

for i = 1, 20 do s:replace{i, string.rep('x', 100 * 1024)} end
while s.index.pk:stat().disk.compact.count < 1 do fiber.sleep(0.001) end
s.index.pk:stat().disk.compact.count -- 1

count_runs() -- 3 (compacted runs created after checkpoint are deleted)

test_run:cmd('switch default')
test_run:cmd("stop server test")
test_run:cmd("cleanup server test")
