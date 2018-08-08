-- Issue 3105: Test logging of request with broken lsn before panicking
env = require('test_run')
test_run = env.new()

test_run:cmd('create server panic_broken_lsn with script="xlog/panic_broken_lsn.lua"')
test_run:cmd('start server panic_broken_lsn')
test_run:switch('panic_broken_lsn')

box.space._schema:replace{"t0", "v0"}
box.snapshot()
box.space._schema:replace{"t0", "v1"}
box.snapshot()
box.space._schema:replace{"t0", "v2"}
box.snapshot()

test_run:switch('default')
test_run:cmd('stop server panic_broken_lsn')

fio = require('fio')

dirname = fio.pathjoin(fio.cwd(), "panic_broken_lsn")

xlogs = fio.glob(dirname .. "/*.xlog")

wal1_name = xlogs[#xlogs - 2]
wal2_name = xlogs[#xlogs - 1]

snaps = fio.glob(dirname .. "/*.snap")

-- Remove empty xlog
fio.unlink(xlogs[#xlogs])

-- Remove two last snapshots
fio.unlink(snaps[#snaps])
fio.unlink(snaps[#snaps - 1])

buffer = require('buffer')
ffi = require('ffi')

test_run:cmd("setopt delimiter ';'")
function read_file(filepath)
    local fh = fio.open(filepath, {'O_RDONLY'})
    local size = fh:stat().size
    local buf = buffer.ibuf()
    fh:read(buf:reserve(size))    
    fh:close()
    buf:alloc(size)
    return buf
end;
function find_marker_pos(buf)
    local sz = buf:size()
    local data = ffi.string(buf.rpos, sz)
    local cnt = 0    
    for i = 1, sz do
        local b = string.byte(data, i)        
        if (cnt == 0 and b == 213) then
            cnt = 1            
        elseif (cnt == 1 and b == 186) then
            cnt = 2            
        elseif (cnt == 2 and b == 11) then
            cnt = 3            
        elseif (cnt == 3 and b == 171) then    
            return i - 3
        else
            cnt = 0
        end
    end
    return 0
end;
function run_panic()
    local tarantool_bin = arg[-1]
    local fmt = [[/bin/sh -c 'cd "%s" && "%s" ../panic_broken_lsn.lua']]
    local cmd = string.format(fmt, dirname, tarantool_bin)
    local res = os.execute(cmd)
    return res
end;
test_run:cmd("setopt delimiter ''");

-- Read WAL 1 and find position of data
buf1 = read_file(wal1_name)
pos1 = find_marker_pos(buf1)

-- Read WAL 2 and find position of data
buf2 = read_file(wal2_name)
pos2 = find_marker_pos(buf2)

-- Create fake WAL file with header of WAL 2 and data of WAL 1 
tmp_file_name = wal2_name .. ".tmp"

fh3 = fio.open(tmp_file_name, {'O_WRONLY', 'O_CREAT'})
fh3:write(buf2.rpos, pos2)
fh3:write(buf1.rpos + pos1, buf1:size() - pos1)
fh3:close()
fio.chmod(tmp_file_name, 0x1B0)

-- Replace WAL 2 with fake WAL file
fio.unlink(wal2_name)
fio.copyfile(tmp_file_name, wal2_name)
fio.unlink(tmp_file_name)

-- Try to start tarantool with xlog containing broken LSN
run_panic()

-- Check that log contains the mention of broken LSN and the request printout
test_run:grep_log('default', "LSN for 1 is used twice or COMMIT order is broken: confirmed: 2, new: 2, req: {.*}")
