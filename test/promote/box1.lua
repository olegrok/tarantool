#!/usr/bin/env tarantool

local INSTANCE_ID = string.match(arg[0], "%d")
local SOCKET_DIR = require('fio').cwd()
local read_only = INSTANCE_ID ~= '1'
local function instance_uri(instance_id)
    return SOCKET_DIR..'/promote'..instance_id..'.sock';
end
local uuid_prefix = '4d71c17c-8c50-11e8-9eb6-529269fb145'
local uuid_to_name = {}
for i = 1, 4 do
    local uuid = uuid_prefix..tostring(i)
    uuid_to_name[uuid] = 'box'..tostring(i)
end
require('console').listen(os.getenv('ADMIN'))

fiber = require('fiber')
errinj = box.error.injection

box.cfg({
    listen = instance_uri(INSTANCE_ID),
    replication = {instance_uri(1), instance_uri(2),
                   instance_uri(3), instance_uri(4)},
    read_only = read_only,
    replication_connect_timeout = 0.1,
    replication_timeout = 0.1,
    instance_uuid = uuid_prefix..tostring(INSTANCE_ID),
})

local round_uuid_to_id = {}

function uuid_free_str(str)
    for uuid, id in pairs(round_uuid_to_id) do
        local template = string.gsub(uuid, '%-', '%%-')
        str = string.gsub(str, template, 'round_'..tostring(id))
    end
    for uuid, name in pairs(uuid_to_name) do
        local template = string.gsub(uuid, '%-', '%%-')
        str = string.gsub(str, template, name)
    end
    return str
end

function promotion_history()
    local ret = {}
    local prev_round_uuid
    for i, t in box.space._promotion:pairs() do
        t = setmetatable(t:tomap({names_only = true}), {__serialize = 'map'})
        round_uuid_to_id[t.round_uuid] = t.id
        t.round_uuid = 'round_'..tostring(t.id)
        t.source_uuid = uuid_to_name[t.source_uuid]
        t.ts = nil
        if t.value == box.NULL then
            t.value = nil
        end
        if t.type == 'error' then
            t.value.message = uuid_free_str(t.value.message)
        end
        table.insert(ret, t)
    end
    return ret
end

-- For recovery rescan round_uuids.
promotion_history()

function promote_check_error(...)
    local ok, err = box.ctl.promote(...)
    if not ok then
        promotion_history()
        err = uuid_free_str(err:unpack().message)
    end
    return ok, err
end

function promotion_history_find_masters()
    local res = {}
    for _, record in pairs(promotion_history()) do
        if record.type == 'status' and record.value.is_master then
            table.insert(res, record)
        end
    end
    return res
end

function promote_info(fields)
    local info = box.ctl.promote_info()
    if fields then
        local tmp = {}
        for _, k in pairs(fields) do
            tmp[k] = info[k]
        end
        info = tmp
    end
    if info.old_master_uuid then
        info.old_master_uuid = uuid_free_str(info.old_master_uuid)
    end
    if info.round_uuid then
        info.round_uuid = 'round_'..tostring(info.round_id)
    end
    if info.initiator_uuid then
        info.initiator_uuid = uuid_free_str(info.initiator_uuid)
    end
    if info.comment then
        info.comment = uuid_free_str(info.comment)
    end
    return info
end

box.once("bootstrap", function()
    box.schema.user.grant('guest', 'read,write,execute', 'universe')
end)
