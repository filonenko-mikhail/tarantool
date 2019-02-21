local ffi = require('ffi')
local merger = require('merger')

local function get_once(...)
    local fired = false
    local data = {...}
    return function()
        if fired then return nil end
        fired = true
        return unpack(data)
    end
end

local struct_ibuf_ctype = ffi.typeof('struct ibuf')

-- Create a source from one buffer.
merger.new_source_frombuffer = function(buf)
    local func_name = 'merger.new_source_frombuffer'
    if type(buf) ~= 'cdata' or not ffi.istype(struct_ibuf_ctype, buf) then
        error(('Usage: %s(<cdata<struct ibuf>>)'):format(func_name), 0)
    end

    return merger.new_buffer_source(get_once(buf))
end

-- Create a source from one table.
merger.new_source_fromtable = function(tbl)
    local func_name = 'merger.new_source_fromtable'
    if type(tbl) ~= 'table' then
        error(('Usage: %s(<table>)'):format(func_name), 0)
    end

    return merger.new_table_source(get_once(tbl))
end

-- Create a source from one iterator.
merger.new_source_fromiterator = function(gen, param, state)
    -- We cannot check whether 'gen' is callable from Lua,
    -- because we cannot get cdata metatype to check for a
    -- __call field (see also gh-3915 and luaL_iscallable()).
    return merger.new_iterator_source(get_once(gen, param, state))
end
