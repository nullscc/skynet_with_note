local skynet = require "skynet"
local c = require "skynet.core"

function skynet.launch(...)
	local addr = c.command("LAUNCH", table.concat({...}," "))
	if addr then
		return tonumber("0x" .. string.sub(addr , 2))
	end
end

function skynet.kill(name)
	if type(name) == "number" then
		skynet.send(".launcher","lua","REMOVE",name, true)
		name = skynet.address(name)
	end
	c.command("KILL",name)
end

function skynet.abort()
	c.command("ABORT")
end

-- 主要是看名字是不是以"."开头,如果不是"."开头，则注册一个整个skynet网络都有效的字符串地址
local function globalname(name, handle)
	local c = string.sub(name,1,1)
	assert(c ~= ':') --字符串地址不能是冒号开头
	if c == '.' then
		return false
	end

	-- 字符串地址长度不能超过16个字符
	assert(#name <= 16)	-- GLOBALNAME_LENGTH is 16, defined in skynet_harbor.h
	assert(tonumber(name) == nil)	-- global name can't be number

	local harbor = require "skynet.harbor"

	harbor.globalname(name, handle)

	return true
end

function skynet.register(name)
	if not globalname(name) then --以"."开头都返回false
		c.command("REG", name)
	end
end

function skynet.name(name, handle)
	if not globalname(name, handle) then --以"."开头都返回false
		c.command("NAME", name .. " " .. skynet.address(handle)) -- 返回地址的字符串形式,以冒号开头
	end
end

local dispatch_message = skynet.dispatch_message

function skynet.forward_type(map, start_func)
	c.callback(function(ptype, msg, sz, ...)
		local prototype = map[ptype]
		if prototype then
			dispatch_message(prototype, msg, sz, ...)
		else
			dispatch_message(ptype, msg, sz, ...)
			c.trash(msg, sz)
		end
	end, true)
	skynet.timeout(0, function()
		skynet.init_service(start_func)
	end)
end

function skynet.filter(f ,start_func)
	c.callback(function(...)
		dispatch_message(f(...))
	end)
	skynet.timeout(0, function()
		skynet.init_service(start_func)
	end)
end

function skynet.monitor(service, query)
	local monitor
	if query then
		monitor = skynet.queryservice(true, service)
	else
		monitor = skynet.uniqueservice(true, service)
	end
	assert(monitor, "Monitor launch failed")
	c.command("MONITOR", string.format(":%08x", monitor))
	return monitor
end

return skynet
