local skynet = require "skynet"
require "skynet.manager"	-- import skynet.register
local snax = require "snax"

local cmd = {}
local service = {}

local function request(name, func, ...)	-- 这里的 func 为 skynet.newservice
	local ok, handle = pcall(func, ...)
	local s = service[name]
	assert(type(s) == "table")
	if ok then
		service[name] = handle
	else
		service[name] = tostring(handle)
	end

	for _,v in ipairs(s) do		-- 唤醒阻塞的协程
		skynet.wakeup(v)
	end

	if ok then
		return handle
	else
		error(tostring(handle))
	end
end

-- 惰性初始化的体现
-- 如果是首次调用会直接返回
local function waitfor(name , func, ...)
	local s = service[name]
	if type(s) == "number" then	-- 如果已经有了，就直接返回
		return s
	end
	local co = coroutine.running()

	if s == nil then
		s = {}
		service[name] = s
	elseif type(s) == "string" then
		error(s)
	end

	assert(type(s) == "table")

	if not s.launch and func then	-- 如果是首次调用,可以直接返回
		s.launch = true
		return request(name, func, ...)
	end

	table.insert(s, co)				-- 后续的调用都需要阻塞在这里
	skynet.wait()
	s = service[name]
	if type(s) == "string" then
		error(s)
	end
	assert(type(s) == "number")
	return s
end

local function read_name(service_name)
	if string.byte(service_name) == 64 then -- 64的ASCII码为 '@'
		return string.sub(service_name , 2)
	else
		return service_name
	end
end

function cmd.LAUNCH(service_name, subname, ...)
	local realname = read_name(service_name)

	if realname == "snaxd" then
		return waitfor(service_name.."."..subname, snax.rawnewservice, subname, ...)
	else
		return waitfor(service_name, skynet.newservice, realname, subname, ...)
	end
end

function cmd.QUERY(service_name, subname)
	local realname = read_name(service_name)

	if realname == "snaxd" then
		return waitfor(service_name.."."..subname)
	else
		return waitfor(service_name)
	end
end

local function list_service()
	local result = {}
	for k,v in pairs(service) do
		if type(v) == "string" then
			v = "Error: " .. v
		elseif type(v) == "table" then
			v = "Querying"
		else
			v = skynet.address(v)
		end

		result[k] = v
	end

	return result
end

-- 单节点模式、多节点中的主节点
local function register_global()
	function cmd.GLAUNCH(name, ...)
		local global_name = "@" .. name
		return cmd.LAUNCH(global_name, ...)
	end

	function cmd.GQUERY(name, ...)
		local global_name = "@" .. name
		return cmd.QUERY(global_name, ...)
	end

	local mgr = {}

	function cmd.REPORT(m)
		mgr[m] = true
	end

	local function add_list(all, m)
		local harbor = "@" .. skynet.harbor(m)
		local result = skynet.call(m, "lua", "LIST")
		for k,v in pairs(result) do
			all[k .. harbor] = v
		end
	end

	function cmd.LIST()
		local result = {}
		for k in pairs(mgr) do
			pcall(add_list, result, k)
		end
		local l = list_service()
		for k, v in pairs(l) do
			result[k] = v
		end
		return result
	end
end

-- 多节点中的非主节点
local function register_local()
	local function waitfor_remote(cmd, name, ...)
		local global_name = "@" .. name
		local local_name
		if name == "snaxd" then
			local_name = global_name .. "." .. (...)
		else
			local_name = global_name
		end
		return waitfor(local_name, skynet.call, "SERVICE", "lua", cmd, global_name, ...)
		-- 注意此时的 func变为 skynet.call 了， cmd为 "LAUNCH"
	end

	function cmd.GLAUNCH(...)
		return waitfor_remote("LAUNCH", ...)
	end

	function cmd.GQUERY(...)
		return waitfor_remote("QUERY", ...)
	end

	function cmd.LIST()
		return list_service()
	end

	skynet.call("SERVICE", "lua", "REPORT", skynet.self())
end

skynet.start(function()
	skynet.dispatch("lua", function(session, address, command, ...)
		local f = cmd[command]
		if f == nil then
			skynet.ret(skynet.pack(nil, "Invalid command " .. command))
			return
		end

		local ok, r = pcall(f, ...)

		if ok then
			skynet.ret(skynet.pack(r))
		else
			skynet.ret(skynet.pack(nil, r))
		end
	end)
	local handle = skynet.localname ".service" --返回"service"的数字地址
	if  handle then
		skynet.error(".service is already register by ", skynet.address(handle))
		skynet.exit()
	else
		skynet.register(".service")
	end
	if skynet.getenv "standalone" then 	-- 主节点,单节点模式
		skynet.register("SERVICE")
		register_global()
	else								-- 多节点模式中的非主节点
		register_local()
	end
end)
