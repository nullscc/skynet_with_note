local skynet = require "skynet"
local socket = require "socket"
require "skynet.manager"	-- import skynet.launch, ...
local table = table

local slaves = {}
local connect_queue = {}
local globalname = {}
local queryname = {}
local harbor = {}
local harbor_service
local monitor = {}
local monitor_master_set = {}

local function read_package(fd)
	local sz = socket.read(fd, 1)
	assert(sz, "closed")
	sz = string.byte(sz)
	local content = assert(socket.read(fd, sz), "closed")
	return skynet.unpack(content)
end

local function pack_package(...)
	local message = skynet.packstring(...)
	local size = #message
	assert(size <= 255 , "too long")
	return string.char(size) .. message
end

-- 通知监控的相应的节点(新上线的/下线的)
local function monitor_clear(id)
	local v = monitor[id]
	if v then
		monitor[id] = nil
		for _, v in ipairs(v) do
			v(true)
		end
	end
end

local function connect_slave(slave_id, address)
	local ok, err = pcall(function()
		if slaves[slave_id] == nil then
			local fd = assert(socket.open(address), "Can't connect to "..address)
			skynet.error(string.format("Connect to harbor %d (fd=%d), %s", slave_id, fd, address))
			slaves[slave_id] = fd
			monitor_clear(slave_id)
			socket.abandon(fd)
			skynet.send(harbor_service, "harbor", string.format("S %d %d",fd,slave_id))
		end
	end)
	if not ok then
		skynet.error(err)
	end
end

-- 与mater握手完毕 把握手完毕前等待的工作都做了
local function ready()
	local queue = connect_queue
	connect_queue = nil
	for k,v in pairs(queue) do
		connect_slave(k,v)
	end
	for name,address in pairs(globalname) do
		skynet.redirect(harbor_service, address, "harbor", 0, "N " .. name)
	end
end

-- 用于此名字查询的被阻塞请求结果的返回
local function response_name(name)
	local address = globalname[name]
	if queryname[name] then
		local tmp = queryname[name]
		queryname[name] = nil
		for _,resp in ipairs(tmp) do
			resp(true, address)
		end
	end
end

local function monitor_master(master_fd)
	while true do
		local ok, t, id_name, address = pcall(read_package,master_fd)
		if ok then
			if t == 'C' then	-- 当有新的 slave 连接上来时，主动连接(从这里可以看出 每个slave都是一一连接的)
				if connect_queue then	
				-- connect_queue 只是为了以防与mster连接还没完成的时候收到另外一个 slave，所以这里只是简单的记录下来，当准备好后再去连接这个slave
					connect_queue[id_name] = address
				else
					connect_slave(id_name, address)	-- 如果已经准备好了，立即连接即可
				end
			elseif t == 'N' then	-- 收到master的从另外 slave 过来的注册新名字的通知
				globalname[id_name] = address	-- 缓存住全局名字
				response_name(id_name)			-- 用于此名字查询的被阻塞请求结果的返回
				if connect_queue == nil then	-- 如果已经准备好了，就给harbor服务发消息，让harbor服务记录下这个地址
					skynet.redirect(harbor_service, address, "harbor", 0, "N " .. id_name)
				end
			elseif t == 'D' then			-- slave挂掉了/下线了
				local fd = slaves[id_name]
				slaves[id_name] = false
				if fd then
					monitor_clear(id_name)
					socket.close(fd)
				end
			end
		else
			skynet.error("Master disconnect")
			for _, v in ipairs(monitor_master_set) do
				v(true)
			end
			socket.close(master_fd)
			break
		end
	end
end

local function accept_slave(fd)
	socket.start(fd)
	local id = socket.read(fd, 1)
	if not id then
		skynet.error(string.format("Connection (fd =%d) closed", fd))
		socket.close(fd)
		return
	end
	id = string.byte(id)
	if slaves[id] ~= nil then
		skynet.error(string.format("Slave %d exist (fd =%d)", id, fd))
		socket.close(fd)
		return
	end
	slaves[id] = fd
	monitor_clear(id)
	socket.abandon(fd)
	skynet.error(string.format("Harbor %d connected (fd = %d)", id, fd))
	skynet.send(harbor_service, "harbor", string.format("A %d %d", fd, id))
end

skynet.register_protocol {
	name = "harbor",
	id = skynet.PTYPE_HARBOR,
	pack = function(...) return ... end,
	unpack = skynet.tostring,
}

skynet.register_protocol {
	name = "text",
	id = skynet.PTYPE_TEXT,
	pack = function(...) return ... end,
	unpack = skynet.tostring,
}

local function monitor_harbor(master_fd)
	return function(session, source, command)
		local t = string.sub(command, 1, 1)
		local arg = string.sub(command, 3)
		if t == 'Q' then
			-- query name
			if globalname[arg] then
				skynet.redirect(harbor_service, globalname[arg], "harbor", 0, "N " .. arg)
			else
				socket.write(master_fd, pack_package("Q", arg))
			end
		elseif t == 'D' then
			-- harbor down
			local id = tonumber(arg)
			if slaves[id] then
				monitor_clear(id)
			end
			slaves[id] = false
		else
			skynet.error("Unknown command ", command)
		end
	end
end

-- fd 为 master 服务对应的描述符 id
function harbor.REGISTER(fd, name, handle)
	assert(globalname[name] == nil)
	globalname[name] = handle	-- 在 slave 服务中缓存住这个全节点有效的名字
	response_name(name)			-- 检查是否有此名字查询的请求阻塞在这里，如果有:返回
	socket.write(fd, pack_package("R", name, handle))	-- 发消息给 master 说:自己要注册这个名字， 然后由 master 将此请求转发给所有 slave
	skynet.redirect(harbor_service, handle, "harbor", 0, "N " .. name)	-- 发消息给 harbor 服务说:我注册这个名字，以便被查找
end

-- 阻塞监控 某个 slave 是否断开，当slave断开，则返回
function harbor.LINK(fd, id)
	if slaves[id] then
		if monitor[id] == nil then
			monitor[id] = {}
		end
		table.insert(monitor[id], skynet.response())
	else
		skynet.ret()
	end
end

-- 阻塞的监控 master，当 master 断开时才返回
function harbor.LINKMASTER()
	table.insert(monitor_master_set, skynet.response())
end

-- 阻塞的等待一个 slave 连接上来 ，如果slave已连接，则直接返回，如果未连接，则等连接连上来后再返回
function harbor.CONNECT(fd, id)
	if not slaves[id] then
		if monitor[id] == nil then
			monitor[id] = {}
		end
		table.insert(monitor[id], skynet.response())
	else
		skynet.ret()
	end
end

-- 阻塞的查询全局名字或本地名字对应的服务地址，如果查不到则一直等到这个名字注册上来
function harbor.QUERYNAME(fd, name)
	if name:byte() == 46 then	-- "." , local name 如果是本节点的服务名字，就直接返回地址
		skynet.ret(skynet.pack(skynet.localname(name)))
		return
	end
	local result = globalname[name]	-- 如果已经缓存过(是此节点的服务)，也直接返回
	if result then
		skynet.ret(skynet.pack(result))
		return
	end
	local queue = queryname[name]
	if queue == nil then	-- 如果为空 说明此名字还没查询过
		socket.write(fd, pack_package("Q", name))
		queue = { skynet.response() }
		queryname[name] = queue
	else					-- 如果不为空 说明此名字已经查询过 但是由于某种原因还没返回(还没注册、slave还没连接上来) 将其加入队列 等注册上来后再返回
		table.insert(queue, skynet.response())
	end
end

skynet.start(function()
	local master_addr = skynet.getenv "master"
	local harbor_id = tonumber(skynet.getenv "harbor")
	local slave_address = assert(skynet.getenv "address")
	local slave_fd = socket.listen(slave_address)
	skynet.error("slave connect to master " .. tostring(master_addr))
	local master_fd = assert(socket.open(master_addr), "Can't connect to master")

	skynet.dispatch("lua", function (_,_,command,...)
		local f = assert(harbor[command])
		f(master_fd, ...)
	end)
	skynet.dispatch("text", monitor_harbor(master_fd))

	harbor_service = assert(skynet.launch("harbor", harbor_id, skynet.self()))

	local hs_message = pack_package("H", harbor_id, slave_address)
	socket.write(master_fd, hs_message)
	local t, n = read_package(master_fd)
	assert(t == "W" and type(n) == "number", "slave shakehand failed")
	skynet.error(string.format("Waiting for %d harbors", n))
	skynet.fork(monitor_master, master_fd)
	if n > 0 then
		local co = coroutine.running()
		socket.start(slave_fd, function(fd, addr)
			skynet.error(string.format("New connection (fd = %d, %s)",fd, addr))
			if pcall(accept_slave,fd) then
				local s = 0
				for k,v in pairs(slaves) do
					s = s + 1
				end
				if s >= n then
					skynet.wakeup(co)
				end
			end
		end)
		skynet.wait()
	end
	socket.close(slave_fd)
	skynet.error("Shakehand ready")
	skynet.fork(ready)
end)
