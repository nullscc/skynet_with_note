local skynet = require "skynet"
local socket = require "socket"

--[[
	master manage data :
		1. all the slaves address : id -> ipaddr:port
		2. all the global names : name -> address

	master hold connections from slaves .

	protocol slave->master :
		package size 1 byte
		type 1 byte :
			'H' : HANDSHAKE, report slave id, and address.
			'R' : REGISTER name address
			'Q' : QUERY name


	protocol master->slave:
		package size 1 byte
		type 1 byte :
			'W' : WAIT n
			'C' : CONNECT slave_id slave_address
			'N' : NAME globalname address
			'D' : DISCONNECT slave_id
]]

local slave_node = {}
local global_name = {}

-- 接受从网络过来的序列化后的包并将其反序列化后返回
local function read_package(fd)
	local sz = socket.read(fd, 1)
	assert(sz, "closed")
	sz = string.byte(sz)
	local content = assert(socket.read(fd, sz), "closed")
	return skynet.unpack(content)
end

-- 将序列化后的包发出去
local function pack_package(...)
	local message = skynet.packstring(...)
	local size = #message
	assert(size <= 255 , "too long")
	return string.char(size) .. message
end

-- 当有新slave连接上来时，返回一个确认包告诉这个slave说:你已经连接到master了。
-- 并且通知其它slave说有哪个新slave已经连接上来了
local function report_slave(fd, slave_id, slave_addr)
	local message = pack_package("C", slave_id, slave_addr)
	local n = 0
	for k,v in pairs(slave_node) do
		if v.fd ~= 0 then
			-- 依次告诉老slave节点，有新节点已经连接上来了
			socket.write(v.fd, message)
			n = n + 1
		end
	end
	
	-- 告诉老新连接上来的 slave，有多少个 slave 已经连接了
	socket.write(fd, pack_package("W", n))
end

-- 在中心节点记录下这个slave，并通知其他slave说这个slave连接上来了，让别的slave都去连接这个新的slave
local function handshake(fd)
	local t, slave_id, slave_addr = read_package(fd)
	assert(t=='H', "Invalid handshake type " .. t)
	assert(slave_id ~= 0 , "Invalid slave id 0")
	if slave_node[slave_id] then
		error(string.format("Slave %d already register on %s", slave_id, slave_node[slave_id].addr))
	end
	report_slave(fd, slave_id, slave_addr)
	slave_node[slave_id] = {
		fd = fd,
		id = slave_id,
		addr = slave_addr,
	}
	return slave_id , slave_addr
end

-- 主要是收取 slave 发过来的请求，然后进行相应的处理并回应(如果需哟啊回应的话)
local function dispatch_slave(fd)
	local t, name, address = read_package(fd)
	if t == 'R' then		-- 注册全局名字
		-- register name
		assert(type(address)=="number", "Invalid request")
		if not global_name[name] then
			global_name[name] = address
		end
		local message = pack_package("N", name, address)
		for k,v in pairs(slave_node) do
			socket.write(v.fd, message)	-- 向所有的 slave 节点广播 'N' 命令
		end
	elseif t == 'Q' then
		-- query name
		local address = global_name[name]
		if address then
			socket.write(fd, pack_package("N", name, address))
		end
	else
		skynet.error("Invalid slave message type " .. t)
	end
end

-- 监控 slave 的协程，其实就是对处理 slave 发过来的消息
local function monitor_slave(slave_id, slave_address)
	local fd = slave_node[slave_id].fd
	skynet.error(string.format("Harbor %d (fd=%d) report %s", slave_id, fd, slave_address))

	-- 调用 dispatch_slave 收取 slave 发过来的网络包
	while pcall(dispatch_slave, fd) do end
	skynet.error("slave " ..slave_id .. " is down")
	local message = pack_package("D", slave_id)
	slave_node[slave_id].fd = 0
	for k,v in pairs(slave_node) do
		socket.write(v.fd, message)
	end
	socket.close(fd)
end

skynet.start(function()
	-- 得到中心节点的地址
	local master_addr = skynet.getenv "standalone"
	skynet.error("master listen socket " .. tostring(master_addr))

	-- 监听中心节点
	local fd = socket.listen(master_addr)

	-- 调用 socket.start 正式开始监听
	socket.start(fd , function(id, addr)
		-- 如果有远端连接过来，会调用此函数，这里是有 slave 连接过来了会调用此函数
		skynet.error("connect from " .. addr .. " " .. id)

		-- 启动数据传输
		socket.start(id)

		-- 调用 handshake 在中心节点记录下这个 slave，并通知其他slave说这个slave连接上来了，让别的slave都去连接这个新的slave)
		local ok, slave, slave_addr = pcall(handshake, id)
		if ok then
			-- 监控 slave 的协程，其实就是对处理 slave 发过来的消息
			skynet.fork(monitor_slave, slave, slave_addr)
		else
			skynet.error(string.format("disconnect fd = %d, error = %s", id, slave))
			socket.close(id)
		end
	end)
end)
