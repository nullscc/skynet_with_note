local skynet = require "skynet"

local command = {}
local database = {}
local wait_queue = {}
local mode = {}

-- 查询叶子节点
local function query(db, key, ...)
	if key == nil then
		return db
	else
		return query(db[key], ...)
	end
end

-- 查询树
function command.QUERY(key, ...)
	local d = database[key]
	if d then
		return query(d, ...)
	end
end

-- 更新树的值
local function update(db, key, value, ...)
	if select("#",...) == 0 then
		local ret = db[key]
		db[key] = value
		return ret, value
	else
		if db[key] == nil then
			db[key] = {}
		end
		return update(db[key], value, ...)
	end
end

-- 唤醒等待的叶节点(但是不能唤醒分支)
local function wakeup(db, key1, ...)
	if key1 == nil then
		return
	end
	local q = db[key1]
	if q == nil then
		return
	end
	if q[mode] == "queue" then
		db[key1] = nil
		if select("#", ...) ~= 1 then
			-- throw error because can't wake up a branch
			for _,response in ipairs(q) do
				response(false)
			end
		else
			return q
		end
	else
		-- it's branch
		return wakeup(q , ...)
	end
end

-- 更新树的值
function command.UPDATE(...)
	local ret, value = update(database, ...)
	if ret or value == nil then
		return ret
	end
	local q = wakeup(wait_queue, ...)	-- 看此次更新的值是不是有等待队列在，如果有，取出闭包，返回值
	if q then
		for _, response in ipairs(q) do	-- 注意这里是用ipairs，它保证会将 key 为 "mode" 的值迭代出来
			response(true,value)
		end
	end
end

-- 等待有值更新
-- 此函数用来当 command.QUERY 得到一个 nil 值时，等待有人更新它(调用skynet.response()生成一个闭包)
local function waitfor(db, key1, key2, ...)
	if key2 == nil then
		-- push queue
		local q = db[key1]
		if q == nil then
			q = { [mode] = "queue" }
			db[key1] = q
		else
			assert(q[mode] == "queue")
		end
		table.insert(q, skynet.response())
	else
		local q = db[key1]
		if q == nil then
			q = { [mode] = "branch" }
			db[key1] = q
		else
			assert(q[mode] == "branch")
		end
		return waitfor(q, key2, ...)
	end
end

skynet.start(function()
	skynet.dispatch("lua", function (_, _, cmd, ...)
		if cmd == "WAIT" then
			local ret = command.QUERY(...)
			if ret then
				skynet.ret(skynet.pack(ret))
			else
				waitfor(wait_queue, ...)
			end
		else
			local f = assert(command[cmd])
			skynet.ret(skynet.pack(f(...)))
		end
	end)
end)
