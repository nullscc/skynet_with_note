local c = require "skynet.core"
local tostring = tostring
local tonumber = tonumber
local coroutine = coroutine
local assert = assert
local pairs = pairs
local pcall = pcall

local profile = require "profile"

-- profile是一个等同于lua的croutine，只不过它能记录协程所花的时间
local coroutine_resume = profile.resume
local coroutine_yield = profile.yield

local proto = {}

-- 消息类型
local skynet = {
	-- read skynet.h
	PTYPE_TEXT = 0,
	PTYPE_RESPONSE = 1,
	PTYPE_MULTICAST = 2,
	PTYPE_CLIENT = 3,
	PTYPE_SYSTEM = 4,
	PTYPE_HARBOR = 5,
	PTYPE_SOCKET = 6,
	PTYPE_ERROR = 7,
	PTYPE_QUEUE = 8,	-- used in deprecated mqueue, use skynet.queue instead
	PTYPE_DEBUG = 9,
	PTYPE_LUA = 10,
	PTYPE_SNAX = 11,
}

-- code cache
skynet.cache = require "skynet.codecache"

-- 注册某种类型消息的接口
function skynet.register_protocol(class)
	local name = class.name
	local id = class.id
	assert(proto[name] == nil)
	assert(type(name) == "string" and type(id) == "number" and id >=0 and id <=255)
	proto[name] = class
	proto[id] = class
end

local session_id_coroutine = {}			
-- 以session为key，协程为value，主要是为了记录session对应的协程，当服务收到返回值后，可以根据session唤醒相应的协程，返回从另外服务返回的值

local session_coroutine_id = {}
-- 以协程为key，session为value，有消息来时记录下协程对应的session

local session_coroutine_address = {}
-- 以协程为key，发送方服务address为value，有消息来时记录下协程对应的源服务的地址

local session_response = {}		-- 以消息的协程co为key，true为value，记录这个协程是不是已经返回它需要的返回值了
local unresponse = {}			-- 当A服务调用skynet.response给B想要给B返回值时，以 suspend中的 elseif command == "RESPONSE" then 中的 response 函数为key，true为value记录在此表中，这样如果A还没来得及返回时A就要退出了。可以从此表中找到response函数，以告诉B(或者其他多个服务)说:"我退出了，你想要的值得不到了"
local wakeup_session = {}		-- 当调用skynet.wakeup时，以协程co为key，true为值，压入此队列，等待 dispatch_wakeup 的调用
local sleep_session = {}		-- 当调用skynet.sleep时，以协程co为key，session为value的依次压入此队列，这里的session是定时器创建时返回的

local watching_service = {}		-- 如果A服务向B服务请求(需要返回值)，那么B服务会以A服务的地址为key，A服务向B服务请求的还在挂起的任务数量的个数为value储存在这个table里面。
local watching_session = {}		-- (调用skynet.call)等待返回值的session(key)对应的服务地址addr(value)
local dead_service = {}			-- 当A服务向B服务发送一个类型为7的消息时，在B服务中会以A服务的地址为key，true为value加入这个table，这样当B返回消息到A时发现A为 dead_service 即丢弃这个消息
local error_queue = {}			-- 如果A服务调用skynet.call向B发起请求，B由于某种错误(通常是调用skynet.exit退出了)不能返回了，B会向A发送一个类型为7的消息，A收到此消息后将错误的session加入此队列的末尾，等待 dispatch_error_queue 的调用
local fork_queue = {}			-- 调用skynet.fork后会创建一个协程co，并将co加入此队列的末尾，等待 skynet.dispatch_message 的调用

-- suspend is function
-- 应该是类似于前置声明的作用
local suspend

-- 参数为带冒号的16进制数字表示的字符串，返回地址
local function string_to_handle(str)
	return tonumber("0x" .. string.sub(str , 2))
end

----- monitor exit

-- 每执行完一个suspend函数执行一次，从error_queue中取出一个协程并唤醒
local function dispatch_error_queue()
	local session = table.remove(error_queue,1)
	if session then
		local co = session_id_coroutine[session]
		session_id_coroutine[session] = nil
		return suspend(co, coroutine_resume(co, false))
		-- 一般会唤醒skynet.call 中的 yield_call 中的 coroutine_yield("CALL", session) 的执行
	end
end

-- 当服务收到消息类型为7的消息时的"真正的"消息处理函数
local function _error_dispatch(error_session, error_source)
	if error_session == 0 then
		-- service is down
		--  Don't remove from watching_service , because user may call dead service
		if watching_service[error_source] then
			dead_service[error_source] = true
		end
		for session, srv in pairs(watching_session) do
			if srv == error_source then
				table.insert(error_queue, session)
			end
		end
	else
		-- capture an error for error_session
		if watching_session[error_session] then
			table.insert(error_queue, error_session)
		end
	end
end

-- coroutine reuse

local coroutine_pool = setmetatable({}, { __mode = "kv" })

-- 用来唤醒 skynet.wakeup 函数中的参数(协程)
local function dispatch_wakeup()
	local co = next(wakeup_session)
	if co then
		wakeup_session[co] = nil
		local session = sleep_session[co]
		if session then
			session_id_coroutine[session] = "BREAK"		-- 因为这里有可能会提前wakeup，所以将对应的 session 的协程置为 "BREAK" 这样当定时器超时时间到了框架会知道这个sleep早就被唤醒了，不需要再处理了
			return suspend(co, coroutine_resume(co, false, "BREAK"))
			-- 一般会唤醒 skynet.sleep 中的 local succ, ret = coroutine_yield("SLEEP", session) 的执行
		end
	end
end

-- 将源服务的引用计数减1
local function release_watching(address)
	local ref = watching_service[address]
	if ref then
		ref = ref - 1
		if ref > 0 then
			watching_service[address] = ref
		else
			watching_service[address] = nil
		end
	end
end

-- suspend is local function
function suspend(co, result, command, param, size)
	if not result then	-- 当协程错误发生时，或skynet.sleep被skynet.wakeup提前唤醒时
		local session = session_coroutine_id[co]
		if session then -- coroutine may fork by others (session is nil)
			local addr = session_coroutine_address[co]
			if session ~= 0 then
				-- only call response error
				c.send(addr, skynet.PTYPE_ERROR, session, "")
			end
			session_coroutine_id[co] = nil
			session_coroutine_address[co] = nil
		end
		error(debug.traceback(co,tostring(command)))
	end
	if command == "CALL" then					-- 调用skynet.call会触发此处执行
		session_id_coroutine[param] = co 		-- 以session为key记录协程
	elseif command == "SLEEP" then				-- 调用skynet.sleep后会触发此处执行
		session_id_coroutine[param] = co		-- 这里的param是session
		sleep_session[co] = param				
	elseif command == "RETURN" then				-- 调用skynet.ret后会触发此处执行
		local co_session = session_coroutine_id[co]
		local co_address = session_coroutine_address[co]
		if param == nil or session_response[co] then
			error(debug.traceback(co))
		end
		session_response[co] = true
		local ret
		if not dead_service[co_address] then
			ret = c.send(co_address, skynet.PTYPE_RESPONSE, co_session, param, size) ~= nil
			if not ret then
				-- If the package is too large, returns nil. so we should report error back
				c.send(co_address, skynet.PTYPE_ERROR, co_session, "")
			end
		elseif size ~= nil then
			c.trash(param, size)
			ret = false
		end
		return suspend(co, coroutine_resume(co, ret))
		--coroutine_resume会恢复处理函数中的协程执行(会从skynet.ret中的coroutine_yield("RETURN", msg, sz)处返回)，到这里处理函数执行完毕了，即co_create中的f函数执行完毕了
	elseif command == "RESPONSE" then		-- 可看例子:testresponse.lua
		local co_session = session_coroutine_id[co]
		local co_address = session_coroutine_address[co]
		if session_response[co] then
			error(debug.traceback(co))
		end
		local f = param						-- 默认为skynet.pack
		local function response(ok, ...)
			if ok == "TEST" then
				if dead_service[co_address] then
					release_watching(co_address)
					unresponse[response] = nil
					f = false
					return false
				else
					return true
				end
			end
			if not f then
				if f == false then
					f = nil
					return false
				end
				error "Can't response more than once"
			end

			local ret
			if not dead_service[co_address] then
				if ok then
					ret = c.send(co_address, skynet.PTYPE_RESPONSE, co_session, f(...)) ~= nil
					if not ret then
						-- If the package is too large, returns false. so we should report error back
						c.send(co_address, skynet.PTYPE_ERROR, co_session, "")
					end
				else
					ret = c.send(co_address, skynet.PTYPE_ERROR, co_session, "") ~= nil
				end
			else
				ret = false
			end
			release_watching(co_address)
			unresponse[response] = nil
			f = nil
			return ret
		end
		watching_service[co_address] = watching_service[co_address] + 1
		session_response[co] = true
		unresponse[response] = true
		return suspend(co, coroutine_resume(co, response))		-- 恢复 skynet.response 中的 coroutine_yield("RESPONSE", pack) 执行，即让 skynet.response 返回
	elseif command == "EXIT" then	-- 执行到 co_create 中的f = coroutine_yield "EXIT"会触发此处的执行，到这里对于收到消息的一方来说这次消息完全处理完毕
		-- coroutine exit
		local address = session_coroutine_address[co]
		release_watching(address)
		session_coroutine_id[co] = nil
		session_coroutine_address[co] = nil
		session_response[co] = nil
	elseif command == "QUIT" then	-- 调用 skynet.exit 会触发此处执行
		-- service exit
		return
	elseif command == "USER" then
		-- See skynet.coutine for detail
		error("Call skynet.coroutine.yield out of skynet.coroutine.resume\n" .. debug.traceback(co))
	elseif command == nil then
		-- debug trace
		return
	else
		error("Unknown command : " .. command .. "\n" .. debug.traceback(co))
	end
	dispatch_wakeup()
	dispatch_error_queue()
end

-- 协程创建与复用函数，调用此函数总会得到一个协程
local function co_create(f)
	local co = table.remove(coroutine_pool)				-- 从协程池取出一个协程
	if co == nil then 									-- 如果没有可用的协程
		co = coroutine.create(function(...)				-- 创建新的协程
			f(...)										-- 当调用coroutine.resume时，执行函数f
			while true do		
				f = nil									-- 将函数置空
				coroutine_pool[#coroutine_pool+1] = co 	-- 协程执行完后，回收协程
				f = coroutine_yield "EXIT"				-- 协程执行完后，让出执行
				f(coroutine_yield())
			end
		end)
	else
		coroutine_resume(co, f)							-- 这里coroutine_resume对应的是上面的coroutine_yield "EXIT"
	end
	return co
end

-- 所有lua服务的消息处理函数(从定时器发过来的消息源地址(source)是 0) 这里的msg就是特定的数据结构体
local function raw_dispatch_message(prototype, msg, sz, session, source)
	-- skynet.PTYPE_RESPONSE = 1, read skynet.h
	if prototype == 1 then 		-- 处理远端发送过来的返回值
		local co = session_id_coroutine[session]
		if co == "BREAK" then
			session_id_coroutine[session] = nil
		elseif co == nil then
			unknown_response(session, source, msg, sz)
		else
			session_id_coroutine[session] = nil
			suspend(co, coroutine_resume(co, true, msg, sz))
			-- 唤醒yield_call中的coroutine_yield("CALL", session)
		end
	else
		local p = proto[prototype]
		if p == nil then
			if session ~= 0 then	-- 如果是需要返回值的，那么告诉源服务，说"我对你来说是dead_service不要再发过来了"
				c.send(source, skynet.PTYPE_ERROR, session, "")
			else
				unknown_request(session, source, msg, sz, prototype)
			end
			return
		end
		local f = p.dispatch
		if f then
			local ref = watching_service[source]
			if ref then
				watching_service[source] = ref + 1
			else
				watching_service[source] = 1
			end
			local co = co_create(f)
			session_coroutine_id[co] = session
			session_coroutine_address[co] = source
			suspend(co, coroutine_resume(co, session,source, p.unpack(msg,sz)))
		else
			unknown_request(session, source, msg, sz, proto[prototype].name)
		end
	end
end

-- 向框架注册一个定时器，并得到一个session，从定时器发过来的消息源地址是 0
function skynet.timeout(ti, func)
	local session = c.intcommand("TIMEOUT",ti)
	assert(session)
	local co = co_create(func)
	assert(session_id_coroutine[session] == nil)
	session_id_coroutine[session] = co
end

-- 将当前协程挂起ti时间，实际上也是向框架注册一个定时器，区别是挂起的时间可以被skynet.wakeup"打断"
function skynet.sleep(ti)
	local session = c.intcommand("TIMEOUT",ti)
	assert(session)
	local succ, ret = coroutine_yield("SLEEP", session)
	sleep_session[coroutine.running()] = nil
	if succ then
		return
	end
	if ret == "BREAK" then
		return "BREAK"
	else
		error(ret)
	end
end

-- 挂起一小段时间(通常是一个或多个协程处理时间)
function skynet.yield()
	return skynet.sleep(0)
end

-- 挂起当前协程，必须由 skynet.wakeup 唤醒
function skynet.wait(co)
	local session = c.genid() -- 由于不需要向框架注册一个定时器，但是挂起的协程需要一个session，所以通过 c.genid生成， c.genid不会把任何消息压入消息队列中
	local ret, msg = coroutine_yield("SLEEP", session)
	co = co or coroutine.running()
	sleep_session[co] = nil
	session_id_coroutine[session] = nil
end

-- 得到自身的服务地址
local self_handle
function skynet.self()
	if self_handle then
		return self_handle
	end
	self_handle = string_to_handle(c.command("REG"))
	return self_handle
end

-- 返回一个带冒号的16进制地址
function skynet.localname(name)
	local addr = c.command("QUERY", name)	--返回一个带冒号的16进制的数字的字符串
	if addr then
		return string_to_handle(addr)
	end
end

skynet.now = c.now

local starttime

function skynet.starttime()
	if not starttime then
		starttime = c.intcommand("STARTTIME")
	end
	return starttime
end

function skynet.time()
	return skynet.now()/100 + (starttime or skynet.starttime())
end

-- 退出一个服务
function skynet.exit()
	fork_queue = {}	-- no fork coroutine can be execute after skynet.exit
	skynet.send(".launcher","lua","REMOVE",skynet.self(), false)
	-- report the sources that call me
	for co, session in pairs(session_coroutine_id) do
		local address = session_coroutine_address[co]
		if session~=0 and address then
			c.redirect(address, 0, skynet.PTYPE_ERROR, session, "")
		end
	end
	for resp in pairs(unresponse) do
		resp(false)
	end
	-- report the sources I call but haven't return
	local tmp = {}
	for session, address in pairs(watching_session) do
		tmp[address] = true
	end
	for address in pairs(tmp) do
		c.redirect(address, 0, skynet.PTYPE_ERROR, 0, "")
	end
	c.command("EXIT")
	-- quit service
	coroutine_yield "QUIT"
end

function skynet.getenv(key)
	return (c.command("GETENV",key))
end

function skynet.setenv(key, value)
	c.command("SETENV",key .. " " ..value)
end

-- 调用此接口发送消息(不需要返回值)
function skynet.send(addr, typename, ...)
	local p = proto[typename]
	return c.send(addr, p.id, 0 , p.pack(...))	--由于skynet.send是不需要返回值的，所以就不需要记录session，所以为0即可
end

skynet.genid = assert(c.genid)

skynet.redirect = function(dest,source,typename,...)
	return c.redirect(dest, source, proto[typename].id, ...)
end

skynet.pack = assert(c.pack)
skynet.packstring = assert(c.packstring)
skynet.unpack = assert(c.unpack)
skynet.tostring = assert(c.tostring)
skynet.trash = assert(c.trash)

local function yield_call(service, session)
	watching_session[session] = service
	local succ, msg, sz = coroutine_yield("CALL", session)	--会让出到raw_dispatch_message中的第二个suspend函数中，即执行:suspend(true, "CALL", session)
	watching_session[session] = nil
	if not succ then
		error "call failed"
	end
	return msg,sz
end

-- 调用此接口发送消息(需要返回值)
function skynet.call(addr, typename, ...)
	local p = proto[typename]
	local session = c.send(addr, p.id , nil , p.pack(...))		-- 发送消息
	-- 由于skynet.call是需要返回值的，所以c.send的第三个参数表示由框架自动分配一个session，以便返回时根据相应的session找到对应的协程进行处理
	if session == nil then
		error("call to invalid address " .. skynet.address(addr))
	end
	return p.unpack(yield_call(addr, session))					-- 阻塞等待返回值
end

-- skynet.call 功能类似、。但发送时不经过 pack 打包流程，收到回应后，也不走 unpack 流程。
function skynet.rawcall(addr, typename, msg, sz)
	local p = proto[typename]
	local session = assert(c.send(addr, p.id , nil , msg, sz), "call to invalid address")
	return yield_call(addr, session)
end

-- 一般用来返回消息给主动调用skynet.call的服务
function skynet.ret(msg, sz)
	msg = msg or ""
	return coroutine_yield("RETURN", msg, sz)
	-- 会让出到raw_dispatch_message函数的else分支中，参数给suspend,就成为:suspend(co, true, "RETURN", msg, sz)
end

-- 与 skynet.ret 有异曲同工之用
-- 区别在于: 1. 可以提供打包函数(默认为skynet.pack) 2.调用者需要调用它返回的调用值(一个函数)并提供参数
-- 共同之处在于一般都是在消息处理函数中进行调用
function skynet.response(pack)
	pack = pack or skynet.pack
	return coroutine_yield("RESPONSE", pack)	-- 一般会让出到 raw_dispatch_message 的else分支的 suspend(co, coroutine_resume(co, session,source, p.unpack(msg,sz))) 处
end

function skynet.retpack(...)
	return skynet.ret(skynet.pack(...))
end

-- 将wakeup_session中的某个协程置为true，由 dispatch_wakeup 从中取出进行处理
function skynet.wakeup(co)
	if sleep_session[co] and wakeup_session[co] == nil then
		wakeup_session[co] = true
		return true
	end
end

-- 将func赋值给p.dispatch， 这里的func就是真正的消息处理函数
function skynet.dispatch(typename, func)
	local p = proto[typename]
	if func then  --lua类型的消息一般走这里
		local ret = p.dispatch
		p.dispatch = func
		return ret
	else
		return p and p.dispatch
	end
end

-- 仅仅做下日志处理，并抛出异常，但是永不返回
local function unknown_request(session, address, msg, sz, prototype)
	skynet.error(string.format("Unknown request (%s): %s", prototype, c.tostring(msg,sz)))
	error(string.format("Unknown session : %d from %x", session, address))
end

function skynet.dispatch_unknown_request(unknown)
	local prev = unknown_request
	unknown_request = unknown
	return prev
end

local function unknown_response(session, address, msg, sz)
	skynet.error(string.format("Response message : %s" , c.tostring(msg,sz)))
	error(string.format("Unknown session : %d from %x", session, address))
end

function skynet.dispatch_unknown_response(unknown)
	local prev = unknown_response
	unknown_response = unknown
	return prev
end

-- 创建一个协程，协程执行func(...)函数，将协程加入fork_queue，等待 skynet.dispatch_message 的调用
function skynet.fork(func,...)
	local args = table.pack(...)
	local co = co_create(function()
		func(table.unpack(args,1,args.n))
	end)
	table.insert(fork_queue, co)
	return co
end

-- lua服务的消息处理函数的最外层
function skynet.dispatch_message(...)
	local succ, err = pcall(raw_dispatch_message,...)
	while true do
		local key,co = next(fork_queue)
		if co == nil then
			break
		end
		fork_queue[key] = nil
		local fork_succ, fork_err = pcall(suspend,co,coroutine_resume(co))
		if not fork_succ then
			if succ then
				succ = false
				err = tostring(fork_err)
			else
				err = tostring(err) .. "\n" .. tostring(fork_err)
			end
		end
	end
	assert(succ, tostring(err))
end

function skynet.newservice(name, ...)
	return skynet.call(".launcher", "lua" , "LAUNCH", "snlua", name, ...)
end

function skynet.uniqueservice(global, ...)
	if global == true then
		return assert(skynet.call(".service", "lua", "GLAUNCH", ...))
	else
		return assert(skynet.call(".service", "lua", "LAUNCH", global, ...))
	end
end

function skynet.queryservice(global, ...)
	if global == true then
		return assert(skynet.call(".service", "lua", "GQUERY", ...))
	else
		return assert(skynet.call(".service", "lua", "QUERY", global, ...))
	end
end

-- 返回地址的字符串形式,以冒号开头
function skynet.address(addr)
	if type(addr) == "number" then
		return string.format(":%08x",addr)
	else
		return tostring(addr)
	end
end

function skynet.harbor(addr)
	return c.harbor(addr)
end

skynet.error = c.error

----- register protocol 默认的三种类型
do
	local REG = skynet.register_protocol

	REG {
		name = "lua",
		id = skynet.PTYPE_LUA,
		pack = skynet.pack,
		unpack = skynet.unpack,
	}

	REG {
		name = "response",
		id = skynet.PTYPE_RESPONSE,
	}

	REG {
		name = "error",
		id = skynet.PTYPE_ERROR,
		unpack = function(...) return ... end,
		dispatch = _error_dispatch,
	}
end

local init_func = {}

function skynet.init(f, name)
	assert(type(f) == "function")
	if init_func == nil then
		f()
	else
		table.insert(init_func, f)
		if name then
			assert(type(name) == "string")
			assert(init_func[name] == nil)
			init_func[name] = f
		end
	end
end

local function init_all()
	local funcs = init_func
	init_func = nil
	if funcs then
		for _,f in ipairs(funcs) do
			f()
		end
	end
end

local function ret(f, ...)
	f()
	return ...
end

local function init_template(start, ...)
	init_all()
	init_func = {}
	return ret(init_all, start(...))
end

function skynet.pcall(start, ...)
	return xpcall(init_template, debug.traceback, start, ...)
end

function skynet.init_service(start)
	local ok, err = skynet.pcall(start)
	if not ok then
		skynet.error("init service failed: " .. tostring(err))
		skynet.send(".launcher","lua", "ERROR")
		skynet.exit()
	else
		skynet.send(".launcher","lua", "LAUNCHOK")
	end
end

function skynet.start(start_func)
	c.callback(skynet.dispatch_message)
	skynet.timeout(0, function()
		skynet.init_service(start_func)
	end)
end

function skynet.endless()
	return c.command("ENDLESS")~=nil
end

function skynet.mqlen()
	return c.intcommand "MQLEN"
end

-- 返回当前服务挂起的任务数
function skynet.task(ret)
	local t = 0
	for session,co in pairs(session_id_coroutine) do
		if ret then
			ret[session] = debug.traceback(co)
		end
		t = t + 1
	end
	return t
end

function skynet.term(service)
	return _error_dispatch(0, service)
end

function skynet.memlimit(bytes)
	debug.getregistry().memlimit = bytes
	skynet.memlimit = nil	-- set only once
end

-- Inject internal debug framework
local debug = require "skynet.debug"
debug.init(skynet, {
	dispatch = skynet.dispatch_message,
	suspend = suspend,
})

return skynet
