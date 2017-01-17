local skynet = require "skynet"

local datacenter = {}

-- 此库的缺点在于如果树的已有的某个值被更新了，别人是不知道的，必须要再次查询

-- 获取树的某个值
function datacenter.get(...)
	return skynet.call("DATACENTER", "lua", "QUERY", ...)
end

-- 更新或者设置树的某个值
function datacenter.set(...)
	return skynet.call("DATACENTER", "lua", "UPDATE", ...)
end

-- 取得树的某个值，如果得到是nil，那么阻塞等待值得更新
function datacenter.wait(...)
	return skynet.call("DATACENTER", "lua", "WAIT", ...)
end

return datacenter

