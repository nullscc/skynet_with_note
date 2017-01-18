local skynet = require "skynet"
local harbor = require "skynet.harbor"
require "skynet.manager"	-- import skynet.launch, ...
local memory = require "memory"

skynet.start(function()
	local sharestring = tonumber(skynet.getenv "sharestring" or 4096)
	memory.ssexpand(sharestring)

	local standalone = skynet.getenv "standalone"
	-- 获取 config 中的 standalone 参数，如果standalone存在，它应该是一个"ip地址:端口"

	local launcher = assert(skynet.launch("snlua","launcher"))
	-- 启动 launcher 服务，用来管理所的本地服务
	skynet.name(".launcher", launcher)

	local harbor_id = tonumber(skynet.getenv "harbor" or 0)
	-- 获取 config 中的 harbor 参数

	-- 如果 harbor 为 0 (即工作在单节点模式下)
	if harbor_id == 0 then
		assert(standalone ==  nil)	-- 如果是单节点， standalone 不能配置
		standalone = true
		skynet.setenv("standalone", "true")	-- 设置 standalone 的环境变量为true

		-- 如果是单节点模式，则slave服务为 cdummy.lua
		local ok, slave = pcall(skynet.newservice, "cdummy")
		if not ok then
			skynet.abort()
		end
		skynet.name(".cslave", slave)

	else					-- 如果是多节点模式
		if standalone then	-- 如果是中心节点则启动 cmaster 服务
			if not pcall(skynet.newservice,"cmaster") then
				skynet.abort()
			end
		end

		-- 如果是多节点模式，则 slave 服务为 cslave.lua
		local ok, slave = pcall(skynet.newservice, "cslave")
		if not ok then
			skynet.abort()
		end
		skynet.name(".cslave", slave)
	end

	if standalone then	-- 如果是中心节点则启动 datacenterd 服务
		local datacenter = skynet.newservice "datacenterd"
		skynet.name("DATACENTER", datacenter)
	end
	skynet.newservice "service_mgr"
	pcall(skynet.newservice,skynet.getenv "start" or "main")
	skynet.exit()
end)
