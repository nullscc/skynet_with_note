#include "skynet.h"
#include "skynet_harbor.h"
#include "skynet_server.h"
#include "skynet_mq.h"
#include "skynet_handle.h"

#include <string.h>
#include <stdio.h>
#include <assert.h>

static struct skynet_context * REMOTE = 0;
static unsigned int HARBOR = ~0;

void 
skynet_harbor_send(struct remote_message *rmsg, uint32_t source, int session) {
	int type = rmsg->sz >> MESSAGE_TYPE_SHIFT;	// 取出此消息在 skynet 框架内的消息类型
	rmsg->sz &= MESSAGE_TYPE_MASK;				// 得到真正的大小
	assert(type != PTYPE_SYSTEM && type != PTYPE_HARBOR && REMOTE);		// 发送到 harbor 服务的消息类型不能是 PTYPE_SYSTEM 、 PTYPE_HARBOR
	skynet_context_send(REMOTE, rmsg, sizeof(*rmsg) , source, type , session);	// 发送消息到 harbor 服务
}

int 
skynet_harbor_message_isremote(uint32_t handle) {
	assert(HARBOR != ~0);
	int h = (handle & ~HANDLE_MASK);
	return h != HARBOR && h !=0;
}

void
skynet_harbor_init(int harbor) {
	HARBOR = (unsigned int)harbor << HANDLE_REMOTE_SHIFT;
}

void
skynet_harbor_start(void *ctx) {
	// the HARBOR must be reserved to ensure the pointer is valid.
	// It will be released at last by calling skynet_harbor_exit
	skynet_context_reserve(ctx);
	REMOTE = ctx;
}

void
skynet_harbor_exit() {
	struct skynet_context * ctx = REMOTE;
	REMOTE= NULL;
	if (ctx) {
		skynet_context_release(ctx);
	}
}
