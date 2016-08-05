#include "skynet.h"

#include "skynet_handle.h"
#include "skynet_server.h"
#include "rwlock.h"

#include <stdlib.h>
#include <assert.h>
#include <string.h>

#define DEFAULT_SLOT_SIZE 4
#define MAX_SLOT_SIZE 0x40000000

struct handle_name {
	char * name;
	uint32_t handle;
};

struct handle_storage {
	struct rwlock lock;		//读写锁

	uint32_t harbor;		//harbor id
	uint32_t handle_index;	//总共有多少个服务
	int slot_size;			//slot的个数，slot_size永远不会小于handle_index
	struct skynet_context ** slot; //slot下挂着所有的服务相关的结构体struct skynet_context
	
	int name_cap;		//存储全局名字的空间的总个数，永远大于name_count
	int name_count;		//当前全局名字的个数
	struct handle_name *name;	//用于管理服务的全局名字
};

static struct handle_storage *H = NULL;

//将struct skynet_context指针挂在struct handle_storage的slot下，统一进行管理
uint32_t
skynet_handle_register(struct skynet_context *ctx) {
	struct handle_storage *s = H;

	rwlock_wlock(&s->lock);
	
	for (;;) {
		int i;
		for (i=0;i<s->slot_size;i++) {
			uint32_t handle = (i+s->handle_index) & HANDLE_MASK; //将高八位置为0
			int hash = handle & (s->slot_size-1);	//从1开始到0终止，如果hash为0了，说明slot_size已经用尽了
			if (s->slot[hash] == NULL) {
				s->slot[hash] = ctx;
				s->handle_index = handle + 1;

				rwlock_wunlock(&s->lock);

				handle |= s->harbor; //
				return handle;
			}
		}

		//如果不够分配新的slot，成倍扩充，将老的slot复制过来
		assert((s->slot_size*2 - 1) <= HANDLE_MASK);
		struct skynet_context ** new_slot = skynet_malloc(s->slot_size * 2 * sizeof(struct skynet_context *));
		memset(new_slot, 0, s->slot_size * 2 * sizeof(struct skynet_context *));
		for (i=0;i<s->slot_size;i++) {
			int hash = skynet_context_handle(s->slot[i]) & (s->slot_size * 2 - 1);
			//复制时hash值为slot_size->1->(s->slot_size-1)，复制完以后hash为0处又为NULL了
			assert(new_slot[hash] == NULL);
			new_slot[hash] = s->slot[i];
		}
		skynet_free(s->slot);
		s->slot = new_slot;
		s->slot_size *= 2;
	}
}

/***********************************
 * 销毁某个服务，销毁一个服务包括:
 * 1.销毁结构体:struct skynet_context的内存
 * 2.将对应的s->slot[hash]置为空
 * 3.将相应的s->name内存释放
 ***********************************/
int
skynet_handle_retire(uint32_t handle) {
	int ret = 0;
	struct handle_storage *s = H;

	rwlock_wlock(&s->lock);

	uint32_t hash = handle & (s->slot_size-1);
	struct skynet_context * ctx = s->slot[hash];

	if (ctx != NULL && skynet_context_handle(ctx) == handle) {
		s->slot[hash] = NULL;	//释放相应的服务的指向
		ret = 1;
		int i;
		int j=0, n=s->name_count;
		for (i=0; i<n; ++i) {
			if (s->name[i].handle == handle) {
				skynet_free(s->name[i].name);
				continue;
			} else if (i!=j) {
				s->name[j] = s->name[i]; //如果有删除的，就将整个数组前移，保证数组的连续性
			}
			++j;
		}
		s->name_count = j;
	} else {
		ctx = NULL;
	}

	rwlock_wunlock(&s->lock);

	if (ctx) {
		// release ctx may call skynet_handle_* , so wunlock first.
		skynet_context_release(ctx);
	}

	return ret;
}

/***********************************************
* 销毁所有的服务,步骤为:
* 1.从s->slot中找到ctx
* 2.由ctx找到handle(即地址)
* 3.由handle调用skynet_handle_retire销毁这个服务
***********************************************/
void 
skynet_handle_retireall() {
	struct handle_storage *s = H;
	for (;;) {
		int n=0;
		int i;
		for (i=0;i<s->slot_size;i++) {
			rwlock_rlock(&s->lock);
			struct skynet_context * ctx = s->slot[i];
			uint32_t handle = 0;
			if (ctx)
				handle = skynet_context_handle(ctx);
			rwlock_runlock(&s->lock);
			if (handle != 0) {
				if (skynet_handle_retire(handle)) {
					++n;
				}
			}
		}
		if (n==0)
			return;
	}
}

/***********************************************
* 由服务地址得到服务结构体，并将ctx->ref原子性加1
***********************************************/
struct skynet_context * 
skynet_handle_grab(uint32_t handle) {
	struct handle_storage *s = H;
	struct skynet_context * result = NULL;

	rwlock_rlock(&s->lock);

	uint32_t hash = handle & (s->slot_size-1);
	struct skynet_context * ctx = s->slot[hash];

	//skynet_context_handle:取得ctx->handle
	if (ctx && skynet_context_handle(ctx) == handle) {
		result = ctx;

		//ctx->ref+1，大概是给读写锁用的
		skynet_context_grab(result);
	}

	rwlock_runlock(&s->lock);

	return result;
}

/***********************************************
* 折半查找全局名字对应的服务的地址
***********************************************/
uint32_t 
skynet_handle_findname(const char * name) {
	struct handle_storage *s = H;

	rwlock_rlock(&s->lock);

	uint32_t handle = 0;

	int begin = 0;
	int end = s->name_count - 1;
	while (begin<=end) {
		int mid = (begin+end)/2;
		struct handle_name *n = &s->name[mid];
		int c = strcmp(n->name, name);
		if (c==0) {
			handle = n->handle;
			break;
		}
		if (c<0) {
			begin = mid + 1;
		} else {
			end = mid - 1;
		}
	}

	rwlock_runlock(&s->lock);

	return handle;
}
/***********************************************
* 在某个名字之前将全局名字注册进去，所以全局名字是按顺序保存的，便于折半查找
***********************************************/
static void
_insert_name_before(struct handle_storage *s, char *name, uint32_t handle, int before) {
	if (s->name_count >= s->name_cap) { 	//如果全局名字空间不够用了，就成倍扩充
		s->name_cap *= 2;
		assert(s->name_cap <= MAX_SLOT_SIZE);
		struct handle_name * n = skynet_malloc(s->name_cap * sizeof(struct handle_name));
		int i;
		for (i=0;i<before;i++) { 	
			n[i] = s->name[i];	//将before之前的老的复制到新的
		}
		for (i=before;i<s->name_count;i++) {
			n[i+1] = s->name[i];	//before的位置留给要插入的name
		}
		skynet_free(s->name);	//释放老的
		s->name = n;	//全局名字地址管理指向新分配的
	} else {
		int i;
		for (i=s->name_count;i>before;i--) {
			s->name[i] = s->name[i-1]; //将before之后的全局往后移，腾出空间给要插入的name
		}
	}
	s->name[before].name = name;
	s->name[before].handle = handle;
	s->name_count ++;
}

/***********************************************
* 按顺序注册全局名字
* 1.先折半查找出要插入的点
* 2.再调用_insert_name_before将对应的name插入进去
***********************************************/
static const char *
_insert_name(struct handle_storage *s, const char * name, uint32_t handle) {
	int begin = 0;
	int end = s->name_count - 1;
	while (begin<=end) {
		int mid = (begin+end)/2;
		struct handle_name *n = &s->name[mid];
		int c = strcmp(n->name, name);
		if (c==0) {
			return NULL;
		}
		if (c<0) {
			begin = mid + 1;
		} else {
			end = mid - 1;
		}
	}
	char * result = skynet_strdup(name);

	_insert_name_before(s, result, handle, begin);

	return result;
}

//注册全局名字
const char * 
skynet_handle_namehandle(uint32_t handle, const char *name) {
	rwlock_wlock(&H->lock);

	const char * ret = _insert_name(H, name, handle);

	rwlock_wunlock(&H->lock);

	return ret;
}

void 
skynet_handle_init(int harbor) {
	assert(H==NULL);
	struct handle_storage * s = skynet_malloc(sizeof(*H));
	s->slot_size = DEFAULT_SLOT_SIZE;
	s->slot = skynet_malloc(s->slot_size * sizeof(struct skynet_context *));
	memset(s->slot, 0, s->slot_size * sizeof(struct skynet_context *));

	rwlock_init(&s->lock);
	// reserve 0 for system
	s->harbor = (uint32_t) (harbor & 0xff) << HANDLE_REMOTE_SHIFT; //将harbor置为高8位的，这样能区分是哪里来的地址
	s->handle_index = 1;
	s->name_cap = 2;
	s->name_count = 0;
	s->name = skynet_malloc(s->name_cap * sizeof(struct handle_name));

	H = s;

	// Don't need to free H
}

