#include "skynet.h"

#include "skynet_timer.h"
#include "skynet_mq.h"
#include "skynet_server.h"
#include "skynet_handle.h"
#include "spinlock.h"

#include <time.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>

#if defined(__APPLE__)
#include <sys/time.h>
#endif

typedef void (*timer_execute_func)(void *ud,void *arg);

#define TIME_NEAR_SHIFT 8
#define TIME_NEAR (1 << TIME_NEAR_SHIFT)
#define TIME_LEVEL_SHIFT 6
#define TIME_LEVEL (1 << TIME_LEVEL_SHIFT)
#define TIME_NEAR_MASK (TIME_NEAR-1)
#define TIME_LEVEL_MASK (TIME_LEVEL-1)

/*
	struct timer_event是挨着struct timer_node分配的
 	即:每个timer_node都对应于一个timer_event
 */
struct timer_event {
	uint32_t handle;
	int session;
};

struct timer_node {
	struct timer_node *next;
	uint32_t expire;
};

//定时器链表，注册的定时器的struct timer_node每个链表下都有一个结构挂在相应的链表下
struct link_list {
	struct timer_node head;
	struct timer_node *tail;
};


struct timer {
/***********************************
* 链表数组，0-255个粒度注册的定时器都是在这里的
***********************************/
	struct link_list near[TIME_NEAR];	//256个粒度

/***********************************
* 4个层级：每个层级都有64个对应的时间点
* t[i]表示如果注册的时间粒度超过了64^i * 256但是不超过 64^(i+1) * 256，则将其放在t[i]的链表下
***********************************/
	struct link_list t[4][TIME_LEVEL];
	struct spinlock lock;
	uint32_t time;						//从系统启动后经过的滴答数，即多少个1/100秒
	uint32_t starttime;					//系统启动时间(绝对时间，单位为秒)
	uint64_t current;					//相对时间(相对于starttime)
	uint64_t current_point;				//绝对时间
};

static struct timer * TI = NULL;

//清空链表,不释放内存，因为还要用的
static inline struct timer_node *
link_clear(struct link_list *list) {
	struct timer_node * ret = list->head.next;
	list->head.next = 0;
	list->tail = &(list->head);

	return ret;
}

//将struct timer_node挂载在某个链表下
static inline void
link(struct link_list *list,struct timer_node *node) {
	list->tail->next = node;
	list->tail = node;
	node->next=0;
}



/***********************************
x & 63的值在0到63循环
x & 0xff的值在0到255循环
((time|0xff)==(current_time|0xff)):的意义在于计算current_time是经过了n个256,如果time的值在n*256与(n+1)*256之间，则等式成立
((time & 0xff)==0):的意义在于如果time是2^8的整数倍(当然：包括time=0),则等式成立
***********************************/

//将struct timer_node加入到定时器管理结构，方便到时间后取出相应的事件
static void
add_node(struct timer *T,struct timer_node *node) {
	uint32_t time=node->expire;
	uint32_t current_time=T->time;
	
	if ((time|TIME_NEAR_MASK)==(current_time|TIME_NEAR_MASK)) {
		link(&T->near[time&TIME_NEAR_MASK],node);
	} else {
		int i;
		uint32_t mask=TIME_NEAR << TIME_LEVEL_SHIFT;
		for (i=0;i<3;i++) {
			if ((time|(mask-1))==(current_time|(mask-1))) {
				break;
			}
			mask <<= TIME_LEVEL_SHIFT;
		}

		//将node挂在相应的层级上
		link(&T->t[i][((time>>(TIME_NEAR_SHIFT + i*TIME_LEVEL_SHIFT)) & TIME_LEVEL_MASK)],node);
	}
}

//添加node到定时器链表进行统一管理
static void
timer_add(struct timer *T,void *arg,size_t sz,int time) {
	struct timer_node *node = (struct timer_node *)skynet_malloc(sizeof(*node)+sz);
	memcpy(node+1,arg,sz);

	SPIN_LOCK(T);

		node->expire=time+T->time;
		add_node(T,node);

	SPIN_UNLOCK(T);
}

//将某个层级的某个节点清空，并将所有的链表重新添加进struct timer进行管理
static void
move_list(struct timer *T, int level, int idx) {
	struct timer_node *current = link_clear(&T->t[level][idx]);
	while (current) {
		struct timer_node *temp=current->next;
		add_node(T,current);
		current=temp;
	}
}

//主要作用是将在高层级上的定时器链表分配到低层级上去，方便timer_execute对其进行处理
static void
timer_shift(struct timer *T) {
	int mask = TIME_NEAR; // 256
	uint32_t ct = ++T->time;	//在这里转时间轮
	if (ct == 0) {	//溢出了
		move_list(T, 3, 0); //将之前某个时间点注册的时间为：2^32 * 0.01秒的node全部重新挂到当前的struct timer上
	} else {
		uint32_t time = ct >> TIME_NEAR_SHIFT; //8
		int i=0;

		while ((ct & (mask-1))==0) {	//如果是2^8、2^14、2^20、2^26、2^32的整数倍
			int idx=time & TIME_LEVEL_MASK;
			if (idx!=0) {				//如果在此层级的粒度下有注册的定时器,则将其添加到最低层级的表中
				move_list(T, i, idx);
				break;				
			}
			mask <<= TIME_LEVEL_SHIFT;
			time >>= TIME_LEVEL_SHIFT;
			++i;
		}
	}
}

static inline void
dispatch_list(struct timer_node *current) {
	do {
		struct timer_event * event = (struct timer_event *)(current+1);	//取出event，然后对skynet消息赋值
		struct skynet_message message;
		message.source = 0;
		message.session = event->session;
		message.data = NULL;
		message.sz = (size_t)PTYPE_RESPONSE << MESSAGE_TYPE_SHIFT;

		skynet_context_push(event->handle, &message);	//将消息压入相应的服务
		
		struct timer_node * temp = current;
		current=current->next;
		skynet_free(temp);	//处理完之后才释放内存
	} while (current);
}


//取当前绝对时间粒度的低8位，依次取出挂在其上的struct timer_node进行处理
static inline void
timer_execute(struct timer *T) {
	int idx = T->time & TIME_NEAR_MASK;		//0xff
	
	while (T->near[idx].head.next) {	//找到较近的定时器容器
		struct timer_node *current = link_clear(&T->near[idx]);
		SPIN_UNLOCK(T);
		// dispatch_list don't need lock T
		dispatch_list(current);
		SPIN_LOCK(T);
	}
}

static void 
timer_update(struct timer *T) {
	SPIN_LOCK(T);

	// try to dispatch timeout 0 (rare condition)
	timer_execute(T);

	// shift time first, and then dispatch timer message
	timer_shift(T);

	timer_execute(T);

	SPIN_UNLOCK(T);
}

//创建struct timer,将定时器管理的2^32个粒度分为5个层级
static struct timer *
timer_create_timer() {
	struct timer *r=(struct timer *)skynet_malloc(sizeof(struct timer));
	memset(r,0,sizeof(*r));

	int i,j;

	for (i=0;i<TIME_NEAR;i++) {
		link_clear(&r->near[i]);
	}

	for (i=0;i<4;i++) {
		for (j=0;j<TIME_LEVEL;j++) {
			link_clear(&r->t[i][j]);
		}
	}

	SPIN_INIT(r)

	r->current = 0;

	return r;
}

//上层的skynet.timeout最终会调用此借口
int
skynet_timeout(uint32_t handle, int time, int session) {
	if (time <= 0) {
		struct skynet_message message;
		message.source = 0;
		message.session = session;
		message.data = NULL;
		message.sz = (size_t)PTYPE_RESPONSE << MESSAGE_TYPE_SHIFT;

		if (skynet_context_push(handle, &message)) {
			return -1;
		}
	} else {
		struct timer_event event;
		event.handle = handle;
		event.session = session;
		timer_add(TI, &event, sizeof(event), time);
	}

	return session;
}

// 返回起始时间(绝对时间),和相对于起始时间经过了多少个0.01s的相对时间
// centisecond: 1/100 second
static void
systime(uint32_t *sec, uint32_t *cs) {
#if !defined(__APPLE__)
	struct timespec ti;
	clock_gettime(CLOCK_REALTIME, &ti);
	*sec = (uint32_t)ti.tv_sec;
	*cs = (uint32_t)(ti.tv_nsec / 10000000);
#else
	struct timeval tv;
	gettimeofday(&tv, NULL);
	*sec = tv.tv_sec;
	*cs = tv.tv_usec / 10000;
#endif
}

//返回从1970年1月1日到经历过多少个1/100秒
static uint64_t
gettime() {
	uint64_t t;
#if !defined(__APPLE__)
	struct timespec ti;
	clock_gettime(CLOCK_MONOTONIC, &ti);
	t = (uint64_t)ti.tv_sec * 100;
	t += ti.tv_nsec / 10000000;
#else
	struct timeval tv;
	gettimeofday(&tv, NULL);
	t = (uint64_t)tv.tv_sec * 100;  //百分之一秒的精度
	t += tv.tv_usec / 10000;
#endif
	return t;
}

void
skynet_updatetime(void) {
	uint64_t cp = gettime();
	if(cp < TI->current_point) {
		skynet_error(NULL, "time diff error: change from %lld to %lld", cp, TI->current_point);
		TI->current_point = cp;
	} else if (cp != TI->current_point) {
		uint32_t diff = (uint32_t)(cp - TI->current_point);
		TI->current_point = cp;	//更新绝对时间
		TI->current += diff;
		int i;
		for (i=0;i<diff;i++) {	//经过了多少个时间粒度就执行多少次,一般diff为1
			timer_update(TI);
		}
	}
}

uint32_t
skynet_starttime(void) {
	return TI->starttime;
}

uint64_t 
skynet_now(void) {
	return TI->current;
}

void 
skynet_timer_init(void) {
	TI = timer_create_timer();
	uint32_t current = 0;

	//执行完systime后TI->starttime单位为秒，current单位为1/100
	systime(&TI->starttime, &current);
	TI->current = current;		//相对时间，相对starttime来说经过了多少个1/100秒
	TI->current_point = gettime();
}

