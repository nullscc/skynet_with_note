#include <stdio.h>
#include <lua.h>
#include <lauxlib.h>

#include <time.h>

#if defined(__APPLE__)
#include <mach/task.h>
#include <mach/mach.h>
#endif

#define NANOSEC 1000000000
#define MICROSEC 1000000

// #define DEBUG_LOG

static double
get_time() {
#if  !defined(__APPLE__)
	struct timespec ti;
	clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ti);

	int sec = ti.tv_sec & 0xffff;
	int nsec = ti.tv_nsec;

	return (double)sec + (double)nsec / NANOSEC;	
#else
	struct task_thread_times_info aTaskInfo;
	mach_msg_type_number_t aTaskInfoCount = TASK_THREAD_TIMES_INFO_COUNT;
	if (KERN_SUCCESS != task_info(mach_task_self(), TASK_THREAD_TIMES_INFO, (task_info_t )&aTaskInfo, &aTaskInfoCount)) {
		return 0;
	}

	int sec = aTaskInfo.user_time.seconds & 0xffff;
	int msec = aTaskInfo.user_time.microseconds;

	return (double)sec + (double)msec / MICROSEC;
#endif
}

static inline double 
diff_time(double start) {
	double now = get_time();
	if (now < start) {
		return now + 0x10000 - start;
	} else {
		return now - start;
	}
}

/*****************************
* 有两个upvalue:
* 1.start time
* 2.total time
*****************************/
static int
lstart(lua_State *L) {
	if (lua_type(L,1) == LUA_TTHREAD) {	//如果第一个参数是一个lua线程,则将栈顶设置为第一个参数
		lua_settop(L,1);
	} else {
		lua_pushthread(L);	//如果第一个参数不是一个lua线程(一般不带参数),则将当前线程压栈
	}
	lua_rawget(L, lua_upvalueindex(2));	//将total time压栈
	if (!lua_isnil(L, -1)) {	//如果不是total time对应的[thread]不是nil，说明没有连续调用了两次start
		return luaL_error(L, "Thread %p start profile more than once", lua_topointer(L, 1));
	}

	//total_time[Lthread]=0
	lua_pushthread(L);
	lua_pushnumber(L, 0);
	lua_rawset(L, lua_upvalueindex(2));

	//start_time[Lthread]=0
	lua_pushthread(L);
	double ti = get_time();
#ifdef DEBUG_LOG
	fprintf(stderr, "PROFILE [%p] start\n", L);
#endif
	lua_pushnumber(L, ti);
	lua_rawset(L, lua_upvalueindex(1));

	return 0;
}

/*****************************
* 有两个upvalue:
* 1.start time
* 2.total time
*
* 返回从start到stop经历的总时间
*****************************/
static int
lstop(lua_State *L) {
	if (lua_type(L,1) == LUA_TTHREAD) {
		lua_settop(L,1);
	} else {
		lua_pushthread(L);
	}
	lua_rawget(L, lua_upvalueindex(1));
	if (lua_type(L, -1) != LUA_TNUMBER) {
		return luaL_error(L, "Call profile.start() before profile.stop()");
	} 
	double ti = diff_time(lua_tonumber(L, -1));

	//以线程为键，取得total_time[thread]的值
	lua_pushthread(L);
	lua_rawget(L, lua_upvalueindex(2));
	double total_time = lua_tonumber(L, -1);

	//start_time[thread] = nil
	lua_pushthread(L);
	lua_pushnil(L);
	lua_rawset(L, lua_upvalueindex(1));

	//total_time[thread] = nil
	lua_pushthread(L);
	lua_pushnil(L);
	lua_rawset(L, lua_upvalueindex(2));

	total_time += ti;
	lua_pushnumber(L, total_time);
#ifdef DEBUG_LOG
	fprintf(stderr, "PROFILE [%p] stop (%lf / %lf)\n", L, ti, total_time);
#endif

	return 1;
}

static int
timing_resume(lua_State *L) {
#ifdef DEBUG_LOG
	lua_State *from = lua_tothread(L, -1);
#endif
	lua_rawget(L, lua_upvalueindex(2));	//得到total_time
	if (lua_isnil(L, -1)) {		// check total time,如果是nil，就证明没有调用profile.start
		lua_pop(L,1);
	} else {
		lua_pop(L,1);
		lua_pushvalue(L,1);
		double ti = get_time();
#ifdef DEBUG_LOG
		fprintf(stderr, "PROFILE [%p] resume\n", from);
#endif
		lua_pushnumber(L, ti);
		lua_rawset(L, lua_upvalueindex(1));	// set start time
	}

	lua_CFunction co_resume = lua_tocfunction(L, lua_upvalueindex(3));

	//调用lua coroutine的coroutine.resume
	return co_resume(L);
}

/*****************************
* 有三个upvalue:
* 1.start time
* 2.total time
* 3.co_resume,即lua的coroutine.resume
*****************************/
static int
lresume(lua_State *L) {
	lua_pushvalue(L,1);	//把lua thread压栈
	
	return timing_resume(L);
}

/*****************************
* 有三个upvalue:
* 1.start time
* 2.total time
* 3.co_resume,即lua的coroutine.resume
*****************************/
static int
lresume_co(lua_State *L) {
	luaL_checktype(L, 2, LUA_TTHREAD);
	lua_rotate(L, 2, -1);

	return timing_resume(L);
}

static int
timing_yield(lua_State *L) {
#ifdef DEBUG_LOG
	lua_State *from = lua_tothread(L, -1);
#endif
	lua_rawget(L, lua_upvalueindex(2));	// check total time
	if (lua_isnil(L, -1)) {
		lua_pop(L,1);
	} else {
		double ti = lua_tonumber(L, -1);
		lua_pop(L,1);

		//以线程为键
		lua_pushthread(L);
		lua_rawget(L, lua_upvalueindex(1));
		double starttime = lua_tonumber(L, -1);
		lua_pop(L,1);

		double diff = diff_time(starttime);
		ti += diff;
#ifdef DEBUG_LOG
		fprintf(stderr, "PROFILE [%p] yield (%lf/%lf)\n", from, diff, ti);
#endif

		lua_pushthread(L);
		lua_pushnumber(L, ti);
		lua_rawset(L, lua_upvalueindex(2));
	}

	lua_CFunction co_yield = lua_tocfunction(L, lua_upvalueindex(3));

	return co_yield(L);
}

/*****************************
* 有三个upvalue:
* 1.start time
* 2.total time
* 3.co_resume,即lua的coroutine.yield
*****************************/
static int
lyield(lua_State *L) {
	lua_pushthread(L);

	return timing_yield(L);
}

/*****************************
* 有三个upvalue:
* 1.start time
* 2.total time
* 3.co_resume,即lua的coroutine.yield
*****************************/
static int
lyield_co(lua_State *L) {
	luaL_checktype(L, 1, LUA_TTHREAD);
	lua_rotate(L, 1, -1);
	
	return timing_yield(L);
}

int
luaopen_profile(lua_State *L) {
	luaL_checkversion(L);
	luaL_Reg l[] = {
		{ "start", lstart },
		{ "stop", lstop },
		{ "resume", lresume },
		{ "yield", lyield },
		{ "resume_co", lresume_co },
		{ "yield_co", lyield_co },
		{ NULL, NULL },
	};
	luaL_newlibtable(L,l);

	//创建一张空表
	lua_newtable(L);	// table thread->start time

	//创建一张空表
	lua_newtable(L);	// table thread->total time

	//创建一张空表
	lua_newtable(L);	// weak table
	//将"kv"压栈
	lua_pushliteral(L, "kv");
	//设置栈的倒数第2个元素的表的"__mode"字段为"kv",并将"kv"从栈上弹出，这样目前栈中只有两张空表，一张{__mode="kv"}
	lua_setfield(L, -2, "__mode");

	//将{__mode="kv"}复制一份,这样栈中有2张空表，2张{__mode="kv"}
	lua_pushvalue(L, -1);

	//给栈中的1,2两张表设置元表为{__mode="kv"},并将两张{__mode="kv"}弹出,这样栈中只有两张元表为{__mode="kv"}的空表了
	lua_setmetatable(L, -3); 
	lua_setmetatable(L, -3);

	lua_pushnil(L);	// cfunction (coroutine.resume or coroutine.yield)

	//数组l中的所有函数都注册到nil中，所以nil现在是一个table，table中的元素为l中的函数
	luaL_setfuncs(L,l,3);

	//libtable为栈顶元素的索引，即栈上目前有x个元素,就返回x，这里为libtable为3
	int libtable = lua_gettop(L);

	//将全局变量"coroutine"里的值压栈,即将协程库压栈
	lua_getglobal(L, "coroutine");

	//将coroutine.resume压栈
	lua_getfield(L, -1, "resume");

	//co_resume = coroutine.resume
	lua_CFunction co_resume = lua_tocfunction(L, -1);
	if (co_resume == NULL)
		return luaL_error(L, "Can't get coroutine.resume");

	//将coroutine.resume从栈上弹出
	lua_pop(L,1);

	//将l.resume压栈
	lua_getfield(L, libtable, "resume");
	//将co_resume压栈
	lua_pushcfunction(L, co_resume);
	//设置l.resume的upvalue为co_resume,并将co_resume弹出
	lua_setupvalue(L, -2, 3);	//-2表示要设置的函数在栈中的位置，3表示要设置的是第几个upvalue
	//弹出l.resume
	lua_pop(L,1);

	//设置l.resume_co的上值为co_resume
	lua_getfield(L, libtable, "resume_co");
	lua_pushcfunction(L, co_resume);
	lua_setupvalue(L, -2, 3);
	lua_pop(L,1);

	//将coroutine.yield压栈
	lua_getfield(L, -1, "yield");

	lua_CFunction co_yield = lua_tocfunction(L, -1);
	if (co_yield == NULL)
		return luaL_error(L, "Can't get coroutine.yield");
	lua_pop(L,1);

	//设置l.yield的上值为co_yield
	lua_getfield(L, libtable, "yield");
	lua_pushcfunction(L, co_yield);
	lua_setupvalue(L, -2, 3);
	lua_pop(L,1);

	//设置l.yield_co的上值为co_yield
	lua_getfield(L, libtable, "yield_co");
	lua_pushcfunction(L, co_yield);
	lua_setupvalue(L, -2, 3);
	lua_pop(L,1);

	lua_settop(L, libtable);

	return 1;
}
