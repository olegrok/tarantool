/*
 * Copyright 2010-2018, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#include "box/lua/ctl.h"
#include "box/lua/info.h"

#include <tarantool_ev.h>

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include "lua/utils.h"

#include "box/info.h"
#include "box/box.h"
#include "box/promote.h"
#include "box/error.h"

static int
lbox_ctl_wait_ro(struct lua_State *L)
{
	int index = lua_gettop(L);
	double timeout = TIMEOUT_INFINITY;
	if (index > 0)
		timeout = luaL_checknumber(L, 1);
	if (box_wait_ro(true, timeout) != 0)
		return luaT_error(L);
	return 0;
}

static int
lbox_ctl_wait_rw(struct lua_State *L)
{
	int index = lua_gettop(L);
	double timeout = TIMEOUT_INFINITY;
	if (index > 0)
		timeout = luaL_checknumber(L, 1);
	if (box_wait_ro(false, timeout) != 0)
		return luaT_error(L);
	return 0;
}

/**
 * Lua binding for box_ctl_promote. Takes non-mandatory options:
 * timeout and quorum.
 * @param L Lua stack.
 * @retval Number of values pushed onto the stack. 2 means nil and
 *         error object. 1 means ok and the value is true.
 */
static int
lbox_ctl_promote(struct lua_State *L)
{
	int quorum = -1;
	double timeout = TIMEOUT_INFINITY;
	int top = lua_gettop(L);
	if (top > 1) {
usage_error:
		return luaL_error(L, "Usage: box.ctl.promote([{timeout = "\
				  "<double>, quorum = <unsigned>}])");
	} else if (top == 1) {
		lua_getfield(L, 1, "quorum");
		int ok;
		if (! lua_isnil(L, -1)) {
			quorum = lua_tointegerx(L, -1, &ok);
			if (ok == 0)
				goto usage_error;
		}
		lua_getfield(L, 1, "timeout");
		if (! lua_isnil(L, -1)) {
			timeout = lua_tonumberx(L, -1, &ok);
			if (ok == 0)
				goto usage_error;
		}
	}
	if (box_ctl_promote(timeout, quorum) != 0) {
		lua_pushnil(L);
		luaT_pusherror(L, box_error_last());
		return 2;
	} else {
		lua_pushboolean(L, true);
		return 1;
	}
}

/**
 * Lua binding for box_ctl_promote_reset. Has no arguments.
 * @param L Lua stack.
 * @retval Number of values pushed onto the stack. 2 means nil and
 *         error object. 1 means ok and the value is true.
 */
static int
lbox_ctl_promote_reset(struct lua_State *L)
{
	if (box_ctl_promote_reset() != 0) {
		lua_pushnil(L);
		luaT_pusherror(L, box_error_last());
		return 2;
	}
	lua_pushboolean(L, true);
	return 1;
}

/**
 * Lua binding for box_ctl_promote_info. Has no arguments.
 * @param L Lua stack.
 * @retval Number of values pushed onto the stack. Always is 1 -
 *         a Lua table with info parameters.
 */
static int
lbox_ctl_promote_info(struct lua_State *L)
{
	struct info_handler info;
	luaT_info_handler_create(&info, L);
	box_ctl_promote_info(&info);
	return 1;
}

static const struct luaL_Reg lbox_ctl_lib[] = {
	{"wait_ro", lbox_ctl_wait_ro},
	{"wait_rw", lbox_ctl_wait_rw},
	{"promote", lbox_ctl_promote},
	{"promote_reset", lbox_ctl_promote_reset},
	{"promote_info", lbox_ctl_promote_info},
	{NULL, NULL}
};

void
box_lua_ctl_init(struct lua_State *L)
{
	luaL_register_module(L, "box.ctl", lbox_ctl_lib);
	lua_pop(L, 1);
}
