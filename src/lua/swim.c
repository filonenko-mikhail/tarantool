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

#include "utils.h"
#include "diag.h"
#include "tt_uuid.h"
#include "swim/swim.h"
#include "swim/swim_transport.h"
#include "small/ibuf.h"
#include "lua/info.h"
#include <info.h>

/** SWIM instances are pushed as cdata with this id. */
uint32_t CTID_STRUCT_SWIM_PTR;

/**
 * Get @a n-th value from a Lua stack as a struct swim pointer.
 * @param L Lua state.
 * @param n Where pointer is stored on Lua stack.
 *
 * @retval NULL The stack position does not exist or it is not a
 *         struct swim pointer.
 * @retval not NULL Valid SWIM pointer.
 */
static inline struct swim *
lua_swim_ptr(struct lua_State *L, int n)
{
	uint32_t ctypeid;
	if (lua_type(L, n) != LUA_TCDATA)
		return NULL;
	void *swim = luaL_checkcdata(L, n, &ctypeid);
	if (ctypeid != CTID_STRUCT_SWIM_PTR)
		return NULL;
	return *(struct swim **) swim;
}

/**
 * Delete SWIM instance passed via first Lua stack position. Used
 * by Lua GC.
 */
static int
lua_swim_gc(struct lua_State *L)
{
	struct swim *swim = lua_swim_ptr(L, 1);
	if (swim == NULL)
		return luaL_error(L, "SWIM gc expected struct swim *");
	swim_delete(swim);
	return 0;
}

/**
 * Get a value from a table that is supposed to be a timeout.
 * @param L Lua state.
 * @param ncfg Where on the Lua stack a table with the timeout is
 *        stored.
 * @param fieldname Name of the table field storing the timeout.
 * @param funcname Caller function name, used to build a detailed
 *        error message.
 *
 * @retval 0 > A timeout value.
 * @retval -1 The field is nil.
 */
static inline double
lua_swim_get_timeout_field(struct lua_State *L, int ncfg, const char *fieldname,
			   const char *funcname)
{
	double timeout = -1;
	lua_getfield(L, ncfg, fieldname);
	if (lua_isnumber(L, -1)) {
		timeout = lua_tonumber(L, -1);
		if (timeout <= 0) {
			return luaL_error(L, "swim.%s: %s should be positive "\
					  "number", funcname, fieldname);
		}
	} else if (! lua_isnil(L, -1)) {
		return luaL_error(L, "swim.%s: %s should be positive number",
				  funcname, fieldname);
	}
	lua_pop(L, 1);
	return timeout;
}

/**
 * Get a value from a table that is supposed to be a UUID.
 * @param L Lua state.
 * @param ncfg Where on the Lua stack a table with the UUID is
 *        stored.
 * @param fieldname Name of the table field storing the UUID.
 * @param funcname Caller function name, used to build a detailed
 *        error message.
 * @param[out] uuid Result UUID. Nil UUID is stored, if the field
 *       was nil.
 */
static inline void
lua_swim_get_uuid_field(struct lua_State *L, int ncfg, const char *fieldname,
			const char *funcname, struct tt_uuid *uuid)
{
	lua_getfield(L, ncfg, fieldname);
	if (lua_isstring(L, -1)) {
		if (tt_uuid_from_string(lua_tostring(L, -1), uuid) != 0) {
			luaL_error(L, "swim.%s: %s is invalid", funcname,
				   fieldname);
		}
	} else if (lua_isnil(L, -1)) {
		*uuid = uuid_nil;
	} else {
		luaL_error(L, "swim.%s: %s should be a string", funcname,
			   fieldname);
	}
	lua_pop(L, 1);
}

/**
 * Get a value from a table that is supposed to be a URI.
 * @param L Lua state.
 * @param ncfg Where on the Lua stack a table with the URI is
 *        stored.
 * @param fieldname Name of the table field storing the URI.
 * @param funcname Caller function name, used to build a detailed
 *        error message.
 *
 * @retval not NULL A URI.
 * @retval NULL The field is nil.
 */
static inline const char *
lua_swim_get_uri_field(struct lua_State *L, int ncfg, const char *fieldname,
		       const char *funcname)
{
	const char *uri = NULL;
	lua_getfield(L, ncfg, fieldname);
	if (lua_isstring(L, -1)) {
		uri = lua_tostring(L, -1);
	} else if (! lua_isnil(L, -1)) {
		luaL_error(L, "swim.%s: %s should be a string URI", funcname,
			   fieldname);
	}
	lua_pop(L, 1);
	return uri;
}

/**
 * Configure @a swim instance using a table stored in @a ncfg-th
 * position on the Lua stack.
 * @param L Lua state.
 * @param ncfg Where configuration is stored on the Lua stack.
 * @param swim SWIM instance to configure.
 * @param funcname Caller function name to use in error messages.
 *
 * @retval 0 Success.
 * @retval -1 Error, stored in diagnostics area. Critical errors
 *         like OOM or incorrect usage are thrown.
 */
static int
lua_swim_cfg_impl(struct lua_State *L, int ncfg, struct swim *swim,
		  const char *funcname)
{
	if (! lua_istable(L, ncfg)) {
		return luaL_error(L, "swim.%s: expected table config",
				  funcname);
	}
	const char *server_uri =
		lua_swim_get_uri_field(L, ncfg, "server", funcname);
	struct tt_uuid uuid;
	lua_swim_get_uuid_field(L, ncfg, "uuid", funcname, &uuid);
	double heartbeat_rate =
		lua_swim_get_timeout_field(L, ncfg, "heartbeat", funcname);
	double ack_timeout =
		lua_swim_get_timeout_field(L, ncfg, "ack_timeout", funcname);

	return swim_cfg(swim, server_uri, heartbeat_rate, ack_timeout, &uuid);
}

/**
 * Create a new SWIM instance. The Lua stack can contain either 0
 * parameters to just create a new non-configured SWIM instance,
 * or 1 parameter with a config to configure the new instance
 * immediately.
 * @param L Lua state.
 * @retval 1 A SWIM instance.
 * @retval 2 Nil and an error object. On invalid Lua parameters
 *         and OOM it throws.
 */
static int
lua_swim_new(struct lua_State *L)
{
	int top = lua_gettop(L);
	if (top > 1)
		return luaL_error(L, "Usage: swim.new([{<config>}])");
	struct swim *swim = swim_new();
	if (swim != NULL) {
		*(struct swim **)luaL_pushcdata(L, CTID_STRUCT_SWIM_PTR) = swim;
		lua_pushcfunction(L, lua_swim_gc);
		luaL_setcdatagc(L, -2);
		if (top == 0 || lua_swim_cfg_impl(L, 1, swim, "new") == 0)
			return 1;
		lua_pop(L, 1);
	}
	lua_pushnil(L);
	luaT_pusherror(L, diag_last_error(diag_get()));
	return 2;
}

/**
 * Configure an existing SWIM instance. The Lua stack should
 * contain two values - a SWIM instance to configure, and a
 * config.
 * @param L Lua state.
 * @retval 1 True.
 * @retval 2 Nil and an error object. On invalid Lua parameters
 *         and OOM it throws.
 */
static int
lua_swim_cfg(struct lua_State *L)
{
	struct swim *swim = lua_swim_ptr(L, 1);
	if (swim == NULL)
		return luaL_error(L, "Usage: swim:cfg({<config>})");
	if (lua_swim_cfg_impl(L, 2, swim, "cfg") != 0) {
		lua_pushnil(L);
		luaT_pusherror(L, diag_last_error(diag_get()));
		return 2;
	}
	lua_pushboolean(L, true);
	return 1;
}

/**
 * Add a new member to a SWIM instance. The Lua stack should
 * contain two values - a SWIM instance to add to, and a config of
 * a new member. Config is a table, containing UUID and URI keys.
 * @param L Lua state.
 * @retval 1 True.
 * @retval 2 Nil and an error object. On invalid Lua parameters
 *         and OOM it throws.
 */
static int
lua_swim_add_member(struct lua_State *L)
{
	struct swim *swim = lua_swim_ptr(L, 1);
	if (lua_gettop(L) != 2 || swim == NULL || !lua_istable(L, 1))
		return luaL_error(L, "Usage: swim:add_member({<config>})");
	const char *uri = lua_swim_get_uri_field(L, 1, "uri", "add_member");
	struct tt_uuid uuid;
	lua_swim_get_uuid_field(L, 1, "uuid", "add_member", &uuid);

	if (swim_add_member(swim, uri, &uuid) != 0) {
		lua_pushnil(L);
		luaT_pusherror(L, diag_last_error(diag_get()));
		return 2;
	}
	lua_pushboolean(L, true);
	return 1;
}

/**
 * Silently remove a member from a SWIM instance's members table.
 * The Lua stack should contain two values - a SWIM instance to
 * remove from, and a UUID of a sentenced member.
 * @param L Lua state.
 * @retval 1 True.
 * @retval 2 Nil and an error object. On invalid Lua parameters
 *         and OOM it throws.
 */
static int
lua_swim_remove_member(struct lua_State *L)
{
	struct swim *swim = lua_swim_ptr(L, 1);
	if (lua_gettop(L) != 2 || swim == NULL)
		return luaL_error(L, "Usage: swim:remove_member(uuid)");
	if (! lua_isstring(L, -1)) {
		return luaL_error(L, "swim.remove_member: member UUID should "\
				  "be a string");
	}
	struct tt_uuid uuid;
	if (tt_uuid_from_string(lua_tostring(L, 1), &uuid) != 0)
		return luaL_error(L, "swim.remove_member: invalid UUID");

	if (swim_remove_member(swim, &uuid) != 0) {
		lua_pushnil(L);
		luaT_pusherror(L, diag_last_error(diag_get()));
		return 2;
	}
	lua_pushboolean(L, true);
	return 1;
}

/** Remove a SWIM instance pointer from Lua space, nullify. */
static void
lua_swim_invalidate(struct lua_State *L)
{
	uint32_t ctypeid;
	struct swim **cdata = (struct swim **) luaL_checkcdata(L, 1, &ctypeid);
	assert(ctypeid == CTID_STRUCT_SWIM_PTR);
	*cdata = NULL;
}

/**
 * Destroy and delete a SWIM instance. All its memory is freed, it
 * stops participating in any rounds, the socket is closed. No
 * special quit messages are broadcasted - the quit is silent. So
 * other members will think that this one is dead. The Lua stack
 * should contain one value - a SWIM instance to delete.
 */
static int
lua_swim_delete(struct lua_State *L)
{
	struct swim *swim = lua_swim_ptr(L, 1);
	if (swim == NULL)
		return luaL_error(L, "Usage: swim:delete()");
	swim_delete(swim);
	lua_swim_invalidate(L);
	return 0;
}

/**
 * Collect information about this instance's members table.
 * @param L Lua state.
 * @retval 1 Info table.
 */
static int
lua_swim_info(struct lua_State *L)
{
	struct swim *swim = lua_swim_ptr(L, 1);
	if (swim == NULL)
		return luaL_error(L, "Usage: swim:info()");
	struct info_handler info;
	luaT_info_handler_create(&info, L);
	swim_info(swim, &info);
	return 1;
}

/**
 * Send a ping to a URI assuming that there is a member, which
 * will respond with an ack, and will be added to the local
 * members table.The Lua stack should contain two values - a SWIM
 * instance to probe by, and a URI of a member.
 * @param L Lua state.
 * @retval 1 True.
 * @retval 2 Nil and an error object. On invalid Lua parameters
 *         and OOM it throws.
 */
static int
lua_swim_probe_member(struct lua_State *L)
{
	struct swim *swim = lua_swim_ptr(L, 1);
	if (lua_gettop(L) != 2 || swim == NULL)
		return luaL_error(L, "Usage: swim:probe_member(uri)");
	if (! lua_isstring(L, 2)) {
		return luaL_error(L, "swim.probe_member: member URI should "\
				  "be a string");
	}
	if (swim_probe_member(swim, lua_tostring(L, 2)) != 0) {
		lua_pushnil(L);
		luaT_pusherror(L, diag_last_error(diag_get()));
		return 2;
	}
	lua_pushboolean(L, true);
	return 1;
}

/**
 * Gracefully leave the cluster. The Lua stack should contain one
 * value - a SWIM instance. After this method is called, the SWIM
 * instance is deleted and can not be used.
 * @param L Lua state.
 */
static int
lua_swim_quit(struct lua_State *L)
{
	struct swim *swim = lua_swim_ptr(L, 1);
	if (swim == NULL)
		return luaL_error(L, "Usage: swim:quit()");
	swim_quit(swim);
	lua_swim_invalidate(L);
	return 0;
}

/**
 * Broadcast a ping over all network interfaces with a speicifed
 * port. Port is optional and in a case of absence it is set to
 * a port of the current instance. The Lua stack should contain a
 * SWIM instance to broadcast from, and optionally a port.
 * @param L Lua state.
 * @retval 1 True.
 * @retval 2 Nil and an error object. On invalid Lua parameters
 *         and OOM it throws.
 */
static int
lua_swim_broadcast(struct lua_State *L)
{
	struct swim *swim = lua_swim_ptr(L, 1);
	if (swim == NULL)
		return luaL_error(L, "Usage: swim:broadcast([port])");
	int port = -1;
	if (lua_gettop(L) > 1) {
		if (! lua_isnumber(L, 2)) {
			return luaL_error(L, "swim.broadcast: port should be "\
					  "a number");
		}
		double dport = lua_tonumber(L, 2);
		port = dport;
		if (dport != (double) port) {
			return luaL_error(L, "swim.broadcast: port should be "\
					  "an integer");
		}
	}
	if (swim_broadcast(swim, port) != 0) {
		lua_pushnil(L);
		luaT_pusherror(L, diag_last_error(diag_get()));
		return 2;
	}
	lua_pushboolean(L, true);
	return 1;
}

void
tarantool_lua_swim_init(struct lua_State *L)
{
	static const struct luaL_Reg lua_swim_methods [] = {
		{"new", lua_swim_new},
		{"cfg", lua_swim_cfg},
		{"add_member", lua_swim_add_member},
		{"remove_member", lua_swim_remove_member},
		{"delete", lua_swim_delete},
		{"info", lua_swim_info},
		{"probe_member", lua_swim_probe_member},
		{"quit", lua_swim_quit},
		{"broadcast", lua_swim_broadcast},
		{NULL, NULL}
	};
	luaL_register_module(L, "swim", lua_swim_methods);
	lua_pop(L, 1);
	int rc = luaL_cdef(L, "struct swim;");
	assert(rc == 0);
	(void) rc;
	CTID_STRUCT_SWIM_PTR = luaL_ctypeid(L, "struct swim *");
	assert(CTID_STRUCT_SWIM_PTR != 0);
};
