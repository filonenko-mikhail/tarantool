#include <lua.h>       /* lua_*() */
#include <lauxlib.h>   /* luaL_*() */
#include <lualib.h>    /* luaL_openlibs() */
#include "unit.h"      /* plan, header, footer, is */
#include "lua/utils.h" /* luaL_iterator_*() */

extern char fun_lua[];

int
main()
{
	struct {
		/* A string to output with a test case. */
		const char *description;
		/* A string with Lua code to push an iterator. */
		const char *init;
		/*
		 * How much values are pushed by the Lua code
		 * above.
		 */
		int init_retvals;
		/*
		 * Start values from this number to distinguish
		 * them from its ordinal.
		 */
		int first_value;
		/*
		 * Lua stack index where {gen, param, state} is
		 * placed or zero.
		 */
		int idx;
		/* How much values are in the iterator. */
		int iterations;
	} cases[] = {
		{
			.description = "pairs, zero idx",
			.init = "return pairs({42})",
			.init_retvals = 3,
			.first_value = 42,
			.idx = 0,
			.iterations = 1,
		},
		{
			.description = "ipairs, zero idx",
			.init = "return ipairs({42, 43, 44})",
			.init_retvals = 3,
			.first_value = 42,
			.idx = 0,
			.iterations = 3,
		},
		{
			.description = "luafun iterator, zero idx",
			.init = "return fun.wrap(ipairs({42, 43, 44}))",
			.init_retvals = 3,
			.first_value = 42,
			.idx = 0,
			.iterations = 3,
		},
		{
			.description = "pairs, from table",
			.init = "return {pairs({42})}",
			.init_retvals = 1,
			.first_value = 42,
			.idx = -1,
			.iterations = 1,
		},
		{
			.description = "ipairs, from table",
			.init = "return {ipairs({42, 43, 44})}",
			.init_retvals = 1,
			.first_value = 42,
			.idx = -1,
			.iterations = 3,
		},
		{
			.description = "luafun iterator, from table",
			.init = "return {fun.wrap(ipairs({42, 43, 44}))}",
			.init_retvals = 1,
			.first_value = 42,
			.idx = -1,
			.iterations = 3,
		},
	};

	int cases_cnt = (int) (sizeof(cases) / sizeof(cases[0]));
	/*
	 * * Check stack size after creating luaL_iterator (triple
	 *   times).
	 * * 4 checks per iteration.
	 * * Check that values ends.
	 */
	int planned = 0;
	for (int i = 0; i < cases_cnt; ++i)
		planned += cases[i].iterations * 4 + 4;

	plan(planned);
	header();

	struct lua_State *L = luaL_newstate();
	luaL_openlibs(L);
	tarantool_L = L;

	/*
	 * Check that everything works fine in a thread (a fiber)
	 * other then the main one.
	 */
	L = lua_newthread(L);

	/*
	 * Expose luafun.
	 *
	 * Don't register it in package.loaded for simplicity.
	 */
	luaL_loadstring(L, fun_lua);
	lua_call(L, 0, 1);
	lua_setglobal(L, "fun");

	for (int i = 0; i < cases_cnt; ++i) {
		const char *description = cases[i].description;
		int top = lua_gettop(L);

		/* Push an iterator to the Lua stack. */
		luaL_loadstring(L, cases[i].init);
		lua_call(L, 0, cases[i].init_retvals);

		/* Create the luaL_iterator structure. */
		struct luaL_iterator *it = luaL_iterator_new(L, cases[i].idx);
		lua_pop(L, cases[i].init_retvals);

		/* Check stack size. */
		is(lua_gettop(L) - top, 0, "%s: stack size", description);

		/* Iterate over values and check them. */
		for (int j = 0; j < cases[i].iterations; ++j) {
			int top = lua_gettop(L);
			int rc = luaL_iterator_next(L, it);
			is(rc, 2, "%s: iter %d: gen() retval count",
			   description, j);
			is(luaL_checkinteger(L, -2), j + 1,
			   "%s: iter %d: gen() 1st retval",
			   description, j);
			is(luaL_checkinteger(L, -1), j + cases[i].first_value,
			   "%s: iter %d: gen() 2nd retval",
			   description, j);
			lua_pop(L, 2);
			is(lua_gettop(L) - top, 0, "%s: iter: %d: stack size",
			   description, j);
		}

		/* Check the iterator ends when expected. */
		int rc = luaL_iterator_next(L, it);
		is(rc, 0, "%s: iterator ends", description);

		/* Check stack size. */
		is(lua_gettop(L) - top, 0, "%s: stack size", description);

		/* Free the luaL_iterator structure. */
		luaL_iterator_delete(it);

		/* Check stack size. */
		is(lua_gettop(L) - top, 0, "%s: stack size", description);
	}

	footer();
	return check_plan();
}
