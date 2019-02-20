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

#include "box/lua/merger.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

#include <lua.h>
#include <lauxlib.h>

#include "lua/error.h"
#include "lua/utils.h"
#include "small/ibuf.h"
#include "msgpuck.h"
#include "mpstream.h"
#include "lua/msgpack.h"

#define HEAP_FORWARD_DECLARATION
#include "salad/heap.h"

#include "box/field_def.h"
#include "box/key_def.h"
#include "box/lua/key_def.h"
#include "box/schema_def.h"
#include "box/tuple.h"
#include "box/lua/tuple.h"
#include "box/box.h"
#include "box/index.h"
#include "diag.h"

static bool
source_less(const heap_t *heap, const struct heap_node *a,
	    const struct heap_node *b);
#define HEAP_NAME merger_heap
#define HEAP_LESS source_less
#include "salad/heap.h"

static uint32_t merger_context_type_id = 0;
static uint32_t merger_state_type_id = 0;
static uint32_t ibuf_type_id = 0;

enum { MERGER_SOURCE_REF_MAX = INT_MAX };

/* {{{ Merger structures */

struct merger_source;
struct merger_context;
struct merger_state;

struct merger_source_vtab {
	/**
	 * Free the merger source.
	 *
	 * Don't call it directly, use merger_source_unref()
	 * instead.
	 *
	 * We need to know Lua state here, because sources of
	 * table and iterator types are saved as references within
	 * the Lua state.
	 */
	void (*delete)(struct merger_source *base, struct lua_State *L);
	/**
	 * Update source->tuple of specific source.
	 *
	 * Increases the reference counter of the tuple.
	 *
	 * Return 0 when successfully fetched a tuple or NULL. In
	 * case of an error set a diag and return -1.
	 */
	int (*next)(struct merger_source *base, box_tuple_format_t *format,
		    const struct merger_state *state, struct lua_State *L);
};

/**
 * Base (abstract) structure to represent a merge source state.
 * Concrete implementations are in box/lua/merger.c.
 */
struct merger_source {
	/* Source-specific methods. */
	struct merger_source_vtab *vtab;
	/* Ordinal number of the source. */
	int idx;
	/* How huch tuples were used from this source. */
	uint32_t processed;
	/* Next tuple. */
	struct tuple *tuple;
	/*
	 * A source is the heap node. Compared by the next tuple.
	 */
	struct heap_node hnode;
	/* Reference counter. */
	int refs;
};

/**
 * Holds immutable parameters of a merger.
 */
struct merger_context {
	struct key_def *key_def;
	box_tuple_format_t *format;
};

/**
 * Holds parameters of merge process, sources, result storage
 * (if any), heap of sources and utility flags / counters.
 */
struct merger_state {
	/* Heap of sources. */
	heap_t heap;
	/*
	 * Copy of key_def from merger_context.
	 *
	 * A merger_context can be collected by LuaJIT GC
	 * independently from a merger_state, so we need either
	 * copy key_def or implement reference counting for
	 * merger_context and save the pointer.
	 *
	 * key_def is needed in source_less(), where merger_state
	 * is known, but merger_context is not.
	 */
	struct key_def *key_def;
	/* Parsed sources. */
	uint32_t sources_count;
	struct merger_source **sources;
	/* Ascending (false) / descending (true) order. */
	bool reverse;
	/* Optional output buffer. */
	struct ibuf *obuf;
	/* Optional fetch_source() callback. */
	int fetch_source_ref;
};

/* }}} */

/* {{{ Helpers for source methods and merger functions */

/**
 * How much more memory the heap will reserve at the next grow.
 *
 * See HEAP(reserve)() function in lib/salad/heap.h.
 */
size_t heap_next_grow_size(const heap_t *heap)
{
	heap_off_t heap_capacity_diff =	heap->capacity == 0 ?
		HEAP_INITIAL_CAPACITY : heap->capacity;
	return heap_capacity_diff * sizeof(struct heap_node *);
}

/**
 * Extract an ibuf object from the Lua stack.
 */
static struct ibuf *
check_ibuf(struct lua_State *L, int idx)
{
	if (lua_type(L, idx) != LUA_TCDATA)
		return NULL;

	uint32_t cdata_type;
	struct ibuf *ibuf_ptr = luaL_checkcdata(L, idx, &cdata_type);
	if (ibuf_ptr == NULL || cdata_type != ibuf_type_id)
		return NULL;
	return ibuf_ptr;
}

/**
 * Extract a merger context from the Lua stack.
 */
static struct merger_context *
check_merger_context(struct lua_State *L, int idx)
{
	uint32_t cdata_type;
	struct merger_context **ctx_ptr = luaL_checkcdata(L, idx, &cdata_type);
	if (ctx_ptr == NULL || cdata_type != merger_context_type_id)
		return NULL;
	return *ctx_ptr;
}

/**
 * Extract a merger state from the Lua stack.
 */
static struct merger_state *
check_merger_state(struct lua_State *L, int idx)
{
	uint32_t cdata_type;
	struct merger_state **state_ptr = luaL_checkcdata(L, idx, &cdata_type);
	if (state_ptr == NULL || cdata_type != merger_state_type_id)
		return NULL;
	return *state_ptr;
}

/**
 * Skip the array around tuples and save its length.
 */
static int
decode_header(struct ibuf *buf, size_t *len_p)
{
	/* Check the buffer is correct. */
	if (buf->rpos > buf->wpos)
		return -1;

	/* Skip decoding if the buffer is empty. */
	if (ibuf_used(buf) == 0) {
		*len_p = 0;
		return 0;
	}

	/* Check and skip the array around tuples. */
	int ok = mp_typeof(*buf->rpos) == MP_ARRAY;
	if (ok)
		ok = mp_check_array(buf->rpos, buf->wpos) <= 0;
	if (ok)
		*len_p = mp_decode_array((const char **) &buf->rpos);
	return ok ? 0 : -1;
}

/**
 * Encode the array around tuples.
 */
static void
encode_header(struct ibuf *obuf, uint32_t result_len)
{
	ibuf_reserve(obuf, mp_sizeof_array(result_len));
	obuf->wpos = mp_encode_array(obuf->wpos, result_len);
}

/**
 * Push certain fields of a source to Lua.
 */
static void
luaL_merger_source_push(const struct merger_source *source, struct lua_State *L)
{
	lua_createtable(L, 0, 1);

	lua_pushinteger(L, source->idx + 1);
	lua_setfield(L, -2, "idx");
}

/* }}} */

/* {{{ Common merger source functions */

/**
 * Increment a merger source reference counter.
 */
static void
merger_source_ref(struct merger_source *source)
{
	if (unlikely(source->refs >= MERGER_SOURCE_REF_MAX))
		panic("Merger source reference counter overflow");
	++source->refs;
}

/**
 * Decrement a merger source reference counter. When it has
 * reached zero, free the source.
 */
static void
merger_source_unref(struct merger_source *source, struct lua_State *L)
{
	assert(source->refs - 1 >= 0);
	if (--source->refs == 0)
		source->vtab->delete(source, L);
}

/* }}} */

/* {{{ Buffer merger source */

struct merger_source_buffer {
	struct merger_source base;
	struct ibuf *buf;
	/*
	 * A merger stops before end of a buffer
	 * when it is not the last merger in the
	 * chain.
	 */
	size_t remaining_tuples_cnt;
};

/* Virtual methods declarations */

static void
luaL_merger_source_buffer_delete(struct merger_source *base,
				 struct lua_State *L);
static int
luaL_merger_source_buffer_next(struct merger_source *base,
			       box_tuple_format_t *format,
			       const struct merger_state *state,
			       struct lua_State *L);

/* Non-virtual methods */

/**
 * Create the new merger source of buffer type using content of a
 * Lua stack.
 *
 * In case of an error it returns NULL and sets a diag.
 */
static struct merger_source *
luaL_merger_source_buffer_new(struct lua_State *L, int idx, int ordinal,
			      struct merger_state *state)
{
	static struct merger_source_vtab merger_source_buffer_vtab = {
		.delete = luaL_merger_source_buffer_delete,
		.next = luaL_merger_source_buffer_next,
	};

	struct merger_source_buffer *source = (struct merger_source_buffer *)
		malloc(sizeof(struct merger_source_buffer));

	if (source == NULL) {
		diag_set(OutOfMemory, sizeof(struct merger_source_buffer),
			 "malloc", "merger_source_buffer");
		return NULL;
	}

	source->base.idx = ordinal;
	source->base.processed = 0;
	source->base.tuple = NULL;
	/* source->base.hnode does not need to be initialized. */
	source->base.refs = 0;

	source->buf = check_ibuf(L, idx);
	assert(source->buf != NULL);
	source->remaining_tuples_cnt = 0;

	/*
	 * We decode a buffer header once at start when no fetch
	 * callback is provided. In case when there is the
	 * callback we should call it first: it is performed in
	 * the source->base.vtab->next() function.
	 *
	 * The reason is that a user can want to skip some data
	 * (say, a request metainformation) before proceed with
	 * merge.
	 */
	if (state->fetch_source_ref <= 0) {
		if (decode_header(source->buf,
		    &source->remaining_tuples_cnt) != 0) {
			free(source);
			diag_set(IllegalParams, "Invalid merge source %d",
				 ordinal + 1);
			return NULL;
		}
	}

	source->base.vtab = &merger_source_buffer_vtab;
	return &source->base;
}

/**
 * Call a user provided function to fill the source and, maybe,
 * to skip data before tuples array.
 *
 * Return 0 at success and -1 at error and set a diag.
 */
static int
luaL_merger_source_buffer_fetch(struct merger_source_buffer *source,
				const struct merger_state *state,
				struct tuple *last_tuple, struct lua_State *L)
{
	/* No fetch callback: do nothing. */
	if (state->fetch_source_ref <= 0)
		return 0;
	/* Push fetch callback. */
	lua_rawgeti(L, LUA_REGISTRYINDEX, state->fetch_source_ref);
	/* Push source, last_tuple, processed. */
	luaL_merger_source_push(&source->base, L);
	if (last_tuple == NULL)
		lua_pushnil(L);
	else
		luaT_pushtuple(L, last_tuple);
	lua_pushinteger(L, source->base.processed);
	/* Invoke the callback and process data. */
	if (luaT_call(L, 3, 1) != 0)
		return -1;
	/* No more data: do nothing. */
	if (lua_isnil(L, -1)) {
		lua_pop(L, 1);
		return 0;
	}
	source->buf = check_ibuf(L, -1);
	assert(source->buf != NULL);
	lua_pop(L, 1);
	/* Update remaining_tuples_cnt and skip the header. */
	if (decode_header(source->buf, &source->remaining_tuples_cnt) != 0) {
		diag_set(IllegalParams, "Invalid merge source %d",
			 source->base.idx + 1);
		return -1;
	}
	return 0;
}

/* Virtual methods */

static void
luaL_merger_source_buffer_delete(struct merger_source *base,
				 MAYBE_UNUSED struct lua_State *L)
{
	struct merger_source_buffer *source = container_of(base,
		struct merger_source_buffer, base);

	if (base->tuple != NULL)
		box_tuple_unref(base->tuple);

	free(source);
}

static int
luaL_merger_source_buffer_next(struct merger_source *base,
			       box_tuple_format_t *format,
			       const struct merger_state *state,
			       struct lua_State *L)
{
	struct merger_source_buffer *source = container_of(base,
		struct merger_source_buffer, base);

	struct tuple *last_tuple = base->tuple;
	base->tuple = NULL;

	/*
	 * Handle the case when all data were processed:
	 * ask more and stop if no data arrived.
	 */
	if (source->remaining_tuples_cnt == 0) {
		int rc = luaL_merger_source_buffer_fetch(source, state,
							 last_tuple, L);
		if (rc != 0)
			return -1;
		if (source->remaining_tuples_cnt == 0)
			return 0;
	}
	if (ibuf_used(source->buf) == 0) {
		diag_set(IllegalParams, "Unexpected msgpack buffer end");
		return -1;
	}
	const char *tuple_beg = source->buf->rpos;
	const char *tuple_end = tuple_beg;
	/*
	 * mp_next() is faster then mp_check(), but can
	 * read bytes outside of the buffer and so can
	 * cause segmentation faults or incorrect result.
	 *
	 * We check buffer boundaries after the mp_next()
	 * call and throw an error when the boundaries are
	 * violated, but it does not save us from possible
	 * segmentation faults.
	 *
	 * It is in a user responsibility to provide valid
	 * msgpack.
	 */
	mp_next(&tuple_end);
	--source->remaining_tuples_cnt;
	if (tuple_end > source->buf->wpos) {
		diag_set(IllegalParams, "Unexpected msgpack buffer end");
		return -1;
	}
	++base->processed;
	source->buf->rpos = (char *) tuple_end;
	base->tuple = box_tuple_new(format, tuple_beg, tuple_end);
	if (base->tuple == NULL)
		return -1;

	box_tuple_ref(base->tuple);
	return 0;
}

/* }}} */

/* {{{ Table merger source */

struct merger_source_table {
	struct merger_source base;
	int ref;
	int next_idx;
};

/* Virtual methods declarations */

static void
luaL_merger_source_table_delete(struct merger_source *base,
				struct lua_State *L);
static int
luaL_merger_source_table_next(struct merger_source *base,
			      box_tuple_format_t *format,
			      const struct merger_state *state,
			      struct lua_State *L);

/* Non-virtual methods */

/**
 * Create the new merger source of table type using content of a
 * Lua stack.
 *
 * In case of an error it returns NULL and set a diag.
 */
static struct merger_source *
luaL_merger_source_table_new(struct lua_State *L, int idx, int ordinal,
			     struct merger_state *state)
{
	(void) state;

	static struct merger_source_vtab merger_source_table_vtab = {
		.delete = luaL_merger_source_table_delete,
		.next = luaL_merger_source_table_next,
	};

	struct merger_source_table *source = (struct merger_source_table *)
		malloc(sizeof(struct merger_source_table));

	if (source == NULL) {
		diag_set(OutOfMemory, sizeof(struct merger_source_table),
			 "malloc", "merger_source_table");
		return NULL;
	}

	source->base.idx = ordinal;
	source->base.processed = 0;
	source->base.tuple = NULL;
	/* source->base.hnode does not need to be initialized. */
	source->base.refs = 0;

	lua_pushvalue(L, idx); /* Popped by luaL_ref(). */
	source->ref = luaL_ref(L, LUA_REGISTRYINDEX);
	source->next_idx = 1;

	source->base.vtab = &merger_source_table_vtab;
	return &source->base;
}

/**
 * Call a user provided function to fill the source.
 *
 * Return 0 at success and -1 at error (set a diag).
 */
static int
luaL_merger_source_table_fetch(struct merger_source_table *source,
			       const struct merger_state *state,
			       struct tuple *last_tuple, struct lua_State *L)
{
	/* No fetch callback: do nothing. */
	if (state->fetch_source_ref <= 0)
		return 0;
	/* Push fetch callback. */
	lua_rawgeti(L, LUA_REGISTRYINDEX, state->fetch_source_ref);
	/* Push source, last_tuple, processed. */
	luaL_merger_source_push(&source->base, L);
	if (last_tuple == NULL)
		lua_pushnil(L);
	else
		luaT_pushtuple(L, last_tuple);
	lua_pushinteger(L, source->base.processed);
	/* Invoke the callback and process data. */
	if (luaT_call(L, 3, 1) != 0)
		return -1;
	/* No more data: do nothing. */
	if (lua_isnil(L, -1)) {
		lua_pop(L, 1);
		return 0;
	}
	/* Set the new table as the source. */
	luaL_unref(L, LUA_REGISTRYINDEX, source->ref);
	source->ref = luaL_ref(L, LUA_REGISTRYINDEX);
	source->next_idx = 1;
	return 0;
}

/* Virtual methods */

static void
luaL_merger_source_table_delete(struct merger_source *base,
				struct lua_State *L)
{
	struct merger_source_table *source = container_of(base,
		struct merger_source_table, base);

	luaL_unref(L, LUA_REGISTRYINDEX, source->ref);

	if (base->tuple != NULL)
		box_tuple_unref(base->tuple);

	free(source);
}

static int
luaL_merger_source_table_next(struct merger_source *base,
			      box_tuple_format_t *format,
			      const struct merger_state *state,
			      struct lua_State *L)
{
	struct merger_source_table *source = container_of(base,
		struct merger_source_table, base);

	struct tuple *last_tuple = base->tuple;
	base->tuple = NULL;

	lua_rawgeti(L, LUA_REGISTRYINDEX, source->ref);
	lua_pushinteger(L, source->next_idx);
	lua_gettable(L, -2);
	/*
	 * If all data were processed, try to fetch more.
	 */
	if (lua_isnil(L, -1)) {
		lua_pop(L, 2);
		int rc = luaL_merger_source_table_fetch(source, state,
							last_tuple, L);
		if (rc != 0)
			return -1;
		/*
		 * Retry tuple extracting after fetching
		 * of the source.
		 */
		lua_rawgeti(L, LUA_REGISTRYINDEX, source->ref);
		lua_pushinteger(L, source->next_idx);
		lua_gettable(L, -2);
		if (lua_isnil(L, -1)) {
			lua_pop(L, 2);
			return 0;
		}
	}
	base->tuple = luaT_tuple_new(L, -1, format);
	if (base->tuple == NULL)
		return -1;
	++source->next_idx;
	++base->processed;
	lua_pop(L, 2);

	box_tuple_ref(base->tuple);
	return 0;
}

/* }}} */

/* {{{ Iterator merger source */

struct merger_source_iterator {
	struct merger_source base;
	struct luaL_iterator *it;
};

/* Virtual methods declarations */

static void
luaL_merger_source_iterator_delete(struct merger_source *base,
				   struct lua_State *L);
static int
luaL_merger_source_iterator_next(struct merger_source *base,
				 box_tuple_format_t *format,
				 const struct merger_state *state,
				 struct lua_State *L);

/* Non-virtual methods */

/**
 * Create the new merger source of iterator type using content of
 * a Lua stack.
 *
 * In case of an error it returns NULL and set a diag.
 */
static struct merger_source *
luaL_merger_source_iterator_new(struct lua_State *L, int idx, int ordinal,
				struct merger_state *state)
{
	(void) state;

	static struct merger_source_vtab merger_source_iterator_vtab = {
		.delete = luaL_merger_source_iterator_delete,
		.next = luaL_merger_source_iterator_next,
	};

	struct merger_source_iterator *source =
		(struct merger_source_iterator *) malloc(
		sizeof(struct merger_source_iterator));

	if (source == NULL) {
		diag_set(OutOfMemory, sizeof(struct merger_source_iterator),
			 "malloc", "merger_source_iterator");
		return NULL;
	}

	source->base.idx = ordinal;
	source->base.processed = 0;
	source->base.tuple = NULL;
	/* source->base.hnode does not need to be initialized. */
	source->base.refs = 0;

	source->it = luaL_iterator_new(L, idx);
	if (source->it == NULL) {
		free(source);
		return NULL;
	}

	source->base.vtab = &merger_source_iterator_vtab;
	return &source->base;
}

/**
 * Call a user provided function to fill the source.
 *
 * Return 0 at success and -1 at error (set a diag).
 */
static int
luaL_merger_source_iterator_fetch(struct merger_source_iterator *source,
				  const struct merger_state *state,
				  struct tuple *last_tuple, struct lua_State *L)
{
	/* No fetch callback: do nothing. */
	if (state->fetch_source_ref <= 0)
		return 0;
	/* Push fetch callback. */
	lua_rawgeti(L, LUA_REGISTRYINDEX, state->fetch_source_ref);
	/* Push source, last_tuple, processed. */
	luaL_merger_source_push(&source->base, L);
	if (last_tuple == NULL)
		lua_pushnil(L);
	else
		luaT_pushtuple(L, last_tuple);
	lua_pushinteger(L, source->base.processed);
	/* Invoke the callback and process data. */
	if (luaT_call(L, 3, 3) != 0)
		return -1;
	/* No more data: do nothing. */
	if (lua_isnil(L, -3)) {
		lua_pop(L, 3);
		return 0;
	}
	/* Set the new iterator as the source. */
	luaL_iterator_delete(L, source->it);
	source->it = luaL_iterator_new(L, 0);
	lua_pop(L, 3);
	if (source->it == NULL)
		return -1;
	return 0;
}

/* Virtual methods */

static void
luaL_merger_source_iterator_delete(struct merger_source *base,
				   struct lua_State *L)
{
	struct merger_source_iterator *source = container_of(base,
		struct merger_source_iterator, base);

	luaL_iterator_delete(L, source->it);

	if (base->tuple != NULL)
		box_tuple_unref(base->tuple);

	free(source);
}

static int
luaL_merger_source_iterator_next(struct merger_source *base,
				 box_tuple_format_t *format,
				 const struct merger_state *state,
				 struct lua_State *L)
{
	(void) state;

	struct merger_source_iterator *source = container_of(base,
		struct merger_source_iterator, base);

	struct tuple *last_tuple = base->tuple;
	base->tuple = NULL;

	int nresult = luaL_iterator_next(L, source->it);
	/*
	 * If all data were processed, try to fetch more.
	 */
	if (nresult == 0) {
		int rc = luaL_merger_source_iterator_fetch(source, state,
							   last_tuple, L);
		if (rc != 0)
			return -1;
		/*
		 * Retry tuple extracting after fetching
		 * of the source.
		 */
		nresult = luaL_iterator_next(L, source->it);
		if (nresult == 0)
			return 0;
	}

	base->tuple = luaT_tuple_new(L, -nresult + 1, format);
	if (base->tuple == NULL)
		return -1;
	++base->processed;
	lua_pop(L, nresult);

	box_tuple_ref(base->tuple);
	return 0;
}

/* }}} */

/* {{{ Create a source using Lua stack */

/**
 * Create the new merger source using content of a Lua stack.
 *
 * In case of an error it returns NULL and pushes the error to the
 * Lua stack.
 */
struct merger_source *
merger_source_new(struct lua_State *L, int idx, int ordinal,
		  struct merger_context *ctx, struct merger_state *state)
{
	struct merger_source *base = NULL;

	/* Determine type of a merger source on the Lua stack. */
	if (lua_type(L, idx) == LUA_TCDATA) {
		struct ibuf *buf = check_ibuf(L, idx);
		if (buf == NULL)
			goto err;
		/* Create the new buffer source. */
		base = luaL_merger_source_buffer_new(L, idx, ordinal, state);
	} else if (lua_istable(L, idx)) {
		lua_rawgeti(L, idx, 1);
		int iscallable = luaL_iscallable(L, idx);
		lua_pop(L, 1);
		if (iscallable) {
			/* Create the new iterator source. */
			base = luaL_merger_source_iterator_new(L, idx, ordinal,
							       state);
		} else {
			/* Create the new table source. */
			base = luaL_merger_source_table_new(L, idx, ordinal,
							    state);
		}
	} else {
		goto err;
	}

	if (base == NULL) {
		luaT_pusherror(L, diag_last_error(diag_get()));
		return NULL;
	}

	/* Acquire the next tuple. */
	int rc = base->vtab->next(base, ctx->format, state, L);
	if (rc) {
		base->vtab->delete(base, L);
		luaT_pusherror(L, diag_last_error(diag_get()));
		return NULL;
	}

	/* Update the heap. */
	if (base->tuple != NULL) {
		rc = merger_heap_insert(&state->heap, &base->hnode);
		if (rc) {
			base->vtab->delete(base, L);
			diag_set(OutOfMemory, heap_next_grow_size(&state->heap),
				 "malloc", "merger heap");
			luaT_pusherror(L, diag_last_error(diag_get()));
			return NULL;
		}
	}

	return base;

err:
	lua_pushfstring(L, "Unknown source type at index %d", ordinal + 1);
	return NULL;
}

/* }}} */

/* {{{ merger_context functions */

/**
 * Free the merger context from a Lua code.
 */
static int
lbox_merger_context_gc(struct lua_State *L)
{
	struct merger_context *ctx;
	if ((ctx = check_merger_context(L, 1)) == NULL)
		return 0;
	box_key_def_delete(ctx->key_def);
	box_tuple_format_unref(ctx->format);
	free(ctx);
	return 0;
}

/**
 * Create the new merger context.
 *
 * Expected a table of key parts on the Lua stack.
 *
 * Returns the new instance.
 */
static int
lbox_merger_context_new(struct lua_State *L)
{
	struct key_def *key_def;
	if (lua_gettop(L) != 1 || (key_def = check_key_def(L, 1)) == NULL)
		return luaL_error(L, "Usage: merger.context.new(key_def)");

	struct merger_context *ctx = (struct merger_context *) malloc(
		sizeof(struct merger_context));
	if (ctx == NULL) {
		diag_set(OutOfMemory, sizeof(struct merger_context), "malloc",
			 "merger_context");
		return luaT_error(L);
	}

	/*
	 * We need to copy key_def, because it can be collected
	 * by LuaJIT GC before merger_context end of life.
	 */
	ctx->key_def = key_def_dup(key_def);
	if (ctx->key_def == NULL) {
		free(ctx);
		return luaT_error(L);
	}

	ctx->format = box_tuple_format_new(&ctx->key_def, 1);
	if (ctx->format == NULL) {
		box_key_def_delete(ctx->key_def);
		free(ctx);
		return luaL_error(L, "Cannot create format");
	}

	*(struct merger_context **) luaL_pushcdata(L, merger_context_type_id) =
		ctx;

	lua_pushcfunction(L, lbox_merger_context_gc);
	luaL_setcdatagc(L, -2);

	return 1;
}

/* }}} */

/* {{{ merger_state functions */

/**
 * Free the merger state.
 *
 * We need to know Lua state here, because sources of table and
 * iterator types are saved as references within the Lua state.
 */
static void
merger_state_delete(struct lua_State *L, struct merger_state *state)
{
	merger_heap_destroy(&state->heap);
	box_key_def_delete(state->key_def);

	for (uint32_t i = 0; i < state->sources_count; ++i) {
		assert(state->sources != NULL);
		assert(state->sources[i] != NULL);
		merger_source_unref(state->sources[i], L);
	}

	if (state->sources != NULL)
		free(state->sources);

	if (state->fetch_source_ref > 0)
		luaL_unref(L, LUA_REGISTRYINDEX, state->fetch_source_ref);

	free(state);
}

/**
 * Free the merger state from a Lua code.
 */
static int
lbox_merger_state_gc(struct lua_State *L)
{
	struct merger_state *state;
	if ((state = check_merger_state(L, 1)) == NULL)
		return 0;
	merger_state_delete(L, state);
	return 0;
}

/**
 * Push 'bad params' / 'bad param X' and the usage info to the Lua
 * stack.
 */
static int
merger_usage(struct lua_State *L, const char *param_name)
{
	static const char *usage = "merger.{ipairs,pairs,select}("
				   "merger_context, "
				   "{source, source, ...}[, {"
				   "reverse = <boolean> or <nil>, "
				   "buffer = <cdata<struct ibuf>> or <nil>, "
				   "fetch_source = <function> or <nil>}])";
	if (param_name == NULL)
		lua_pushfstring(L, "Bad params, use: %s", usage);
	else
		lua_pushfstring(L, "Bad param \"%s\", use: %s", param_name,
				usage);
	return 1;
}

/**
 * Parse optional third parameter of merger.pairs() and
 * merger.select() into the merger_state structure.
 *
 * Returns 0 on success. In case of an error it pushes an error
 * message to the Lua stack and returns 1.
 *
 * It is the helper for merger_state_new().
 */
static int
parse_opts(struct lua_State *L, int idx, struct merger_state *state)
{
	/* No opts: use defaults. */
	if (lua_isnoneornil(L, idx))
		return 0;

	/* Not a table: error. */
	if (!lua_istable(L, idx))
		return merger_usage(L, NULL);

	/* Parse reverse. */
	lua_pushstring(L, "reverse");
	lua_gettable(L, idx);
	if (!lua_isnil(L, -1)) {
		if (lua_isboolean(L, -1))
			state->reverse = lua_toboolean(L, -1);
		else
			return merger_usage(L, "reverse");
	}
	lua_pop(L, 1);

	/* Parse buffer. */
	lua_pushstring(L, "buffer");
	lua_gettable(L, idx);
	if (!lua_isnil(L, -1)) {
		if ((state->obuf = check_ibuf(L, -1)) == NULL)
			return merger_usage(L, "buffer");
	}
	lua_pop(L, 1);

	/* Parse fetch_source. */
	lua_pushstring(L, "fetch_source");
	lua_gettable(L, idx);
	if (!lua_isnil(L, -1)) {
		if (!luaL_iscallable(L, -1))
			return merger_usage(L, "fetch_source");
		lua_pushvalue(L, -1); /* Popped by luaL_ref(). */
		state->fetch_source_ref = luaL_ref(L, LUA_REGISTRYINDEX);
	}
	lua_pop(L, 1);

	return 0;
}

/**
 * Parse sources table: second parameter of merger.pairs()
 * and merger.select() into the merger_state structure.
 *
 * Note: This function should be called when options are already
 * parsed (using parse_opts()).
 *
 * Returns 0 on success. In case of an error it pushes an error
 * message to the Lua stack and returns 1.
 *
 * It is the helper for merger_state_new().
 */
static int
parse_sources(struct lua_State *L, int idx, struct merger_context *ctx,
	      struct merger_state *state)
{
	/* Allocate sources array. */
	uint32_t capacity = lua_objlen(L, idx);
	const ssize_t sources_size = capacity * sizeof(struct merger_source *);
	state->sources = (struct merger_source **) malloc(sources_size);
	if (state->sources == NULL) {
		diag_set(OutOfMemory, sources_size, "malloc", "state->sources");
		luaT_pusherror(L, diag_last_error(diag_get()));
		return 1;
	}

	/* Fetch all sources. */
	while (true) {
		lua_pushinteger(L, state->sources_count + 1);
		lua_gettable(L, idx);
		if (lua_isnil(L, -1))
			break;

		/* Create the new source. */
		struct merger_source *source = merger_source_new(L, -1,
			state->sources_count, ctx, state);
		if (source == NULL)
			return 1;
		state->sources[state->sources_count] = source;
		++state->sources_count;
		merger_source_ref(source);
	}
	lua_pop(L, state->sources_count + 1);

	return 0;
}

/**
 * Parse sources and options on Lua stack and create the new
 * merger_state instance.
 *
 * It is common code for parsing parameters for
 * lbox_merger_ipairs() and lbox_merger_select().
 */
static struct merger_state *
merger_state_new(struct lua_State *L)
{
	struct merger_context *ctx;
	int ok = (lua_gettop(L) == 2 || lua_gettop(L) == 3) &&
		/* Merger context. */
		(ctx = check_merger_context(L, 1)) != NULL &&
		/* Sources. */
		lua_istable(L, 2) == 1 &&
		/* Opts. */
		(lua_isnoneornil(L, 3) == 1 || lua_istable(L, 3) == 1);
	if (!ok) {
		merger_usage(L, NULL);
		lua_error(L);
		unreachable();
		return NULL;
	}

	struct merger_state *state = (struct merger_state *)
		malloc(sizeof(struct merger_state));
	merger_heap_create(&state->heap);
	state->key_def = key_def_dup(ctx->key_def);
	state->sources_count = 0;
	state->sources = NULL;
	state->reverse = false;
	state->obuf = NULL;
	state->fetch_source_ref = 0;

	if (parse_opts(L, 3, state) != 0 ||
	    parse_sources(L, 2, ctx, state) != 0) {
		merger_state_delete(L, state);
		lua_error(L);
		unreachable();
		return NULL;
	}

	return state;
}

/* }}} */

/* {{{ merger module logic */

/**
 * Data comparing function to construct heap of sources.
 */
static bool
source_less(const heap_t *heap, const struct heap_node *a,
	    const struct heap_node *b)
{
	struct merger_source *left = container_of(a, struct merger_source,
						  hnode);
	struct merger_source *right = container_of(b, struct merger_source,
						   hnode);
	if (left->tuple == NULL && right->tuple == NULL)
		return false;
	if (left->tuple == NULL)
		return false;
	if (right->tuple == NULL)
		return true;
	struct merger_state *state = container_of(heap, struct merger_state,
						  heap);
	int cmp = box_tuple_compare(left->tuple, right->tuple, state->key_def);
	return state->reverse ? cmp >= 0 : cmp < 0;
}

/**
 * Get a tuple from a top source, update the source, update the
 * heap.
 *
 * The reference counter of the tuple is increased (in
 * source->vtab->next()).
 *
 * Return 1 and set @a out (output tuple) to NULL when all sources
 * are drained.
 *
 * Return -1 at an error and set a diag.
 *
 * Otherwise return 0 and store the next tuple in @a out.
 */
static int
merger_next(struct lua_State *L, struct merger_context *ctx,
	    struct merger_state *state, struct tuple **out)
{
	struct heap_node *hnode = merger_heap_top(&state->heap);
	if (hnode == NULL) {
		*out = NULL;
		return 1;
	}

	struct merger_source *source = container_of(hnode, struct merger_source,
						    hnode);
	struct tuple *tuple = source->tuple;
	assert(tuple != NULL);
	int rc = source->vtab->next(source, ctx->format, state, L);
	if (rc != 0)
		return -1;
	if (source->tuple == NULL)
		merger_heap_delete(&state->heap, hnode);
	else
		merger_heap_update(&state->heap, hnode);

	*out = tuple;
	return 0;
}

/**
 * Iterator gen function to traverse merger results.
 *
 * Expected a merger context as the first parameter (state) and a
 * merger_state as the second parameter (param) on the Lua
 * stack.
 *
 * Push the merger_state (the new param) and the next tuple.
 */
static int
lbox_merger_gen(struct lua_State *L)
{
	struct merger_context *ctx;
	struct merger_state *state;
	bool ok = (ctx = check_merger_context(L, -2)) != NULL &&
		(state = check_merger_state(L, -1)) != NULL;
	if (!ok)
		return luaL_error(L, "Bad params, use: "
				     "lbox_merger_gen(merger_context, "
				     "merger_state)");

	struct tuple *tuple;
	if (merger_next(L, ctx, state, &tuple) < 0)
		return luaT_error(L);
	if (tuple == NULL) {
		lua_pushnil(L);
		lua_pushnil(L);
		return 2;
	}

	/* Push merger_state, tuple. */
	*(struct merger_state **)
		luaL_pushcdata(L, merger_state_type_id) = state;
	luaT_pushtuple(L, tuple);

	box_tuple_unref(tuple);
	return 2;
}

/**
 * Iterate over merge results from Lua.
 *
 * Push three values to the Lua stack:
 *
 * 1. gen (lbox_merger_gen wrapped by fun.wrap());
 * 2. param (merger_context);
 * 3. state (merger_state).
 */
static int
lbox_merger_ipairs(struct lua_State *L)
{
	/* Create merger_state. */
	struct merger_state *state = merger_state_new(L);
	lua_settop(L, 1); /* Pop sources, [opts]. */
	/* Stack: merger_context. */

	if (state->obuf != NULL) {
		merger_state_delete(L, state);
		return luaL_error(L, "\"buffer\" option is forbidden with "
				  "merger.pairs(<...>)");
	}

	luaL_loadstring(L, "return require('fun').wrap");
	lua_call(L, 0, 1);
	lua_insert(L, -2); /* Swap merger_context and wrap. */
	/* Stack: wrap, merger_context. */

	lua_pushcfunction(L, lbox_merger_gen);
	lua_insert(L, -2); /* Swap merger_context and gen. */
	/* Stack: wrap, gen, merger_context. */

	*(struct merger_state **)
		luaL_pushcdata(L, merger_state_type_id) = state;
	lua_pushcfunction(L, lbox_merger_state_gc);
	luaL_setcdatagc(L, -2);
	/* Stack: wrap, gen, merger_context, merger_state. */

	/* Call fun.wrap(gen, merger_context, merger_state). */
	lua_call(L, 3, 3);
	return 3;
}

/**
 * Write merge results into ibuf.
 *
 * It is the helper for lbox_merger_select().
 */
static int
encode_result_buffer(struct lua_State *L, struct merger_context *ctx,
		     struct merger_state *state)
{
	struct ibuf *obuf = state->obuf;
	uint32_t result_len = 0;
	uint32_t result_len_offset = 4;

	/*
	 * Reserve maximum size for the array around resulting
	 * tuples to set it later.
	 */
	encode_header(state->obuf, UINT32_MAX);

	/* Fetch, merge and copy tuples to the buffer. */
	struct tuple *tuple;
	int rc;
	while ((rc = merger_next(L, ctx, state, &tuple)) == 0) {
		assert(tuple != NULL);
		uint32_t bsize = tuple->bsize;
		ibuf_reserve(obuf, bsize);
		memcpy(obuf->wpos, tuple_data(tuple), bsize);
		obuf->wpos += bsize;
		result_len_offset += bsize;
		box_tuple_unref(tuple);
		++result_len;
	}

	if (rc < 0)
		return luaT_error(L);

	/* Write the real array size. */
	mp_store_u32(obuf->wpos - result_len_offset, result_len);

	return 1;
}

/**
 * Write merge results into the new Lua table.
 *
 * It is the helper for lbox_merger_select().
 */
static int
create_result_table(struct lua_State *L, struct merger_context *ctx,
		    struct merger_state *state)
{
	/* Create result table. */
	lua_newtable(L);

	uint32_t cur = 1;

	/* Fetch, merge and save tuples to the table. */
	struct tuple *tuple;
	int rc;
	while ((rc = merger_next(L, ctx, state, &tuple)) == 0) {
		assert(tuple != NULL);
		luaT_pushtuple(L, tuple);
		lua_rawseti(L, -2, cur);
		box_tuple_unref(tuple);
		++cur;
	}

	if (rc < 0)
		return luaT_error(L);

	return 1;
}

/**
 * Perform the merge.
 *
 * Write results into a buffer or a Lua table depending on
 * options.
 *
 * Expected a merger context, sources table and options (optional)
 * on the Lua stack.
 *
 * Return the Lua table or nothing when the 'buffer' option is
 * provided.
 */
static int
lbox_merger_select(struct lua_State *L)
{
	struct merger_context *ctx = check_merger_context(L, 1);
	if (ctx == NULL) {
		merger_usage(L, NULL);
		lua_error(L);
	}

	struct merger_state *state = merger_state_new(L);
	lua_settop(L, 0); /* Pop merger_context, sources, [opts]. */

	if (state->obuf == NULL) {
		create_result_table(L, ctx, state); // XXX: can raise, will leak
						    // state
		merger_state_delete(L, state);
		return 1;
	} else {
		encode_result_buffer(L, ctx, state); // XXX: can raise, will
						     // leak state
		merger_state_delete(L, state);
		return 0;
	}
}

/**
 * Register the module.
 */
LUA_API int
luaopen_merger(lua_State *L)
{
	luaL_cdef(L, "struct merger_context;");
	luaL_cdef(L, "struct merger_state;");
	luaL_cdef(L, "struct ibuf;");

	merger_context_type_id = luaL_ctypeid(L, "struct merger_context&");
	merger_state_type_id = luaL_ctypeid(L, "struct merger_state&");
	ibuf_type_id = luaL_ctypeid(L, "struct ibuf");

	/* Export C functions to Lua. */
	static const struct luaL_Reg meta[] = {
		{"select", lbox_merger_select},
		{"ipairs", lbox_merger_ipairs},
		{"pairs", lbox_merger_ipairs},
		{NULL, NULL}
	};
	luaL_register_module(L, "merger", meta);

	/* Add context.new(). */
	lua_newtable(L); /* merger.context */
	lua_pushcfunction(L, lbox_merger_context_new);
	lua_setfield(L, -2, "new");
	lua_setfield(L, -2, "context");

	return 1;
}

/* }}} */
