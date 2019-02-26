/*
 * Copyright 2010-2019, Tarantool AUTHORS, please see AUTHORS file.
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
#include <stdbool.h>
#include <stdint.h>
#include "tuple_compare.h"
#include "memtx_tree.h"

/* {{{ Memtx tree of tuples class. ******************************/

/** Struct that is used as a key in BPS tree definition. */
struct memtx_basic_tree_key_data {
	/** Sequence of msgpacked search fields. */
	const char *key;
	/** Number of msgpacked search fields. */
	uint32_t part_count;
};

#define MEMTX_TREE_NAME memtx_basic_tree
#define memtx_tree_elem_t struct tuple *
#define memtx_tree_key_t struct memtx_basic_tree_key_data
#define MEMTX_TREE_COMPARE(elem_a_ptr, elem_b_ptr, key_def)			\
	tuple_compare(*elem_a_ptr, *elem_b_ptr, key_def)
#define MEMTX_TREE_COMPARE_KEY(elem_ptr, key_ptr, key_def)			\
	tuple_compare_with_key(*elem_ptr, (key_ptr)->key,			\
			       (key_ptr)->part_count, key_def)
#define MEMTX_TREE_ELEM_SET(elem_ptr, tuple, key_def)				\
	({(void)key_def; *elem_ptr = tuple;})
#define MEMTX_TREE_KEY_SET(key_ptr, key_val, part_count_val, key_def)		\
	({(void)key_def; (key_ptr)->key = key_val;				\
	 (key_ptr)->part_count = part_count_val;})
#define MEMTX_TREE_ELEM_GET(elem_ptr) (*(elem_ptr))
#define MEMTX_TREE_KEY_GET(key_ptr, part_count_ptr)				\
	({*part_count_ptr = (key_ptr)->part_count; (key_ptr)->key;})
#define MEMTX_TREE_IDENTICAL(elem_a_ptr, elem_b_ptr)				\
	({*elem_a_ptr == *elem_b_ptr;})

#include "memtx_tree_impl.h"

#undef memtx_tree_key_t
#undef memtx_tree_elem_t
#undef MEMTX_TREE_KEY_GET
#undef MEMTX_TREE_ELEM_GET
#undef MEMTX_TREE_KEY_SET
#undef MEMTX_TREE_ELEM_SET
#undef MEMTX_TREE_COMPARE_KEY
#undef MEMTX_TREE_COMPARE
#undef MEMTX_TREE_NAME
#undef MEMTX_TREE_IDENTICAL

/* }}} */

/* {{{ Memtx hinted tree class. *********************************/

/**
 * Struct that is used as a key in BPS tree definition in
 * memtx_hint_only_tree and memtx_hinted_tree.
*/
struct memtx_hinted_tree_key_data {
	/** Sequence of msgpacked search fields. */
	const char *key;
	/** Number of msgpacked search fields. */
	uint32_t part_count;
	/**
	 * Comparison hint. Calculated automatically on 'set'
	 * operation with MEMTX_TREE_KEY_SET().
	 */
	uint64_t hint;
};

/**
 * Struct that is used as a key in BPS tree definition in
 * memtx_hint_only_tree and memtx_hinted_tree.
 */
struct memtx_hinted_tree_data {
	/** Tuple this node is represents. */
	struct tuple *tuple;
	/**
	 * Comparison hint. Calculated automatically on 'set'
	 * operation with MEMTX_TREE_ELEM_SET().
	 */
	uint64_t hint;
};

#define MEMTX_TREE_NAME memtx_hinted_tree
#define memtx_tree_elem_t struct memtx_hinted_tree_data
#define memtx_tree_key_t struct memtx_hinted_tree_key_data
#define MEMTX_TREE_COMPARE(elem_a_ptr, elem_b_ptr, key_def) ({			\
	int rc;									\
	if ((elem_a_ptr)->hint != (elem_b_ptr)->hint) {				\
		rc = (elem_a_ptr)->hint < (elem_b_ptr)->hint ? -1 : 1;		\
	} else {								\
		rc = tuple_compare((elem_a_ptr)->tuple, (elem_b_ptr)->tuple,	\
				   key_def);					\
	}									\
	rc;									\
})
#define MEMTX_TREE_COMPARE_KEY(elem_ptr, key_ptr, key_def) ({			\
	int rc;									\
	if ((elem_ptr)->hint != (key_ptr)->hint) {				\
		rc = (elem_ptr)->hint < (key_ptr)->hint ? -1 : 1;		\
	} else {								\
		rc = tuple_compare_with_key((elem_ptr)->tuple, (key_ptr)->key,	\
					    (key_ptr)->part_count, key_def);	\
	}									\
	rc;									\
})
#define MEMTX_TREE_ELEM_SET(elem_ptr, info, key_def) ({				\
	(elem_ptr)->tuple = info;						\
	(elem_ptr)->hint = tuple_hint(info, key_def);				\
})
#define MEMTX_TREE_KEY_SET(key_ptr, key_val, part_count_val, key_def) ({	\
	(key_ptr)->key = key_val;						\
	(key_ptr)->part_count = part_count_val;					\
	(key_ptr)->hint = part_count_val > 0 ? key_hint(key_val, key_def) : 0;	\
})
#define MEMTX_TREE_ELEM_GET(elem_ptr) ((elem_ptr)->tuple)
#define MEMTX_TREE_KEY_GET(key_ptr, part_count_ptr)				\
	({*part_count_ptr = (key_ptr)->part_count; (key_ptr)->key;})
#define MEMTX_TREE_IDENTICAL(elem_a_ptr, elem_b_ptr)				\
	({(elem_a_ptr)->tuple == (elem_b_ptr)->tuple;})

#include "memtx_tree_impl.h"

#undef memtx_tree_key_t
#undef memtx_tree_elem_t
#undef MEMTX_TREE_KEY_GET
#undef MEMTX_TREE_ELEM_GET
#undef MEMTX_TREE_KEY_SET
#undef MEMTX_TREE_ELEM_SET
#undef MEMTX_TREE_COMPARE_KEY
#undef MEMTX_TREE_COMPARE
#undef MEMTX_TREE_NAME
#undef MEMTX_TREE_IDENTICAL

/* }}} */

/* {{{ Memtx multikey tree class. *******************************/

/**
 * Struct that is used as a key in BPS tree definition in
 * memtx_multikey_tree.
 */
struct memtx_multikey_tree_data {
	/** Tuple this node is represents. */
	struct tuple *tuple;
	/**
	 * Hin for multikey index. Array index placeholder value.
	 */
	uint32_t multikey_idx;
};

static int
memtx_multikey_tree_index_replace(struct index *base, struct tuple *old_tuple,
				  struct tuple *new_tuple,
				  enum dup_replace_mode mode,
				  struct tuple **result);

static int
memtx_multikey_tree_index_build_next(struct index *base, struct tuple *tuple);

#define MEMTX_TREE_NAME memtx_multikey_tree
#define memtx_tree_elem_t struct memtx_multikey_tree_data
#define memtx_tree_key_t struct memtx_basic_tree_key_data
#define MEMTX_TREE_COMPARE(elem_a_ptr, elem_b_ptr, key_def)			\
	tuple_compare_multikey((elem_a_ptr)->tuple, (elem_a_ptr)->multikey_idx,	\
			       (elem_b_ptr)->tuple, (elem_b_ptr)->multikey_idx,	\
			       key_def)
#define MEMTX_TREE_COMPARE_KEY(elem_ptr, key_ptr, key_def)			\
	tuple_compare_with_key_multikey((elem_ptr)->tuple,			\
					(elem_ptr)->multikey_idx,		\
					(key_ptr)->key, (key_ptr)->part_count,	\
					key_def)
#define MEMTX_TREE_KEY_SET(key_ptr, key_val, part_count_val, key_def)		\
	({(void)key_def; (key_ptr)->key = key_val;				\
	 (key_ptr)->part_count = part_count_val;})
#define MEMTX_TREE_ELEM_GET(elem_ptr) ((elem_ptr)->tuple)
#define MEMTX_TREE_KEY_GET(key_ptr, part_count_ptr)				\
	({*part_count_ptr = (key_ptr)->part_count; (key_ptr)->key;})
#define MEMTX_TREE_IDENTICAL(elem_a_ptr, elem_b_ptr) ({				\
	(elem_a_ptr)->tuple == (elem_b_ptr)->tuple &&				\
	(elem_a_ptr)->multikey_idx == (elem_b_ptr)->multikey_idx;})
#define memtx_tree_index_replace memtx_multikey_tree_index_replace
#define memtx_tree_index_build_next memtx_multikey_tree_index_build_next

#include "memtx_tree_impl.h"

#undef memtx_tree_key_t
#undef memtx_tree_elem_t
#undef MEMTX_TREE_KEY_GET
#undef MEMTX_TREE_ELEM_GET
#undef MEMTX_TREE_KEY_SET
#undef MEMTX_TREE_COMPARE_KEY
#undef MEMTX_TREE_COMPARE
#undef MEMTX_TREE_NAME
#undef MEMTX_TREE_IDENTICAL
#undef memtx_tree_index_replace
#undef memtx_tree_index_build_next

static int
memtx_multikey_tree_index_insert_tuple(struct index *base, struct tuple *tuple,
				       struct tuple **replaced,
				       uint32_t multikey_idx)
{
	struct memtx_multikey_tree_index *index =
		(struct memtx_multikey_tree_index *)base;
	struct memtx_multikey_tree_data data;
	data.tuple = tuple;
	data.multikey_idx = multikey_idx;
	struct memtx_multikey_tree_data data_replaced;
	memset(&data_replaced, 0, sizeof(data_replaced));
	int rc = memtx_multikey_tree_tree_insert(&index->tree, data, &data_replaced);
	if (replaced != NULL)
		*replaced = data_replaced.tuple;
	return rc;
}

static void
memtx_multikey_tree_index_delete_tuple(struct index *base, struct tuple *tuple,
				       uint32_t multikey_idx)
{
	struct memtx_multikey_tree_index *index =
		(struct memtx_multikey_tree_index *)base;
	struct memtx_multikey_tree_data data;
	data.tuple = tuple;
	data.multikey_idx = multikey_idx;
	memtx_multikey_tree_tree_delete(&index->tree, data);
}

static int
memtx_multikey_tree_index_get_array_sz(struct tuple *tuple,
				       struct key_def *key_def)
{
	assert(key_def->has_multikey_parts);
	struct json_lexer lexer;
	struct key_part *part = key_def->parts;
	while (!part->is_multikey)
		part++;
	assert(part->path != NULL);
	struct json_token token;
	json_lexer_create(&lexer, part->path, part->path_len, TUPLE_INDEX_BASE);
	int rc;
	while ((rc = json_lexer_next_token(&lexer, &token)) != 0 &&
		token.type != JSON_TOKEN_ANY) {};
	assert(rc == 0 && token.type == JSON_TOKEN_ANY);
	const char *field =
		tuple_field_by_part_multikey(tuple, part,
					     -1 /* To do not dig below array */);
	assert(mp_typeof(*field) == MP_ARRAY);
	return mp_decode_array(&field);
}

static int
memtx_multikey_tree_index_replace(struct index *base, struct tuple *old_tuple,
				  struct tuple *new_tuple,
				  enum dup_replace_mode mode,
				  struct tuple **result)
{
	struct memtx_multikey_tree_index *index =
		(struct memtx_multikey_tree_index *)base;
	struct key_def *key_def = index->tree.arg;
	if (new_tuple) {
		int sz = memtx_multikey_tree_index_get_array_sz(new_tuple,
								key_def);
		struct tuple *dup_tuple = NULL;
		int multikey_idx = 0;
		int tree_res = memtx_multikey_tree_index_insert_tuple(base,
					new_tuple, &dup_tuple, multikey_idx++);
		if (tree_res != 0)
			goto insertion_error;
		int errcode = replace_check_dup(old_tuple, dup_tuple, mode);
		if (errcode != 0) {
			memtx_multikey_tree_index_delete_tuple(base,
					new_tuple, 0);
			if (dup_tuple) {
				memtx_multikey_tree_index_insert_tuple(base,
						dup_tuple, NULL, 0);
			}
			struct space *sp = space_cache_find(base->def->space_id);
			if (sp != NULL)
				diag_set(ClientError, errcode, base->def->name,
					 space_name(sp));
			return -1;
		}

		for (; multikey_idx < sz; multikey_idx++) {
			tree_res = memtx_multikey_tree_index_insert_tuple(base,
					new_tuple, &dup_tuple, multikey_idx++);
			if (tree_res != 0)
				break;
			assert(replace_check_dup(old_tuple, dup_tuple,
						 mode) == 0);
		}
		if (tree_res != 0) {
			--multikey_idx;
			for (; multikey_idx >= 0; multikey_idx--) {
				memtx_multikey_tree_index_delete_tuple(base,
					old_tuple, multikey_idx);
			}
insertion_error:
			diag_set(OutOfMemory, MEMTX_EXTENT_SIZE,
				 "memtx_tree_index", "replace");
			return -1;
		}
		if (dup_tuple) {
			*result = dup_tuple;
			return 0;
		}
	}
	if (old_tuple) {
		int sz = memtx_multikey_tree_index_get_array_sz(old_tuple,
								key_def);
		for (int multikey_idx = 0; multikey_idx < sz; multikey_idx++) {
			memtx_multikey_tree_index_delete_tuple(base, old_tuple,
							       multikey_idx);
		}
	}
	*result = old_tuple;
	return 0;
}

static int
memtx_multikey_tree_index_build_next(struct index *base, struct tuple *tuple)
{
	(void)base;
	(void)tuple;
	struct memtx_multikey_tree_index *index =
		(struct memtx_multikey_tree_index *)base;
	struct key_def *key_def = index->tree.arg;
	if (index->build_array == NULL) {
		index->build_array =
			(struct memtx_multikey_tree_data *)
			malloc(MEMTX_EXTENT_SIZE);
		if (index->build_array == NULL) {
			diag_set(OutOfMemory, MEMTX_EXTENT_SIZE,
				 "memtx_tree_index", "build_next");
			return -1;
		}
		index->build_array_alloc_size =
			MEMTX_EXTENT_SIZE / (sizeof(index->build_array[0]));
	}
	assert(index->build_array_size <= index->build_array_alloc_size);
	if (index->build_array_size == index->build_array_alloc_size) {
		index->build_array_alloc_size = index->build_array_alloc_size +
					index->build_array_alloc_size / 2;
		struct memtx_multikey_tree_data *tmp =
			realloc(index->build_array,
				index->build_array_alloc_size * sizeof(*tmp));
		if (tmp == NULL) {
			diag_set(OutOfMemory, index->build_array_alloc_size *
				 sizeof(*tmp), "memtx_tree_index", "build_next");
			return -1;
		}
		index->build_array = tmp;
	}
	int sz = memtx_multikey_tree_index_get_array_sz(tuple, key_def);
	for (int multikey_idx = 0; multikey_idx < sz; multikey_idx++) {
		struct memtx_multikey_tree_data *elem =
			&index->build_array[index->build_array_size++];
		elem->tuple = tuple;
		elem->multikey_idx = multikey_idx;
	}
	return 0;
}

// /* }}} */

struct index *
memtx_tree_index_new(struct memtx_engine *memtx, struct index_def *def)
{
	if (def->key_def->has_multikey_parts)
		return memtx_multikey_tree_index_new(memtx, def);
	if (def->cmp_def->parts->type == FIELD_TYPE_STRING ||
	    def->cmp_def->parts->type == FIELD_TYPE_UNSIGNED ||
	    def->cmp_def->parts->type == FIELD_TYPE_INTEGER)
		return memtx_hinted_tree_index_new(memtx, def);
	return memtx_basic_tree_index_new(memtx, def);
}
