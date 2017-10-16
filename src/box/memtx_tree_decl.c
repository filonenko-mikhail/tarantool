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
#define MEMTX_TREE_ELEM_CLEAR(elem_ptr) ({*elem_ptr = NULL;})
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
#undef MEMTX_TREE_ELEM_CLEAR
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
#define MEMTX_TREE_ELEM_CLEAR(elem_ptr) ({					\
	(elem_ptr)->tuple = NULL; (elem_ptr)->hint = 0;})
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
#undef MEMTX_TREE_ELEM_CLEAR
#undef MEMTX_TREE_COMPARE_KEY
#undef MEMTX_TREE_COMPARE
#undef MEMTX_TREE_NAME
#undef MEMTX_TREE_IDENTICAL

/* }}} */

struct index *
memtx_tree_index_new(struct memtx_engine *memtx, struct index_def *def)
{
	if (def->cmp_def->parts->type == FIELD_TYPE_STRING ||
	    def->cmp_def->parts->type == FIELD_TYPE_UNSIGNED ||
	    def->cmp_def->parts->type == FIELD_TYPE_INTEGER)
		return memtx_hinted_tree_index_new(memtx, def);
	return memtx_basic_tree_index_new(memtx, def);
}
