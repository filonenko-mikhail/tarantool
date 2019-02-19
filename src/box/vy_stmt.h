#ifndef INCLUDES_TARANTOOL_BOX_VY_STMT_H
#define INCLUDES_TARANTOOL_BOX_VY_STMT_H
/*
 * Copyright 2010-2016, Tarantool AUTHORS, please see AUTHORS file.
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
 * THIS SOFTWARE IS PROVIDED BY AUTHORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * AUTHORS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include <trivia/util.h>
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <assert.h>
#include <msgpuck.h>
#include <bit/bit.h>

#include "tuple.h"
#include "iproto_constants.h"

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

struct xrow_header;
struct region;
struct tuple_format;
struct tuple_dictionary;
struct iovec;

#define MAX_LSN (INT64_MAX / 2)

enum {
	VY_UPSERT_THRESHOLD = 128,
	VY_UPSERT_INF,
};
static_assert(VY_UPSERT_THRESHOLD <= UINT8_MAX, "n_upserts max value");
static_assert(VY_UPSERT_INF == VY_UPSERT_THRESHOLD + 1,
	      "inf must be threshold + 1");

/** Vinyl statement environment. */
struct vy_stmt_env {
	/** Vinyl statement vtable. */
	struct tuple_format_vtab tuple_format_vtab;
	/**
	 * Max tuple size
	 * @see box.cfg.vinyl_max_tuple_size
	 */
	size_t max_tuple_size;
	/** Tuple format for keys. */
	struct tuple_format *key_format;
};

/** Initialize a vinyl statement environment. */
void
vy_stmt_env_create(struct vy_stmt_env *env);

/** Destroy a vinyl statement environment. */
void
vy_stmt_env_destroy(struct vy_stmt_env *env);

/** Create a vinyl statement format. */
struct tuple_format *
vy_stmt_format_new(struct vy_stmt_env *env, struct key_def *const *keys,
		   uint16_t key_count, const struct field_def *fields,
		   uint32_t field_count, uint32_t exact_field_count,
		   struct tuple_dictionary *dict);

/** Statement flags. */
enum {
	/**
	 * A REPLACE/DELETE request is supposed to delete the old
	 * tuple from all indexes. In order to generate a DELETE
	 * statement for a secondary index, we need to look up the
	 * old tuple in the primary index, which is expensive as
	 * it implies a random disk access. We can optimize out the
	 * lookup by deferring generation of the DELETE statement
	 * until primary index compaction.
	 *
	 * The following flag is set for those REPLACE and DELETE
	 * statements that skipped deletion of the old tuple from
	 * secondary indexes. It makes the write iterator generate
	 * DELETE statements for them during compaction.
	 */
	VY_STMT_DEFERRED_DELETE		= 1 << 0,
	/**
	 * Statements that have this flag set are ignored by the
	 * read iterator.
	 *
	 * We set this flag for deferred DELETE statements, because
	 * they may violate the invariant which the read relies upon:
	 * the older a source, the older statements it stores for a
	 * particular key.
	 */
	VY_STMT_SKIP_READ		= 1 << 1,
	/**
	 * This flag is set for those REPLACE statements that were
	 * generated by UPDATE operations. It is used by the write
	 * iterator to turn such REPLACEs into INSERTs in secondary
	 * indexes so that they can get annihilated with DELETEs on
	 * compaction. It is never written to disk.
	 */
	VY_STMT_UPDATE			= 1 << 2,
	/**
	 * Bit mask of all statement flags.
	 */
	VY_STMT_FLAGS_ALL = (VY_STMT_DEFERRED_DELETE | VY_STMT_SKIP_READ |
			     VY_STMT_UPDATE),
};

/**
 * There are two groups of statements:
 *
 *  - SELECT is "key" statement.
 *  - DELETE, UPSERT and REPLACE are "tuple" statements.
 *
 * REPLACE/UPSERT/DELETE statements structure:
 *                               data_offset
 *                                    ^
 * +----------------------------------+
 * |               4 bytes      4 bytes     MessagePack data.
 * |               +------+----+------+---------------------------+- - - - - - .
 *tuple, ..., raw: | offN | .. | off1 | header ..|key1|..|keyN|.. | operations |
 *                 +--+---+----+--+---+---------------------------+- - - - - - .
 *                 |     ...    |                 ^       ^
 *                 |            +-----------------+       |
 *                 +--------------------------------------+
 * Offsets are stored only for indexed fields, though MessagePack'ed tuple data
 * can contain also not indexed fields. For example, if fields 3 and 5 are
 * indexed then before MessagePack data are stored offsets only for field 3 and
 * field 5.
 *
 * SELECT statements structure.
 * +--------------+-----------------+
 * | array header | part1 ... partN |  -  MessagePack data
 * +--------------+-----------------+
 *
 * Field 'operations' is used for storing operations of UPSERT statement.
 */
struct vy_stmt {
	struct tuple base;
	int64_t lsn;
	uint8_t  type; /* IPROTO_SELECT/REPLACE/UPSERT/DELETE */
	uint8_t flags;
	/**
	 * Offsets array concatenated with MessagePack fields
	 * array.
	 * char raw[0];
	 */
};

/** Get LSN of the vinyl statement. */
static inline int64_t
vy_stmt_lsn(const struct tuple *stmt)
{
	return ((const struct vy_stmt *) stmt)->lsn;
}

/** Set LSN of the vinyl statement. */
static inline void
vy_stmt_set_lsn(struct tuple *stmt, int64_t lsn)
{
	((struct vy_stmt *) stmt)->lsn = lsn;
}

/** Get type of the vinyl statement. */
static inline enum iproto_type
vy_stmt_type(const struct tuple *stmt)
{
	return (enum iproto_type)((const struct vy_stmt *) stmt)->type;
}

/** Set type of the vinyl statement. */
static inline void
vy_stmt_set_type(struct tuple *stmt, enum iproto_type type)
{
	((struct vy_stmt *) stmt)->type = type;
}

/** Get flags of the vinyl statement. */
static inline uint8_t
vy_stmt_flags(const struct tuple *stmt)
{
	return ((const struct vy_stmt *)stmt)->flags;
}

/** Set flags of the vinyl statement. */
static inline void
vy_stmt_set_flags(struct tuple *stmt, uint8_t flags)
{
	((struct vy_stmt *)stmt)->flags = flags;
}

/**
 * Get upserts count of the vinyl statement.
 * Only for UPSERT statements allocated on lsregion.
 */
static inline uint8_t
vy_stmt_n_upserts(const struct tuple *stmt)
{
	assert(stmt->refs == 0);
	assert(vy_stmt_type(stmt) == IPROTO_UPSERT);
	return *((uint8_t *)stmt - 1);
}

/**
 * Set upserts count of the vinyl statement.
 * Only for UPSERT statements allocated on lsregion.
 */
static inline void
vy_stmt_set_n_upserts(struct tuple *stmt, uint8_t n)
{
	assert(stmt->refs == 0);
	assert(vy_stmt_type(stmt) == IPROTO_UPSERT);
	*((uint8_t *)stmt - 1) = n;
}

/**
 * Return the number of key parts defined in the given vinyl
 * statement.
 *
 * If the statement represents a tuple, we assume that it has
 * all key parts defined.
 */
static inline uint32_t
vy_stmt_key_part_count(const struct tuple *stmt, struct key_def *key_def)
{
	if (vy_stmt_type(stmt) == IPROTO_SELECT) {
		uint32_t part_count = tuple_field_count(stmt);
		assert(part_count <= key_def->part_count);
		return part_count;
	}
	return key_def->part_count;
}

/**
 * Return true if the given vinyl statement contains all
 * key parts, i.e. can be used for an exact match lookup.
 */
static inline bool
vy_stmt_is_full_key(const struct tuple *stmt, struct key_def *key_def)
{
	return vy_stmt_key_part_count(stmt, key_def) == key_def->part_count;
}

/**
 * Return true if the given vinyl statement stores an empty
 * (match all) key.
 */
static inline bool
vy_stmt_is_empty_key(const struct tuple *stmt)
{
	return tuple_field_count(stmt) == 0;
}

/**
 * Duplicate the statememnt.
 *
 * @param stmt statement
 * @return new statement of the same type with the same data.
 */
struct tuple *
vy_stmt_dup(const struct tuple *stmt);

struct lsregion;

/**
 * Duplicate the statement, using the lsregion as allocator.
 * @param stmt      Statement to duplicate.
 * @param lsregion  Allocator.
 * @param alloc_id  Allocation identifier for the lsregion.
 *
 * @retval not NULL The new statement with the same data.
 * @retval     NULL Memory error.
 */
struct tuple *
vy_stmt_dup_lsregion(const struct tuple *stmt, struct lsregion *lsregion,
		     int64_t alloc_id);

/**
 * Return true if @a stmt can be referenced. Now to be not refable
 * it must be allocated on lsregion.
 * @param stmt a statement
 * @retval true if @a stmt was allocated on lsregion
 * @retval false otherwise
 */
static inline bool
vy_stmt_is_refable(const struct tuple *stmt)
{
	return stmt->refs > 0;
}

/**
 * Ref tuple, if it exists (!= NULL) and can be referenced.
 * @sa vy_stmt_is_refable.
 *
 * @param tuple Tuple to ref or NULL.
 */
static inline void
vy_stmt_ref_if_possible(struct tuple *stmt)
{
	if (vy_stmt_is_refable(stmt))
		tuple_ref(stmt);
}

/**
 * Unref tuple, if it exists (!= NULL) and can be unreferenced.
 * @sa vy_stmt_is_refable.
 *
 * @param tuple Tuple to unref or NULL.
 */
static inline void
vy_stmt_unref_if_possible(struct tuple *stmt)
{
	if (vy_stmt_is_refable(stmt))
		tuple_unref(stmt);
}

/**
 * Compare two vinyl statements taking into account their
 * formats (key or tuple).
 */
static inline int
vy_stmt_compare(const struct tuple *a, const struct tuple *b,
		struct key_def *key_def)
{
	bool a_is_tuple = vy_stmt_type(a) != IPROTO_SELECT;
	bool b_is_tuple = vy_stmt_type(b) != IPROTO_SELECT;
	if (a_is_tuple && b_is_tuple) {
		return tuple_compare(a, b, key_def);
	} else if (a_is_tuple && !b_is_tuple) {
		const char *key = tuple_data(b);
		uint32_t part_count = mp_decode_array(&key);
		return tuple_compare_with_key(a, key, part_count, key_def);
	} else if (!a_is_tuple && b_is_tuple) {
		const char *key = tuple_data(a);
		uint32_t part_count = mp_decode_array(&key);
		return -tuple_compare_with_key(b, key, part_count, key_def);
	} else {
		assert(!a_is_tuple && !b_is_tuple);
		return key_compare(tuple_data(a), tuple_data(b), key_def);
	}
}

/**
 * Compare a vinyl statement (key or tuple) with a raw key
 * (msgpack array).
 */
static inline int
vy_stmt_compare_with_raw_key(const struct tuple *stmt, const char *key,
			     struct key_def *key_def)
{
	if (vy_stmt_type(stmt) != IPROTO_SELECT) {
		uint32_t part_count = mp_decode_array(&key);
		return tuple_compare_with_key(stmt, key, part_count, key_def);
	}
	return key_compare(tuple_data(stmt), key, key_def);
}

/**
 * Create a key statement from raw MessagePack data.
 * @param format     Format of an index.
 * @param key        MessagePack data that contain an array of
 *                   fields WITHOUT the array header.
 * @param part_count Count of the key fields that will be saved as
 *                   result.
 *
 * @retval NULL     Memory allocation error.
 * @retval not NULL Success.
 */
struct tuple *
vy_stmt_new_key(struct tuple_format *format, const char *key,
		uint32_t part_count);

/**
 * Create a new surrogate DELETE from @a key using format.
 *
 * Example:
 * key: {a3, a5}
 * key_def: { 3, 5 }
 * result: {nil, nil, a3, nil, a5}
 *
 * @param key     MessagePack array with key fields.
 * @param cmp_def Key definition of the result statement (incudes
 *                primary key parts).
 * @param format  Target tuple format.
 *
 * @retval not NULL Success.
 * @retval     NULL Memory or format error.
 */
struct tuple *
vy_stmt_new_surrogate_delete_from_key(const char *key,
				      struct key_def *cmp_def,
				      struct tuple_format *format);

/**
 * Create a new surrogate DELETE from @a tuple using @a format.
 * A surrogate tuple has format->field_count fields from the source
 * with all unindexed fields replaced with MessagePack NIL.
 *
 * Example:
 * original:      {a1, a2, a3, a4, a5}
 * index key_def: {2, 4}
 * result:        {null, a2, null, a4, null}
 *
 * @param format Target tuple format.
 * @param src    Source tuple from the primary index.
 *
 * @retval not NULL Success.
 * @retval     NULL Memory or fields format error.
 */
struct tuple *
vy_stmt_new_surrogate_delete_raw(struct tuple_format *format,
				 const char *data, const char *data_end);

/** @copydoc vy_stmt_new_surrogate_delete_raw. */
static inline struct tuple *
vy_stmt_new_surrogate_delete(struct tuple_format *format,
			     const struct tuple *tuple)
{
	uint32_t size;
	const char *data = tuple_data_range(tuple, &size);
	return vy_stmt_new_surrogate_delete_raw(format, data, data + size);
}

/**
 * Create the REPLACE statement from raw MessagePack data.
 * @param format Format of a tuple for offsets generating.
 * @param tuple_begin MessagePack data that contain an array of fields WITH the
 *                    array header.
 * @param tuple_end End of the array that begins from @param tuple_begin.
 *
 * @retval NULL     Memory allocation error.
 * @retval not NULL Success.
 */
struct tuple *
vy_stmt_new_replace(struct tuple_format *format, const char *tuple,
                    const char *tuple_end);

/**
 * Create the INSERT statement from raw MessagePack data.
 * @param format Format of a tuple for offsets generating.
 * @param tuple_begin MessagePack data that contain an array of fields WITH the
 *                    array header.
 * @param tuple_end End of the array that begins from @param tuple_begin.
 *
 * @retval NULL     Memory allocation error.
 * @retval not NULL Success.
 */
struct tuple *
vy_stmt_new_insert(struct tuple_format *format, const char *tuple_begin,
		   const char *tuple_end);

 /**
 * Create the UPSERT statement from raw MessagePack data.
 * @param tuple_begin MessagePack data that contain an array of fields WITH the
 *                    array header.
 * @param tuple_end End of the array that begins from @param tuple_begin.
 * @param format Format of a tuple for offsets generating.
 * @param part_count Part count from key definition.
 * @param operations Vector of update operations.
 * @param ops_cnt Length of the update operations vector.
 *
 * @retval NULL     Memory allocation error.
 * @retval not NULL Success.
 */
struct tuple *
vy_stmt_new_upsert(struct tuple_format *format,
		   const char *tuple_begin, const char *tuple_end,
		   struct iovec *operations, uint32_t ops_cnt);

/**
 * Create REPLACE statement from UPSERT statement.
 *
 * @param upsert         Upsert statement.
 * @retval not NULL Success.
 * @retval     NULL Memory error.
 */
struct tuple *
vy_stmt_replace_from_upsert(const struct tuple *upsert);

/**
 * Extract MessagePack data from the REPLACE/UPSERT statement.
 * @param stmt An UPSERT or REPLACE statement.
 * @param[out] p_size Size of the MessagePack array in bytes.
 *
 * @return MessagePack array of tuple fields.
 */
static inline const char *
vy_upsert_data_range(const struct tuple *tuple, uint32_t *p_size)
{
	assert(vy_stmt_type(tuple) == IPROTO_UPSERT);
	const char *mp = tuple_data(tuple);
	assert(mp_typeof(*mp) == MP_ARRAY);
	const char *mp_end = mp;
	mp_next(&mp_end);
	assert(mp < mp_end);
	*p_size = mp_end - mp;
	return mp;
}

/**
 * Extract the operations array from the UPSERT statement.
 * @param stmt An UPSERT statement.
 * @param mp_size Out parameter for size of the returned array.
 *
 * @retval Pointer on MessagePack array of update operations.
 */
static inline const char *
vy_stmt_upsert_ops(const struct tuple *tuple, uint32_t *mp_size)
{
	assert(vy_stmt_type(tuple) == IPROTO_UPSERT);
	const char *mp = tuple_data(tuple);
	mp_next(&mp);
	*mp_size = tuple_data(tuple) + tuple->bsize - mp;
	return mp;
}

/**
 * Create a key statement from MessagePack array.
 * @param format  Format of an index.
 * @param key     MessagePack array of key fields.
 *
 * @retval not NULL Success.
 * @retval     NULL Memory error.
 */
static inline struct tuple *
vy_stmt_new_key_from_array(struct tuple_format *format, const char *key)
{
	uint32_t part_count = mp_decode_array(&key);
	return vy_stmt_new_key(format, key, part_count);
}

/**
 * Extract the key from a tuple by the given key definition
 * and store the result in a SELECT statement allocated with
 * malloc().
 */
struct tuple *
vy_stmt_extract_key(const struct tuple *stmt, struct key_def *key_def,
		    struct tuple_format *format);

/**
 * Extract the key from msgpack by the given key definition
 * and store the result in a SELECT statement allocated with
 * malloc().
 */
struct tuple *
vy_stmt_extract_key_raw(const char *data, const char *data_end,
			struct key_def *key_def,
			struct tuple_format *format);

/**
 * Encode vy_stmt for a primary key as xrow_header
 *
 * @param value statement to encode
 * @param key_def key definition
 * @param space_id is written to the request header unless it is 0.
 * Pass 0 to save some space in xrow.
 * @param xrow[out] xrow to fill
 *
 * @retval 0 if OK
 * @retval -1 if error
 */
int
vy_stmt_encode_primary(const struct tuple *value, struct key_def *key_def,
		       uint32_t space_id, struct xrow_header *xrow);

/**
 * Encode vy_stmt for a secondary key as xrow_header
 *
 * @param value statement to encode
 * @param key_def key definition
 * @param xrow[out] xrow to fill
 *
 * @retval 0 if OK
 * @retval -1 if error
 */
int
vy_stmt_encode_secondary(const struct tuple *value, struct key_def *cmp_def,
			 struct xrow_header *xrow);

/**
 * Reconstruct vinyl tuple info and data from xrow
 *
 * @retval stmt on success
 * @retval NULL on error
 */
struct tuple *
vy_stmt_decode(struct xrow_header *xrow, const struct key_def *key_def,
	       struct tuple_format *format, bool is_primary);

/**
 * Format a statement into string.
 * Example: REPLACE([1, 2, "string"], lsn=48)
 */
int
vy_stmt_snprint(char *buf, int size, const struct tuple *stmt);

/*
 * Format a statement into string using a static buffer.
 * Useful for gdb and say_debug().
 * \sa vy_stmt_snprint()
 */
const char *
vy_stmt_str(const struct tuple *stmt);

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */

#endif /* INCLUDES_TARANTOOL_BOX_VY_STMT_H */
