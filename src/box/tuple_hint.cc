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
#include "coll.h"
#include "tuple.h"
#include "tuple_hint.h"

static uint64_t
key_hint_default(const char *key, struct key_def *key_def)
{
	(void)key;
	(void)key_def;
	return 0;
}

static uint64_t
tuple_hint_default(const struct tuple *tuple, struct key_def *key_def)
{
	(void)tuple;
	(void)key_def;
	return 0;
}

template<bool is_nullable>
static uint64_t
key_hint_uint(const char *key, struct key_def *key_def)
{
	(void)key_def;
	assert(key_part_is_nullable(key_def->parts) == is_nullable);
	assert(key_def->parts->type == FIELD_TYPE_UNSIGNED);
	if (is_nullable && mp_typeof(*key) == MP_NIL)
		return 0;
	assert(mp_typeof(*key) == MP_UINT);
	uint64_t val = mp_decode_uint(&key);
	if (val > INT64_MAX)
		return INT64_MAX;
	return val - (uint64_t)INT64_MIN;
}

template<bool is_nullable>
static uint64_t
tuple_hint_uint(const struct tuple *tuple, struct key_def *key_def)
{
	assert(key_part_is_nullable(key_def->parts) == is_nullable);
	assert(key_def->parts->type == FIELD_TYPE_UNSIGNED);
	const char *field = tuple_field_by_part(tuple, key_def->parts);
	if (is_nullable && field == NULL)
		return 0;
	return key_hint_uint<is_nullable>(field, key_def);
}

template<bool is_nullable>
static uint64_t
key_hint_int(const char *key, struct key_def *key_def)
{
	assert(key_part_is_nullable(key_def->parts) == is_nullable);
	assert(key_def->parts->type == FIELD_TYPE_INTEGER);
	if (is_nullable && mp_typeof(*key) == MP_NIL)
		return 0;
	if (mp_typeof(*key) == MP_UINT) {
		uint64_t val = mp_decode_uint(&key);
		if (val > INT64_MAX)
			return INT64_MAX;
		return val - (uint64_t)INT64_MIN;
	} else {
		assert(mp_typeof(*key) == MP_INT);
		int64_t val = mp_decode_int(&key);
		return (uint64_t)val - (uint64_t)INT64_MIN;
	}
}

template<bool is_nullable>
static uint64_t
tuple_hint_int(const struct tuple *tuple, struct key_def *key_def)
{
	assert(key_part_is_nullable(key_def->parts) == is_nullable);
	assert(key_def->parts->type == FIELD_TYPE_INTEGER);
	const char *field = tuple_field_by_part(tuple, key_def->parts);
	if (is_nullable && field == NULL)
		return 0;
	return key_hint_int<is_nullable>(field, key_def);
}

template<bool is_nullable>
static uint64_t
key_hint_string(const char *key, struct key_def *key_def)
{
	(void)key_def;
	assert(key_part_is_nullable(key_def->parts) == is_nullable);
	assert(key_def->parts->coll == NULL);
	assert(key_def->parts->type == FIELD_TYPE_STRING);
	if (is_nullable && mp_typeof(*key) == MP_NIL)
		return 0;
	assert(mp_typeof(*key) == MP_STR);
	uint32_t len;
	const unsigned char *str =
		(const unsigned char *)mp_decode_str(&key, &len);
	uint64_t result = 0;
	uint32_t process_len = MIN(len, 8);
	for (uint32_t i = 0; i < process_len; i++) {
		result <<= 8;
		result |= str[i];
	}
	result <<= 8 * (8 - process_len);
	return result;
}

template<bool is_nullable>
static uint64_t
tuple_hint_string(const struct tuple *tuple, struct key_def *key_def)
{
	assert(key_part_is_nullable(key_def->parts) == is_nullable);
	assert(key_def->parts->coll == NULL);
	assert(key_def->parts->type == FIELD_TYPE_STRING);
	const char *field = tuple_field_by_part(tuple, key_def->parts);
	if (is_nullable && field == NULL)
		return 0;
	return key_hint_string<is_nullable>(field, key_def);
}

template<bool is_nullable>
static uint64_t
key_hint_string_coll(const char *key, struct key_def *key_def)
{
	assert(key_part_is_nullable(key_def->parts) == is_nullable);
	assert(key_def->parts->type == FIELD_TYPE_STRING &&
	        key_def->parts->coll != NULL);
	if (is_nullable && mp_typeof(*key) == MP_NIL)
		return 0;
	assert(mp_typeof(*key) == MP_STR);
	uint32_t len;
	const char *str = mp_decode_str(&key, &len);
	return key_def->parts->coll->hint(str, len, key_def->parts->coll);
}

template<bool is_nullable>
static uint64_t
tuple_hint_string_coll(const struct tuple *tuple, struct key_def *key_def)
{
	assert(key_part_is_nullable(key_def->parts) == is_nullable);
	assert(key_def->parts->type == FIELD_TYPE_STRING &&
	        key_def->parts->coll != NULL);
	const char *field = tuple_field_by_part(tuple, key_def->parts);
	if (is_nullable && field == NULL)
		return 0;
	return key_hint_string_coll<is_nullable>(field, key_def);
}

void
key_def_set_hint_func(struct key_def *def)
{
	def->key_hint = key_hint_default;
	def->tuple_hint = tuple_hint_default;
	bool is_nullable = key_part_is_nullable(def->parts);
	if (def->parts->type == FIELD_TYPE_STRING && def->parts->coll != NULL) {
		def->key_hint = is_nullable ? key_hint_string_coll<true> :
					      key_hint_string_coll<false>;
		def->tuple_hint = is_nullable ? tuple_hint_string_coll<true> :
						tuple_hint_string_coll<false>;
		return;
	}
	switch (def->parts->type) {
	case FIELD_TYPE_UNSIGNED:
		def->key_hint = is_nullable ? key_hint_uint<true> :
					      key_hint_uint<false>;
		def->tuple_hint = is_nullable ? tuple_hint_uint<true> :
						tuple_hint_uint<false>;
		break;
	case FIELD_TYPE_INTEGER:
		def->key_hint = is_nullable ? key_hint_int<true> :
					      key_hint_int<false>;
		def->tuple_hint = is_nullable ? tuple_hint_int<true> :
						tuple_hint_int<false>;
		break;
	case FIELD_TYPE_STRING:
		def->key_hint = is_nullable ? key_hint_string<true> :
					      key_hint_string<false>;
		def->tuple_hint = is_nullable ? tuple_hint_string<true> :
						tuple_hint_string<false>;
		break;
	default:
		break;
	};
}
