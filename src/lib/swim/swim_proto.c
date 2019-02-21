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
#include "swim_proto.h"
#include "msgpuck.h"
#include "say.h"
#include "version.h"
#include "diag.h"

const char *swim_member_status_strs[] = {
	"alive",
	"suspected",
	"dead",
	"left",
};

const char *swim_fd_msg_type_strs[] = {
	"ping",
	"ack",
};

int
swim_decode_map(const char **pos, const char *end, uint32_t *size,
		const char *msg_pref, const char *param_name)
{
	if (mp_typeof(**pos) != MP_MAP || mp_check_map(*pos, end) > 0) {
		diag_set(SwimError, "%s %s should be a map", msg_pref,
			 param_name);
		return -1;
	}
	*size = mp_decode_map(pos);
	return 0;
}

int
swim_decode_array(const char **pos, const char *end, uint32_t *size,
		  const char *msg_pref, const char *param_name)
{
	if (mp_typeof(**pos) != MP_ARRAY || mp_check_array(*pos, end) > 0) {
		diag_set(SwimError, "%s %s should be an array", msg_pref,
			 param_name);
		return -1;
	}
	*size = mp_decode_array(pos);
	return 0;
}

int
swim_decode_uint(const char **pos, const char *end, uint64_t *value,
		 const char *msg_pref, const char *param_name)
{
	if (mp_typeof(**pos) != MP_UINT || mp_check_uint(*pos, end) > 0) {
		diag_set(SwimError, "%s %s should be a uint", msg_pref,
			 param_name);
		return -1;
	}
	*value = mp_decode_uint(pos);
	return 0;
}

static inline int
swim_decode_ip(struct sockaddr_in *address, const char **pos, const char *end,
	       const char *msg_pref, const char *param_name)
{
	uint64_t ip;
	if (swim_decode_uint(pos, end, &ip, msg_pref, param_name) != 0)
		return -1;
	if (ip > UINT32_MAX) {
		diag_set(SwimError, "%s %s is an invalid IP address", msg_pref,
			 param_name);
		return -1;
	}
	address->sin_addr.s_addr = ip;
	return 0;
}

static inline int
swim_decode_port(struct sockaddr_in *address, const char **pos, const char *end,
		 const char *msg_pref, const char *param_name)
{
	uint64_t port;
	if (swim_decode_uint(pos, end, &port, msg_pref, param_name) != 0)
		return -1;
	if (port > UINT16_MAX) {
		diag_set(SwimError, "%s %s is an invalid port", msg_pref,
			 param_name);
		return -1;
	}
	address->sin_port = port;
	return 0;
}

static inline int
swim_decode_bin(const char **bin, uint32_t *size, const char **pos,
		const char *end, const char *msg_pref, const char *param_name)
{
	if (mp_typeof(**pos) != MP_BIN || mp_check_binl(*pos, end) > 0) {
		diag_set(SwimError, "%s %s should be bin", msg_pref,
			 param_name);
		return -1;
	}
	*bin = mp_decode_bin(pos, size);
	if (*pos > end) {
		diag_set(SwimError, "%s %s is invalid", msg_pref, param_name);
		return -1;
	}
	return 0;
}

int
swim_decode_uuid(struct tt_uuid *uuid, const char **pos, const char *end,
		 const char *msg_pref, const char *param_name)
{
	uint32_t size;
	const char *bin;
	if (swim_decode_bin(&bin, &size, pos, end, msg_pref, param_name) != 0)
		return -1;
	if (size != UUID_LEN) {
		diag_set(SwimError, "%s %s is invalid", msg_pref, param_name);
		return -1;
	}
	memcpy(uuid, bin, UUID_LEN);
	return 0;
}

void
swim_member_def_create(struct swim_member_def *def)
{
	memset(def, 0, sizeof(*def));
	def->status = MEMBER_ALIVE;
}

/**
 * Decode a MessagePack value of @a key and store it in @a def.
 * @param key Key to read value of.
 * @param[in][out] pos Where a value is stored.
 * @param end End of the buffer.
 * @param msg_pref Error message prefix.
 * @param[out] def Where to store the value.
 *
 * @retval 0 Success.
 * @retval -1 Error.
 */
static int
swim_decode_member_key(enum swim_member_key key, const char **pos,
		       const char *end, const char *msg_pref,
		       struct swim_member_def *def)
{
	uint64_t tmp;
	uint32_t len;
	switch (key) {
	case SWIM_MEMBER_STATUS:
		if (swim_decode_uint(pos, end, &tmp, msg_pref,
				     "member status") != 0)
			return -1;
		if (tmp >= swim_member_status_MAX) {
			diag_set(SwimError, "%s unknown member status",
				 msg_pref);
			return -1;
		}
		def->status = (enum swim_member_status) tmp;
		break;
	case SWIM_MEMBER_ADDRESS:
		if (swim_decode_ip(&def->addr, pos, end, msg_pref,
				   "member address") != 0)
			return -1;
		break;
	case SWIM_MEMBER_PORT:
		if (swim_decode_port(&def->addr, pos, end, msg_pref,
				     "member port") != 0)
			return -1;
		break;
	case SWIM_MEMBER_UUID:
		if (swim_decode_uuid(&def->uuid, pos, end, msg_pref,
				     "member uuid") != 0)
			return -1;
		break;
	case SWIM_MEMBER_INCARNATION:
		if (swim_decode_uint(pos, end, &def->incarnation, msg_pref,
				     "member incarnation") != 0)
			return -1;
		break;
	case SWIM_MEMBER_OLD_UUID:
		if (swim_decode_uuid(&def->old_uuid, pos, end, msg_pref,
				     "member old uuid") != 0)
			return -1;
		break;
	case SWIM_MEMBER_PAYLOAD:
		if (swim_decode_bin(&def->payload, &len, pos, end, msg_pref,
				    "member payload") != 0)
			return -1;
		if (len > MAX_PAYLOAD_SIZE) {
			diag_set(SwimError, "%s member payload size should be "\
				 "<= %d", msg_pref, MAX_PAYLOAD_SIZE);
			return -1;
		}
		def->payload_size = (int) len;
		def->is_payload_specified = true;
		break;
	default:
		unreachable();
	}
	return 0;
}

int
swim_member_def_decode(struct swim_member_def *def, const char **pos,
		       const char *end, const char *msg_pref)
{
	uint32_t size;
	if (swim_decode_map(pos, end, &size, msg_pref, "member") != 0)
		return -1;
	swim_member_def_create(def);
	for (uint32_t j = 0; j < size; ++j) {
		uint64_t key;
		if (swim_decode_uint(pos, end, &key, msg_pref,
				     "member key") != 0)
			return -1;
		if (key >= swim_member_key_MAX) {
			diag_set(SwimError, "%s unknown member key", msg_pref);
			return -1;
		}
		if (swim_decode_member_key(key, pos, end, msg_pref, def) != 0)
			return -1;
	}
	if (def->addr.sin_port == 0 || def->addr.sin_addr.s_addr == 0) {
		diag_set(SwimError, "%s member address is mandatory", msg_pref);
		return -1;
	}
	if (tt_uuid_is_nil(&def->uuid)) {
		diag_set(SwimError, "%s member uuid is mandatory", msg_pref);
		return -1;
	}
	return 0;
}

void
swim_src_uuid_bin_create(struct swim_src_uuid_bin *header,
			 const struct tt_uuid *uuid)
{
	header->k_uuid = SWIM_SRC_UUID;
	header->m_uuid = 0xc4;
	header->m_uuid_len = UUID_LEN;
	memcpy(header->v_uuid, uuid, UUID_LEN);
}

void
swim_fd_header_bin_create(struct swim_fd_header_bin *header,
			  enum swim_fd_msg_type type, uint64_t incarnation)
{
	header->k_header = SWIM_FAILURE_DETECTION;
	header->m_header = 0x82;

	header->k_type = SWIM_FD_MSG_TYPE;
	header->v_type = type;

	header->k_incarnation = SWIM_FD_INCARNATION;
	header->m_incarnation = 0xcf;
	header->v_incarnation = mp_bswap_u64(incarnation);
}

int
swim_failure_detection_def_decode(struct swim_failure_detection_def *def,
				  const char **pos, const char *end,
				  const char *msg_pref)
{
	uint32_t size;
	if (swim_decode_map(pos, end, &size, msg_pref, "root") != 0)
		return -1;
	memset(def, 0, sizeof(*def));
	def->type = swim_fd_msg_type_MAX;
	if (size != 2) {
		diag_set(SwimError, "%s root map should have two keys - "\
			 "message type and incarnation", msg_pref);
		return -1;
	}
	for (int i = 0; i < (int) size; ++i) {
		uint64_t key;
		if (swim_decode_uint(pos, end, &key, msg_pref, "a key") != 0)
			return -1;
		switch(key) {
		case SWIM_FD_MSG_TYPE:
			if (swim_decode_uint(pos, end, &key, msg_pref,
					     "message type") != 0)
				return -1;
			if (key >= swim_fd_msg_type_MAX) {
				diag_set(SwimError, "%s unknown message type",
					 msg_pref);
				return -1;
			}
			def->type = key;
			break;
		case SWIM_FD_INCARNATION:
			if (swim_decode_uint(pos, end, &def->incarnation,
					     msg_pref, "incarnation") != 0)
				return -1;
			break;
		default:
			diag_set(SwimError, "%s unexpected key", msg_pref);
			return -1;
		}
	}
	if (def->type == swim_fd_msg_type_MAX) {
		diag_set(SwimError, "%s message type should be specified",
			 msg_pref);
		return -1;
	}
	return 0;
}

void
swim_anti_entropy_header_bin_create(struct swim_anti_entropy_header_bin *header,
				    uint16_t batch_size)
{
	header->k_anti_entropy = SWIM_ANTI_ENTROPY;
	header->m_anti_entropy = 0xcd;
	header->v_anti_entropy = mp_bswap_u16(batch_size);
}

void
swim_member_bin_fill(struct swim_member_bin *header,
		     const struct sockaddr_in *addr, const struct tt_uuid *uuid,
		     enum swim_member_status status, uint64_t incarnation,
		     uint16_t payload_size)
{
	header->v_status = status;
	header->v_addr = mp_bswap_u32(addr->sin_addr.s_addr);
	header->v_port = mp_bswap_u16(addr->sin_port);
	memcpy(header->v_uuid, uuid, UUID_LEN);
	header->v_incarnation = mp_bswap_u64(incarnation);
	header->v_payload_size = mp_bswap_u16(payload_size);
}

void
swim_member_bin_create(struct swim_member_bin *header)
{
	header->m_header = 0x85;
	header->k_status = SWIM_MEMBER_STATUS;
	header->k_addr = SWIM_MEMBER_ADDRESS;
	header->m_addr = 0xce;
	header->k_port = SWIM_MEMBER_PORT;
	header->m_port = 0xcd;
	header->k_uuid = SWIM_MEMBER_UUID;
	header->m_uuid = 0xc4;
	header->m_uuid_len = UUID_LEN;
	header->k_incarnation = SWIM_MEMBER_INCARNATION;
	header->m_incarnation = 0xcf;
	header->k_payload = SWIM_MEMBER_PAYLOAD;
	header->m_payload_size = 0xc5;
}

void
swim_diss_header_bin_create(struct swim_diss_header_bin *header,
			    uint16_t batch_size)
{
	header->k_header = SWIM_DISSEMINATION;
	header->m_header = 0xcd;
	header->v_header = mp_bswap_u16(batch_size);
}

void
swim_event_bin_create(struct swim_event_bin *header)
{
	header->k_status = SWIM_MEMBER_STATUS;
	header->k_addr = SWIM_MEMBER_ADDRESS;
	header->m_addr = 0xce;
	header->k_port = SWIM_MEMBER_PORT;
	header->m_port = 0xcd;
	header->k_uuid = SWIM_MEMBER_UUID;
	header->m_uuid = 0xc4;
	header->m_uuid_len = UUID_LEN;
	header->k_incarnation = SWIM_MEMBER_INCARNATION;
	header->m_incarnation = 0xcf;
}

void
swim_event_bin_fill(struct swim_event_bin *header,
		    enum swim_member_status status,
		    const struct sockaddr_in *addr, const struct tt_uuid *uuid,
		    uint64_t incarnation, int old_uuid_ttl, int payload_ttl)
{
	header->m_header = 0x85 + (old_uuid_ttl > 0) + (payload_ttl > 0);
	header->v_status = status;
	header->v_addr = mp_bswap_u32(addr->sin_addr.s_addr);
	header->v_port = mp_bswap_u16(addr->sin_port);
	memcpy(header->v_uuid, uuid, UUID_LEN);
	header->v_incarnation = mp_bswap_u64(incarnation);
}

void
swim_old_uuid_bin_create(struct swim_old_uuid_bin *header)
{
	header->k_uuid = SWIM_MEMBER_OLD_UUID;
	header->m_uuid = 0xc4;
	header->m_uuid_len = UUID_LEN;
}

void
swim_old_uuid_bin_fill(struct swim_old_uuid_bin *header,
		       const struct tt_uuid *uuid)
{
	memcpy(header->v_uuid, uuid, UUID_LEN);
}

void
swim_meta_header_bin_create(struct swim_meta_header_bin *header,
			    const struct sockaddr_in *src, bool has_routing)
{
	header->m_header = 0x83 + has_routing;
	header->k_version = SWIM_META_TARANTOOL_VERSION;
	header->m_version = 0xce;
	header->v_version = mp_bswap_u32(tarantool_version_id());
	header->k_addr = SWIM_META_SRC_ADDRESS;
	header->m_addr = 0xce;
	header->v_addr = mp_bswap_u32(src->sin_addr.s_addr);
	header->k_port = SWIM_META_SRC_PORT;
	header->m_port = 0xcd;
	header->v_port = mp_bswap_u16(src->sin_port);
}

/**
 * Decode meta routing section into meta definition object.
 * @param[out] def Definition to decode into.
 * @param[in][out] pos MessagePack buffer to decode.
 * @param end End of the MessagePack buffer.
 *
 * @retval 0 Success.
 * @retval -1 Error.
 */
static int
swim_meta_def_decode_route(struct swim_meta_def *def, const char **pos,
			   const char *end)
{
	const char *msg_pref = "invalid routing section:";
	uint32_t size;
	if (swim_decode_map(pos, end, &size, msg_pref, "route") != 0)
		return -1;
	for (uint32_t i = 0; i < size; ++i) {
		uint64_t key;
		if (swim_decode_uint(pos, end, &key, msg_pref, "a key") != 0)
			return -1;
		switch (key) {
		case SWIM_ROUTE_SRC_ADDRESS:
			if (swim_decode_ip(&def->route.src, pos, end, msg_pref,
					   "source address") != 0)
				return -1;
			break;
		case SWIM_ROUTE_SRC_PORT:
			if (swim_decode_port(&def->route.src, pos, end,
					     msg_pref, "source port") != 0)
				return -1;
			break;
		case SWIM_ROUTE_DST_ADDRESS:
			if (swim_decode_ip(&def->route.dst, pos, end, msg_pref,
					   "destination address") != 0)
				return -1;
			break;
		case SWIM_ROUTE_DST_PORT:
			if (swim_decode_port(&def->route.dst, pos, end,
					     msg_pref, "destination port") != 0)
				return -1;
			break;
		default:
			diag_set(SwimError, "%s unknown key", msg_pref);
			return -1;
		}
	}
	if (def->route.src.sin_port == 0 ||
	    def->route.src.sin_addr.s_addr == 0) {
		diag_set(SwimError, "%s source address should be specified",
			 msg_pref);
		return -1;
	}
	if (def->route.dst.sin_port == 0 ||
	    def->route.dst.sin_addr.s_addr == 0) {
		diag_set(SwimError, "%s destination address should be "\
			 "specified", msg_pref);
		return -1;
	}
	def->is_route_specified = true;
	return 0;
}

int
swim_meta_def_decode(struct swim_meta_def *def, const char **pos,
		     const char *end)
{
	const char *msg_pref = "invalid meta section:";
	uint32_t size;
	if (swim_decode_map(pos, end, &size, msg_pref, "root") != 0)
		return -1;
	memset(def, 0, sizeof(*def));
	for (uint32_t i = 0; i < size; ++i) {
		uint64_t key;
		if (swim_decode_uint(pos, end, &key, msg_pref, "a key") != 0)
			return -1;
		switch (key) {
		case SWIM_META_ROUTING:
			if (swim_meta_def_decode_route(def, pos, end) != 0)
				return -1;
			break;
		case SWIM_META_TARANTOOL_VERSION:
			if (swim_decode_uint(pos, end, &key, msg_pref,
					     "version") != 0)
				return -1;
			if (key > UINT32_MAX) {
				diag_set(SwimError, "%s invalid version, too "\
					 "big", msg_pref);
				return -1;
			}
			def->version = key;
			break;
		case SWIM_META_SRC_ADDRESS:
			if (swim_decode_ip(&def->src, pos, end, msg_pref,
					   "source address") != 0)
				return -1;
			break;
		case SWIM_META_SRC_PORT:
			if (swim_decode_port(&def->src, pos, end, msg_pref,
					     "source port") != 0)
				return -1;
			break;
		default:
			diag_set(SwimError, "%s unknown key", msg_pref);
			return -1;
		}
	}
	if (def->version == 0) {
		diag_set(SwimError, "%s version is mandatory", msg_pref);
		return -1;
	}
	if (def->src.sin_port == 0 || def->src.sin_addr.s_addr == 0) {
		diag_set(SwimError, "%s source address is mandatory", msg_pref);
		return -1;
	}
	return 0;
}

void
swim_route_bin_create(struct swim_route_bin *route,
		      const struct sockaddr_in *src,
		      const struct sockaddr_in *dst)
{
	route->k_routing = SWIM_META_ROUTING;
	route->m_routing = 0x84;
	route->k_src_addr = SWIM_ROUTE_SRC_ADDRESS;
	route->m_src_addr = 0xce;
	route->v_src_addr = mp_bswap_u32(src->sin_addr.s_addr);
	route->k_src_port = SWIM_ROUTE_SRC_PORT;
	route->m_src_port = 0xcd;
	route->v_src_port = mp_bswap_u16(src->sin_port);
	route->k_dst_addr = SWIM_ROUTE_DST_ADDRESS;
	route->m_dst_addr = 0xce;
	route->v_dst_addr = mp_bswap_u32(dst->sin_addr.s_addr);
	route->k_dst_port = SWIM_ROUTE_DST_PORT;
	route->m_dst_port = 0xcd;
	route->v_dst_port = mp_bswap_u16(dst->sin_port);
}

void
swim_quit_bin_create(struct swim_quit_bin *header, uint64_t incarnation)
{
	header->k_quit = SWIM_QUIT;
	header->m_quit = 0x81;
	header->k_incarnation = SWIM_QUIT_INCARNATION;
	header->m_incarnation = 0xcf;
	header->v_incarnation = mp_bswap_u64(incarnation);
}
