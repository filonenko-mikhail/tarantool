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
#include "swim.h"
#include "swim_io.h"
#include "swim_proto.h"
#include "swim_ev.h"
#include "uri/uri.h"
#include "fiber.h"
#include "msgpuck.h"
#include "info.h"
#include "assoc.h"
#include "sio.h"
#define HEAP_FORWARD_DECLARATION
#include "salad/heap.h"

/**
 * SWIM - Scalable Weakly-consistent Infection-style Process Group
 * Membership Protocol. It consists of 2 components: events
 * dissemination and failure detection, and stores in memory a
 * table of known remote hosts - members. Also some SWIM
 * implementations have an additional component: anti-entropy -
 * periodical broadcast of a random subset of members table.
 *
 * Each SWIM component is different from others in both message
 * structures and goals, they even could be sent in different
 * messages. But SWIM describes piggybacking of messages: a ping
 * message can piggyback a dissemination's one. SWIM has a main
 * operating cycle during which it randomly chooses members from a
 * member table and sends to them events + ping. Replies are
 * processed out of the main cycle asynchronously.
 *
 * Random selection provides even network load about ~1 message to
 * each member per protocol step regardless of the cluster size.
 * Without randomness a member would get a network load of N
 * messages each protocol step, since all other members will
 * choose the same member on each step where N is the cluster size.
 *
 * Also SWIM describes a kind of fairness: when selecting a next
 * member to ping, the protocol prefers LRU members. In code it
 * would be too complicated, so Tarantool's implementation is
 * slightly different, easier.
 *
 * Tarantool splits protocol operation into rounds. At the
 * beginning of a round all members are randomly reordered and
 * linked into a list. At each round step a member is popped from
 * the list head, a message is sent to him, and he waits for the
 * next round. In such implementation all random selection of the
 * original SWIM is executed once per round. The round is
 * 'planned' actually. A list is used instead of an array since
 * new members can be added to its tail without realloc, and dead
 * members can be removed as easy as that.
 *
 * Also Tarantool implements third component - anti-entropy. Why
 * is it needed and even vital? Consider the example: two SWIM
 * nodes, both are alive. Nothing happens, so the events list is
 * empty, only pings are being sent periodically. Then a third
 * node appears. It knows about one of existing nodes. How should
 * it learn about another one? Sure, its known counterpart can try
 * to notify another one, but it is UDP, so this event can lost.
 * Anti-entropy is an extra simple component, it just piggybacks
 * random part of members table with each regular round message.
 * In the example above the new node will learn about the third
 * one via anti-entropy messages of the second one soon or late.
 *
 * Surprisingly, original SWIM does not describe any addressing,
 * how to uniquely identify a member. IP/port fallaciously could
 * be considered as a good unique identifier, but some arguments
 * below demolish this belief:
 *
 *     - if instances work in separate containers, they can have
 *       the same IP/port inside a container NATed to a unique
 *       IP/port outside the container;
 *
 *     - IP/port are likely to change during instance lifecycle.
 *       Once IP/port are changed, a ghost of the old member's
 *       configuration still lives for a while until it is
 *       suspected, dead and GC-ed. Taking into account that ACK
 *       timeout can be tens of seconds, 'Dead Souls' can exist
 *       unpleasantly long.
 *
 * Tarantool SWIM implementation uses UUIDs as unique identifiers.
 * UUID is much more unlikely to change than IP/port. But even if
 * that happens, dissemination component for a while gossips the
 * new UUID together with the old one.
 *
 * SWIM implementation is split into 3 parts: protocol logic,
 * transport level, protocol structure.
 *
 *     - protocol logic consist of how to react on various events,
 *       failure detection pings/acks, how often to send messages,
 *       handles logic of three components (failure detection,
 *       anti-entropy, dissemination);
 *
 *     - transport level handles routing, transport headers,
 *       packet forwarding;
 *
 *     - protocol structure describes how packet looks in
 *       MessagePack, which section and header follows which
 *       another one.
 */

enum {
	/**
	 * How often to send membership messages and pings in
	 * seconds. Nothing special in this concrete default
	 * value.
	 */
	HEARTBEAT_RATE_DEFAULT = 1,
	/**
	 * If a ping was sent, it is considered to be lost after
	 * this time without an ack. Nothing special in this
	 * value.
	 */
	ACK_TIMEOUT_DEFAULT = 30,
	/**
	 * If a member has not been responding to pings this
	 * number of times, it is considered to be dead.
	 * According to the SWIM paper, for a member it is enough
	 * to do not respond on one direct ping, and on K
	 * simultanous indirect pings, to be considered as dead.
	 * Seems too little, so here it is bigger.
	 */
	NO_ACKS_TO_DEAD = 3,
	/**
	 * If a member confirmed to be dead, it is removed from
	 * the membership after at least this number of
	 * unacknowledged pings. According to the SWIM paper, a
	 * dead member is deleted immediately. But here it is held
	 * for a while to 1) maybe refute its dead status, 2)
	 * disseminate the status via dissemination and
	 * anti-entropy components.
	 */
	NO_ACKS_TO_GC = 2,
};

/**
 * Take a random number not blindly calculating a modulo, but
 * scaling random number down the given boundaries to preserve the
 * original distribution. The result belongs the range
 * [start, end].
 */
static inline int
swim_scaled_rand(int start, int end)
{
	assert(end >= start);
	/*
	 * RAND_MAX is likely to be INT_MAX - hardly SWIM will
	 * ever be used in such a huge cluster.
	 */
	assert(end - start < RAND_MAX);
	return rand() / (RAND_MAX / (end - start + 1) + 1);
}

/** Calculate UUID hash to use as a members table key. */
static inline uint32_t
swim_uuid_hash(const struct tt_uuid *uuid)
{
	return mh_strn_hash((const char *) uuid, UUID_LEN);
}

/**
 * Helper to do not call tt_static_buf() in all places where it is
 * wanted to get string UUID.
 */
static inline const char *
swim_uuid_str(const struct tt_uuid *uuid)
{
	char *buf = tt_static_buf();
	tt_uuid_to_string(uuid, buf);
	return buf;
}

/**
 * A cluster member description. This structure describes the
 * last known state of an instance, that is updated periodically
 * via UDP according to SWIM protocol.
 */
struct swim_member {
	/**
	 * Member status. Since the communication goes via UDP,
	 * actual status can be different, as well as different on
	 * other SWIM nodes. But SWIM guarantees that each member
	 * will learn a real status of an instance sometime.
	 */
	enum swim_member_status status;
	/** Address of the instance to which send UDP packets. */
	struct sockaddr_in addr;
	/** Unique identifier of the member. Members table key. */
	struct tt_uuid uuid;
	/** Cached hash of the uuid for members table lookups. */
	uint32_t hash;
	/**
	 * Position in a queue of members in the current round.
	 */
	struct rlist in_queue_round;
	/**
	 *
	 *               Failure detection component
	 */
	/** Growing number to refute old messages. */
	uint64_t incarnation;
	/**
	 * How many pings did not receive an ack in a row being in
	 * the current status. After a threshold the instance is
	 * marked as dead. After more it is removed from the
	 * table. On each status or incarnation change this
	 * counter is reset.
	 */
	int unacknowledged_pings;
	/**
	 * When the latest ping is considered to be
	 * unacknowledged.
	 */
	double ping_deadline;
	/** Ready at hand regular ACK task. */
	struct swim_task ack_task;
	/** Ready at hand regular PING task. */
	struct swim_task ping_task;
	/** Position in a queue of members waiting for an ack. */
	struct heap_node in_heap_wait_ack;
};

#define mh_name _swim_table
struct mh_swim_table_key {
	uint32_t hash;
	const struct tt_uuid *uuid;
};
#define mh_key_t struct mh_swim_table_key
#define mh_node_t struct swim_member *
#define mh_arg_t void *
#define mh_hash(a, arg) ((*a)->hash)
#define mh_hash_key(a, arg) (a.hash)
#define mh_cmp(a, b, arg) (tt_uuid_compare(&(*a)->uuid, &(*b)->uuid))
#define mh_cmp_key(a, b, arg) (tt_uuid_compare(a.uuid, &(*b)->uuid))
#define MH_SOURCE 1
#include "salad/mhash.h"

#define HEAP_NAME heap_wait_ack
#define HEAP_LESS(h, a, b) ((a)->ping_deadline < (b)->ping_deadline)
#define heap_value_t struct swim_member
#define heap_value_attr in_heap_wait_ack
#include "salad/heap.h"

/**
 * A wrapper around mh(random) function to always return not the
 * end. The original function for some values depending on bucket
 * count and occupancy can return the end.
 */
static inline mh_int_t
mh_swim_table_random_notnull(struct mh_swim_table_t *t, mh_int_t rnd)
{
	assert(mh_size(t) > 0);
	mh_int_t rc;
	while ((rc = mh_swim_table_random(t, rnd++)) == mh_end(t));
	return rc;
}

/**
 * SWIM instance. Stores configuration, manages periodical tasks,
 * rounds. Each member has an object of this type on its host,
 * while on others it is represented as a struct swim_member
 * object.
 */
struct swim {
	/**
	 * Global hash of all known members of the cluster. Hash
	 * key is UUID, value is a struct member, describing a
	 * remote instance. Discovered members live here until
	 * they are detected as dead - in such a case they are
	 * removed from the hash after a while.
	 */
	struct mh_swim_table_t *members;
	/**
	 * This node. Is used to do not send messages to self,
	 * it's meaningless. Also to refute false gossips about
	 * self status.
	 */
	struct swim_member *self;
	/**
	 * Members to which a message should be sent next during
	 * this round.
	 */
	struct rlist queue_round;
	/** Generator of round step events. */
	struct ev_periodic round_tick;
	/**
	 * Single round step task. It is impossible to have
	 * multiple round steps in the same SWIM instance at the
	 * same time, so it is single and preallocated per SWIM
	 * instance.
	 */
	struct swim_task round_step_task;
	/**
	 * Scheduler of output requests, receiver of incomming
	 * ones.
	 */
	struct swim_scheduler scheduler;
	/**
	 *
	 *               Failure detection component
	 */
	/**
	 * Members waiting for an ACK. On too long absence of an
	 * ACK a member is considered to be dead and is removed.
	 * The list is sorted by deadline in ascending order (tail
	 * is newer, head is older).
	 */
	heap_t heap_wait_ack;
	/** Generator of ack checking events. */
	struct ev_periodic wait_ack_tick;
};

/** Put the member into a list of ACK waiters. */
static void
swim_member_wait_ack(struct swim *swim, struct swim_member *member)
{
	if (heap_node_is_stray(&member->in_heap_wait_ack)) {
		member->ping_deadline = swim_time() +
					swim->wait_ack_tick.interval;
		heap_wait_ack_insert(&swim->heap_wait_ack, member);
	}
}

/**
 * Make all needed actions to process a member's update like a
 * change of its status, or incarnation, or both.
 */
static void
swim_member_status_is_updated(struct swim_member *member)
{
	member->unacknowledged_pings = 0;
}

/**
 * Update status and incarnation of the member if needed. Statuses
 * are compared as a compound key: {incarnation, status}. So @a
 * new_status can override an old one only if its incarnation is
 * greater, or the same, but its status is "bigger". Statuses are
 * compared by their identifier, so "alive" < "dead". This
 * protects from the case when a member is detected as dead on one
 * instance, but overriden by another instance with the same
 * incarnation "alive" message.
 */
static inline void
swim_member_update_status(struct swim_member *member,
			  enum swim_member_status new_status,
			  uint64_t incarnation, struct swim *swim)
{
	(void) swim;
	/*
	 * Source of truth about self is this instance and it is
	 * never updated from remote. Refutation is handled
	 * separately.
	 */
	assert(member != swim->self);
	if (member->incarnation == incarnation) {
		if (member->status < new_status) {
			member->status = new_status;
			swim_member_status_is_updated(member);
		}
	} else if (member->incarnation < incarnation) {
		member->status = new_status;
		member->incarnation = incarnation;
		swim_member_status_is_updated(member);
	}
}

/**
 * SWIM fd mainly is needed to be printed into the logs in order
 * to distinguish between different SWIM instances logs.
 */
static inline int
swim_fd(const struct swim *swim)
{
	return swim->scheduler.transport.fd;
}

/**
 * A helper to get a pointer to a SWIM instance having only a
 * pointer to it scheduler. It is used by task complete functions.
 */
static inline struct swim *
swim_by_scheduler(struct swim_scheduler *scheduler)
{
	return container_of(scheduler, struct swim, scheduler);
}

/**
 * Remove the member from all queues, hashes, destroy it and free
 * the memory.
 */
static void
swim_member_delete(struct swim *swim, struct swim_member *member)
{
	say_verbose("SWIM %d: member %s is deleted", swim_fd(swim),
		    swim_uuid_str(&member->uuid));
	struct mh_swim_table_key key = {member->hash, &member->uuid};
	mh_int_t rc = mh_swim_table_find(swim->members, key, NULL);
	assert(rc != mh_end(swim->members));
	mh_swim_table_del(swim->members, rc, NULL);
	rlist_del_entry(member, in_queue_round);

	/* Failure detection component. */
	if (! heap_node_is_stray(&member->in_heap_wait_ack))
		heap_wait_ack_delete(&swim->heap_wait_ack, member);
	swim_task_destroy(&member->ack_task);
	swim_task_destroy(&member->ping_task);

	free(member);
}

/** Find a member by UUID. */
static inline struct swim_member *
swim_find_member(struct swim *swim, const struct tt_uuid *uuid)
{
	struct mh_swim_table_key key = {swim_uuid_hash(uuid), uuid};
	mh_int_t node = mh_swim_table_find(swim->members, key, NULL);
	if (node == mh_end(swim->members))
		return NULL;
	return *mh_swim_table_node(swim->members, node);
}

/**
 * Once a ping is sent, the member should start waiting for an
 * ACK.
 */
static void
swim_ping_task_complete(struct swim_task *task,
			struct swim_scheduler *scheduler, int rc)
{
	/*
	 * If ping send has failed, it makes to sense to wait for
	 * an ACK.
	 */
	if (rc != 0)
		return;
	struct swim *swim = swim_by_scheduler(scheduler);
	struct swim_member *m = container_of(task, struct swim_member,
					     ping_task);
	swim_member_wait_ack(swim, m);
}

/**
 * Register a new member with a specified status. Here it is
 * added to the hash, to the 'next' queue.
 */
static struct swim_member *
swim_member_new(struct swim *swim, const struct sockaddr_in *addr,
		const struct tt_uuid *uuid, enum swim_member_status status,
		uint64_t incarnation)
{
	if (heap_wait_ack_reserve(&swim->heap_wait_ack) != 0) {
		diag_set(OutOfMemory, sizeof(struct heap_node), "realloc",
			 "heap_wait_ack");
		return NULL;
	}
	struct swim_member *member =
		(struct swim_member *) calloc(1, sizeof(*member));
	if (member == NULL) {
		diag_set(OutOfMemory, sizeof(*member), "calloc", "member");
		return NULL;
	}
	member->status = status;
	member->addr = *addr;
	member->uuid = *uuid;
	member->hash = swim_uuid_hash(uuid);
	mh_int_t rc = mh_swim_table_put(swim->members,
					(const struct swim_member **) &member,
					NULL, NULL);
	if (rc == mh_end(swim->members)) {
		free(member);
		diag_set(OutOfMemory, sizeof(mh_int_t), "malloc", "node");
		return NULL;
	}
	rlist_add_entry(&swim->queue_round, member, in_queue_round);

	/* Failure detection component. */
	member->incarnation = incarnation;
	heap_node_create(&member->in_heap_wait_ack);
	swim_task_create(&member->ack_task, NULL, NULL);
	swim_task_create(&member->ping_task, swim_ping_task_complete, NULL);

	say_verbose("SWIM %d: member %s is added", swim_fd(swim),
		    swim_uuid_str(uuid));
	return member;
}

/**
 * Take all the members from the table and shuffle them randomly.
 * Is used for forthcoming round planning.
 */
static struct swim_member **
swim_shuffle_members(struct swim *swim)
{
	struct mh_swim_table_t *members = swim->members;
	struct swim_member **shuffled;
	int bsize = sizeof(shuffled[0]) * mh_size(members);
	shuffled = (struct swim_member **) malloc(bsize);
	if (shuffled == NULL) {
		diag_set(OutOfMemory, bsize, "malloc", "shuffled");
		return NULL;
	}
	int i = 0;
	/*
	 * This shuffling preserves even distribution of a random
	 * sequence, that is proved by testing.
	 */
	for (mh_int_t node = mh_first(members), end = mh_end(members);
	     node != end; node = mh_next(members, node), ++i) {
		shuffled[i] = *mh_swim_table_node(members, node);
		int j = swim_scaled_rand(0, i);
		SWAP(shuffled[i], shuffled[j]);
	}
	return shuffled;
}

/**
 * Shuffle members, build randomly ordered queue of addressees. In
 * other words, do all round preparation work.
 */
static int
swim_new_round(struct swim *swim)
{
	int size = mh_size(swim->members);
	say_verbose("SWIM %d: start a new round with %d members", swim_fd(swim),
		    size);
	struct swim_member **shuffled = swim_shuffle_members(swim);
	if (shuffled == NULL)
		return -1;
	rlist_create(&swim->queue_round);
	for (int i = 0; i < size; ++i) {
		if (shuffled[i] != swim->self) {
			rlist_add_entry(&swim->queue_round, shuffled[i],
					in_queue_round);
		}
	}
	free(shuffled);
	return 0;
}

/**
 * Encode anti-entropy header and random members data as many as
 * possible to the end of the packet.
 * @retval 0 Not error, but nothing is encoded.
 * @retval 1 Something is encoded.
 */
static int
swim_encode_anti_entropy(struct swim *swim, struct swim_packet *packet)
{
	struct swim_anti_entropy_header_bin ae_header_bin;
	struct swim_member_bin member_bin;
	int size = sizeof(ae_header_bin);
	char *header = swim_packet_reserve(packet, size);
	if (header == NULL)
		return 0;
	swim_member_bin_create(&member_bin);
	struct mh_swim_table_t *t = swim->members;
	int i = 0, member_count = mh_size(t);
	int rnd = swim_scaled_rand(0, member_count - 1);
	for (mh_int_t rc = mh_swim_table_random_notnull(t, rnd),
	     end = mh_end(t); i < member_count; ++i) {
		struct swim_member *m = *mh_swim_table_node(t, rc);
		int new_size = size + sizeof(member_bin);
		char *pos = swim_packet_reserve(packet, new_size);
		if (pos == NULL)
			break;
		size = new_size;
		swim_member_bin_fill(&member_bin, &m->addr, &m->uuid,
				     m->status, m->incarnation);
		memcpy(pos, &member_bin, sizeof(member_bin));
		/*
		 * First random member could be choosen too close
		 * to the hash end. Here the cycle is wrapped, if
		 * a packet still has free memory, but the
		 * iterator has already reached the hash end.
		 */
		rc = mh_next(t, rc);
		if (rc == end)
			rc = mh_first(t);
	}
	if (i == 0)
		return 0;
	swim_packet_advance(packet, size);
	swim_anti_entropy_header_bin_create(&ae_header_bin, i);
	memcpy(header, &ae_header_bin, sizeof(ae_header_bin));
	return 1;
}

/**
 * Encode source UUID.
 * @retval 0 Not error, but nothing is encoded.
 * @retval 1 Something is encoded.
 */
static inline int
swim_encode_src_uuid(struct swim *swim, struct swim_packet *packet)
{
	struct swim_src_uuid_bin uuid_bin;
	char *pos = swim_packet_alloc(packet, sizeof(uuid_bin));
	if (pos == NULL)
		return 0;
	swim_src_uuid_bin_create(&uuid_bin, &swim->self->uuid);
	memcpy(pos, &uuid_bin, sizeof(uuid_bin));
	return 1;
}

/**
 * Encode failure detection component.
 * @retval 0 Not error, but nothing is encoded.
 * @retval 1 Something is encoded.
 */
static int
swim_encode_failure_detection(struct swim *swim, struct swim_packet *packet,
			      enum swim_fd_msg_type type)
{
	struct swim_fd_header_bin fd_header_bin;
	int size = sizeof(fd_header_bin);
	char *pos = swim_packet_alloc(packet, size);
	if (pos == NULL)
		return 0;
	swim_fd_header_bin_create(&fd_header_bin, type,
				  swim->self->incarnation);
	memcpy(pos, &fd_header_bin, size);
	return 1;
}

/** Encode SWIM components into a UDP packet. */
static void
swim_encode_round_msg(struct swim *swim, struct swim_packet *packet)
{
	swim_packet_create(packet);
	char *header = swim_packet_alloc(packet, 1);
	int map_size = 0;
	map_size += swim_encode_src_uuid(swim, packet);
	map_size += swim_encode_failure_detection(swim, packet,
						  SWIM_FD_MSG_PING);
	map_size += swim_encode_anti_entropy(swim, packet);

	assert(mp_sizeof_map(map_size) == 1 && map_size >= 2);
	mp_encode_map(header, map_size);
}

/**
 * Once per specified timeout trigger a next round step. In round
 * step a next memeber is taken from the round queue and a round
 * message is sent to him. One member per step.
 */
static void
swim_round_step_begin(struct ev_loop *loop, struct ev_periodic *p, int events)
{
	assert((events & EV_PERIODIC) != 0);
	(void) events;
	struct swim *swim = (struct swim *) p->data;
	if (rlist_empty(&swim->queue_round) && swim_new_round(swim) != 0) {
		diag_log();
		return;
	}
	/*
	 * Possibly empty, if no members but self are specified.
	 */
	if (rlist_empty(&swim->queue_round))
		return;

	swim_encode_round_msg(swim, &swim->round_step_task.packet);
	struct swim_member *m =
		rlist_first_entry(&swim->queue_round, struct swim_member,
				  in_queue_round);
	swim_task_send(&swim->round_step_task, &m->addr, &swim->scheduler);
	swim_ev_periodic_stop(loop, p);
}

/**
 * After a round message is sent, the addressee can be popped from
 * the queue, and the next step is scheduled.
 */
static void
swim_round_step_complete(struct swim_task *task,
			 struct swim_scheduler *scheduler, int rc)
{
	(void) rc;
	(void) task;
	struct swim *swim = swim_by_scheduler(scheduler);
	swim_ev_periodic_start(loop(), &swim->round_tick);
	struct swim_member *m =
		rlist_shift_entry(&swim->queue_round, struct swim_member,
				  in_queue_round);
	if (rc == 0) {
		/*
		 * Each round message contains failure detection
		 * section with a ping.
		 */
		swim_member_wait_ack(swim, m);
	}
}

/** Schedule send of a failure detection message. */
static void
swim_send_fd_request(struct swim *swim, struct swim_task *task,
		     const struct sockaddr_in *dst, enum swim_fd_msg_type type)
{
	/*
	 * Reset packet allocator in case if task is being reused.
	 */
	swim_packet_create(&task->packet);
	char *header = swim_packet_alloc(&task->packet, 1);
	int map_size = swim_encode_src_uuid(swim, &task->packet);
	map_size += swim_encode_failure_detection(swim, &task->packet, type);
	assert(map_size == 2);
	mp_encode_map(header, map_size);
	say_verbose("SWIM %d: send %s to %s", swim_fd(swim),
		    swim_fd_msg_type_strs[type],
		    sio_strfaddr((struct sockaddr *) dst, sizeof(*dst)));
	swim_task_send(task, dst, &swim->scheduler);
}

/** Schedule send of an ack. */
static inline void
swim_send_ack(struct swim *swim, struct swim_task *task,
	      const struct sockaddr_in *dst)
{
	swim_send_fd_request(swim, task, dst, SWIM_FD_MSG_ACK);
}

/** Schedule send of a ping. */
static inline void
swim_send_ping(struct swim *swim, struct swim_task *task,
	       const struct sockaddr_in *dst)
{
	swim_send_fd_request(swim, task, dst, SWIM_FD_MSG_PING);
}

/**
 * Check for unacknowledged pings. A ping is unacknowledged if an
 * ack was not received during ack timeout. An unacknowledged ping
 * is resent here.
 */
static void
swim_check_acks(struct ev_loop *loop, struct ev_periodic *p, int events)
{
	assert((events & EV_PERIODIC) != 0);
	(void) loop;
	(void) events;
	struct swim *swim = (struct swim *) p->data;
	double current_time = swim_time();
	struct swim_member *m;
	while ((m = heap_wait_ack_top(&swim->heap_wait_ack)) != NULL) {
		if (current_time < m->ping_deadline)
			break;
		heap_wait_ack_pop(&swim->heap_wait_ack);
		++m->unacknowledged_pings;
		switch (m->status) {
		case MEMBER_ALIVE:
			if (m->unacknowledged_pings >= NO_ACKS_TO_DEAD) {
				m->status = MEMBER_DEAD;
				swim_member_status_is_updated(m);
			}
			break;
		case MEMBER_DEAD:
			if (m->unacknowledged_pings >= NO_ACKS_TO_GC)
				swim_member_delete(swim, m);
			break;
		default:
			unreachable();
		}
		swim_send_ping(swim, &m->ping_task, &m->addr);
	}
}

/**
 * Update member's UUID if it is changed. On UUID change the
 * member is reinserted into the members table with a new UUID.
 * @retval 0 Success.
 * @retval -1 Error. Out of memory or the new UUID is already in
 *         use.
 */
static int
swim_member_update_uuid(struct swim_member *member,
			const struct tt_uuid *new_uuid, struct swim *swim)
{
	if (tt_uuid_is_equal(new_uuid, &member->uuid))
		return 0;
	if (swim_find_member(swim, new_uuid) != NULL) {
		diag_set(SwimError, "duplicate UUID '%s'",
			 swim_uuid_str(new_uuid));
		return -1;
	}
	struct mh_swim_table_t *t = swim->members;
	struct tt_uuid old_uuid = member->uuid;
	member->uuid = *new_uuid;
	if (mh_swim_table_put(t, (const struct swim_member **) &member, NULL,
			      NULL) == mh_end(t)) {
		member->uuid = old_uuid;
		diag_set(OutOfMemory, sizeof(mh_int_t), "malloc", "node");
		return -1;
	}
	struct mh_swim_table_key key = {member->hash, &old_uuid};
	mh_swim_table_del(t, mh_swim_table_find(t, key, NULL), NULL);
	member->hash = swim_uuid_hash(new_uuid);
	return 0;
}

/** Update member's address.*/
static inline void
swim_member_update_addr(struct swim_member *member,
			const struct sockaddr_in *addr)
{
	if (addr->sin_port != member->addr.sin_port ||
	    addr->sin_addr.s_addr != member->addr.sin_addr.s_addr) {
		member->addr = *addr;
		swim_member_status_is_updated(member);
	}
}

/**
 * Update or create a member by its definition, received from a
 * remote instance.
 * @retval NULL Error.
 * @retval New member, or updated old member.
 */
static struct swim_member *
swim_update_member(struct swim *swim, const struct swim_member_def *def)
{
	struct swim_member *member = swim_find_member(swim, &def->uuid);
	if (member == NULL) {
		if (def->status == MEMBER_DEAD) {
			/*
			 * Do not 'resurrect' dead members to
			 * prevent 'ghost' members. Ghost member
			 * is a one declared as dead, sent via
			 * anti-entropy, and removed from local
			 * members table, but then returned back
			 * from received anti-entropy, as again
			 * dead. Such dead members could 'live'
			 * forever.
			 */
			return NULL;
		}
		member = swim_member_new(swim, &def->addr, &def->uuid,
					 def->status, def->incarnation);
		return member;
	}
	struct swim_member *self = swim->self;
	if (member != self) {
		if (def->incarnation >= member->incarnation) {
			swim_member_update_addr(member, &def->addr);
			swim_member_update_status(member, def->status,
						  def->incarnation, swim);
		}
		return member;
	}
	/*
	 * It is possible that other instances know a bigger
	 * incarnation of this instance - such thing happens when
	 * the instance restarts and loses its local incarnation
	 * number. It will be restored by receiving dissemination
	 * and anti-entropy messages about self.
	 */
	if (self->incarnation < def->incarnation)
		self->incarnation = def->incarnation;
	if (def->status != MEMBER_ALIVE &&
	    def->incarnation == self->incarnation) {
		/*
		 * In the cluster a gossip exists that this
		 * instance is not alive. Refute this information
		 * with a bigger incarnation.
		 */
		self->incarnation++;
	}
	return member;
}

/** Decode an anti-entropy message, update members table. */
static int
swim_process_anti_entropy(struct swim *swim, const char **pos, const char *end)
{
	const char *msg_pref = "invalid anti-entropy message:";
	uint32_t size;
	if (swim_decode_array(pos, end, &size, msg_pref, "root") != 0)
		return -1;
	for (uint64_t i = 0; i < size; ++i) {
		struct swim_member_def def;
		if (swim_member_def_decode(&def, pos, end, msg_pref) != 0)
			return -1;
		if (swim_update_member(swim, &def) == NULL) {
			/*
			 * Not a critical error. Other members
			 * still can be updated.
			 */
			diag_log();
		}
	}
	return 0;
}

/**
 * Decode a failure detection message. Schedule acks, process
 * acks.
 */
static int
swim_process_failure_detection(struct swim *swim, const char **pos,
			       const char *end, const struct sockaddr_in *src,
			       const struct tt_uuid *uuid)
{
	const char *msg_pref = "invalid failure detection message:";
	struct swim_failure_detection_def def;
	struct swim_member_def mdef;
	if (swim_failure_detection_def_decode(&def, pos, end, msg_pref) != 0)
		return -1;
	swim_member_def_create(&mdef);
	memset(&mdef, 0, sizeof(mdef));
	mdef.addr = *src;
	mdef.incarnation = def.incarnation;
	mdef.uuid = *uuid;
	struct swim_member *member = swim_update_member(swim, &mdef);
	if (member == NULL)
		return -1;

	switch (def.type) {
	case SWIM_FD_MSG_PING:
		swim_send_ack(swim, &member->ack_task, &member->addr);
		break;
	case SWIM_FD_MSG_ACK:
		if (def.incarnation >= member->incarnation) {
			/*
			 * Pings are reset above, in
			 * swim_update_member().
			 */
			assert(member->unacknowledged_pings == 0);
			if (! heap_node_is_stray(&member->in_heap_wait_ack)) {
				heap_wait_ack_delete(&swim->heap_wait_ack,
						     member);
			}
		}
		break;
	default:
		unreachable();
	}
	return 0;
}

/** Process a new message. */
static void
swim_on_input(struct swim_scheduler *scheduler, const char *pos,
	      const char *end, const struct sockaddr_in *src)
{
	const char *msg_pref = "invalid message:";
	struct swim *swim = swim_by_scheduler(scheduler);
	struct tt_uuid uuid;
	uint32_t size;
	if (swim_decode_map(&pos, end, &size, msg_pref, "root") != 0)
		goto error;
	if (size == 0) {
		diag_set(SwimError, "%s body can not be empty", msg_pref);
		goto error;
	}
	uint64_t key;
	if (swim_decode_uint(&pos, end, &key, msg_pref, "a key") != 0)
		goto error;
	if (key != SWIM_SRC_UUID) {
		diag_set(SwimError, "%s first key should be source UUID",
			 msg_pref);
		goto error;
	}
	if (swim_decode_uuid(&uuid, &pos, end, msg_pref, "source uuid") != 0)
		goto error;
	--size;
	for (uint32_t i = 0; i < size; ++i) {
		if (swim_decode_uint(&pos, end, &key, msg_pref, "a key") != 0)
			goto error;
		switch(key) {
		case SWIM_ANTI_ENTROPY:
			say_verbose("SWIM %d: process anti-entropy",
				    swim_fd(swim));
			if (swim_process_anti_entropy(swim, &pos, end) != 0)
				goto error;
			break;
		case SWIM_FAILURE_DETECTION:
			say_verbose("SWIM %d: process failure detection",
				    swim_fd(swim));
			if (swim_process_failure_detection(swim, &pos, end,
							   src, &uuid) != 0)
				goto error;
			break;
		default:
			diag_set(SwimError, "%s unexpected key", msg_pref);
			goto error;
		}
	}
	return;
error:
	diag_log();
}

struct swim *
swim_new(void)
{
	struct swim *swim = (struct swim *) calloc(1, sizeof(*swim));
	if (swim == NULL) {
		diag_set(OutOfMemory, sizeof(*swim), "calloc", "swim");
		return NULL;
	}
	swim->members = mh_swim_table_new();
	if (swim->members == NULL) {
		free(swim);
		diag_set(OutOfMemory, sizeof(*swim->members),
			 "mh_swim_table_new", "members");
		return NULL;
	}
	rlist_create(&swim->queue_round);
	swim_ev_init(&swim->round_tick, swim_round_step_begin);
	swim_ev_periodic_set(&swim->round_tick, 0, HEARTBEAT_RATE_DEFAULT,
			     NULL);
	swim->round_tick.data = (void *) swim;
	swim_task_create(&swim->round_step_task, swim_round_step_complete,
			 NULL);
	swim_scheduler_create(&swim->scheduler, swim_on_input);

	/* Failure detection component. */
	heap_wait_ack_create(&swim->heap_wait_ack);
	swim_ev_init(&swim->wait_ack_tick, swim_check_acks);
	swim_ev_periodic_set(&swim->wait_ack_tick, 0, ACK_TIMEOUT_DEFAULT, NULL);
	swim->wait_ack_tick.data = (void *) swim;
	return swim;
}

/**
 * Parse URI, filter out everything but IP addresses and ports,
 * and fill a struct sockaddr_in.
 */
static inline int
swim_uri_to_addr(const char *uri, struct sockaddr_in *addr,
		 const char *msg_pref)
{
	struct sockaddr_storage storage;
	if (sio_uri_to_addr(uri, (struct sockaddr *) &storage) != 0)
		return -1;
	if (storage.ss_family != AF_INET) {
		diag_set(IllegalParams, "%s only IP sockets are supported",
			 msg_pref);
		return -1;
	}
	*addr = *((struct sockaddr_in *) &storage);
	return 0;
}

int
swim_cfg(struct swim *swim, const char *uri, double heartbeat_rate,
	 double ack_timeout, const struct tt_uuid *uuid)
{
	const char *msg_pref = "swim.cfg:";
	struct sockaddr_in addr;
	if (uri != NULL && swim_uri_to_addr(uri, &addr, msg_pref) != 0)
		return -1;
	bool is_first_cfg = swim->self == NULL;
	if (is_first_cfg) {
		if (uuid == NULL || tt_uuid_is_nil(uuid) || uri == NULL) {
			diag_set(SwimError, "%s UUID and URI are mandatory in "\
				 "a first config", msg_pref);
			return -1;
		}
		swim->self = swim_member_new(swim, &addr, uuid, MEMBER_ALIVE,
					     0);
		if (swim->self == NULL)
			return -1;
	} else if (uuid == NULL || tt_uuid_is_nil(uuid)) {
		uuid = &swim->self->uuid;
	} else if (! tt_uuid_is_equal(uuid, &swim->self->uuid)) {
		if (swim_find_member(swim, uuid) != NULL) {
			diag_set(SwimError, "%s a member with such UUID "\
				 "already exists", msg_pref);
			return -1;
		}
		/*
		 * Reserve one cell for reinsertion of self with a
		 * new UUID. Reserve is necessary for atomic
		 * reconfiguration. Without reservation it is
		 * possible that the instance is bound to a new
		 * URI, but failed to update UUID due to memory
		 * issues.
		 */
		if (mh_swim_table_reserve(swim->members,
					  mh_size(swim->members) + 1,
					  NULL) != 0) {
			diag_set(OutOfMemory, sizeof(mh_int_t), "malloc",
				 "node");
			return -1;
		}

	}
	if (uri != NULL && swim_scheduler_bind(&swim->scheduler, &addr) != 0) {
		if (is_first_cfg) {
			swim_member_delete(swim, swim->self);
			swim->self = NULL;
		}
		return -1;
	}
	if (swim->round_tick.interval != heartbeat_rate && heartbeat_rate > 0) {
		swim_ev_periodic_set(&swim->round_tick, 0, heartbeat_rate,
				     NULL);
	}

	if (swim->wait_ack_tick.interval != ack_timeout && ack_timeout > 0) {
		swim_ev_periodic_set(&swim->wait_ack_tick, 0, ack_timeout,
				     NULL);
	}

	swim_ev_periodic_start(loop(), &swim->round_tick);
	swim_ev_periodic_start(loop(), &swim->wait_ack_tick);

	if (! is_first_cfg) {
		swim_member_update_addr(swim->self, &addr);
		int rc = swim_member_update_uuid(swim->self, uuid, swim);
		/* Reserved above. */
		assert(rc == 0);
		(void) rc;
	}
	return 0;
}

/**
 * Check if a SWIM instance is not configured, and if so - set an
 * error in a diagnostics area.
 */
static inline int
swim_check_is_configured(const struct swim *swim, const char *msg_pref)
{
	if (swim->self != NULL)
		return 0;
	diag_set(SwimError, "%s the instance is not configured", msg_pref);
	return -1;
}

int
swim_add_member(struct swim *swim, const char *uri, const struct tt_uuid *uuid)
{
	const char *msg_pref = "swim.add_member:";
	if (swim_check_is_configured(swim, msg_pref) != 0)
		return -1;
	struct sockaddr_in addr;
	if (swim_uri_to_addr(uri, &addr, msg_pref) != 0)
		return -1;
	struct swim_member *member = swim_find_member(swim, uuid);
	if (member == NULL) {
		member = swim_member_new(swim, &addr, uuid, MEMBER_ALIVE, 0);
		return member == NULL ? -1 : 0;
	}
	diag_set(SwimError, "%s a member with such UUID already exists",
		 msg_pref);
	return 1;
}

int
swim_remove_member(struct swim *swim, const struct tt_uuid *uuid)
{
	const char *msg_pref = "swim.remove_member:";
	if (swim_check_is_configured(swim, msg_pref) != 0)
		return -1;
	struct swim_member *member = swim_find_member(swim, uuid);
	if (member == NULL)
		return 0;
	if (member == swim->self) {
		diag_set(SwimError, "%s can not remove self", msg_pref);
		return -1;
	}
	swim_member_delete(swim, member);
	return 0;
}

int
swim_probe_member(struct swim *swim, const char *uri)
{
	const char *msg_pref = "swim.probe_member:";
	if (swim_check_is_configured(swim, msg_pref) != 0)
		return -1;
	struct sockaddr_in addr;
	if (swim_uri_to_addr(uri, &addr, msg_pref) != 0)
		return -1;
	struct swim_task *t = swim_task_new(swim_task_delete_cb,
					    swim_task_delete_cb);
	if (t == NULL)
		return -1;
	swim_send_ping(swim, t, &addr);
	return 0;
}

void
swim_info(struct swim *swim, struct info_handler *info)
{
	info_begin(info);
	for (mh_int_t node = mh_first(swim->members),
	     end = mh_end(swim->members); node != end;
	     node = mh_next(swim->members, node)) {
		struct swim_member *m =
			*mh_swim_table_node(swim->members, node);
		info_table_begin(info,
				 sio_strfaddr((struct sockaddr *) &m->addr,
					      sizeof(m->addr)));
		info_append_str(info, "status",
				swim_member_status_strs[m->status]);
		info_append_str(info, "uuid", swim_uuid_str(&m->uuid));
		info_append_int(info, "incarnation", (int64_t) m->incarnation);
		info_table_end(info);
	}
	info_end(info);
}

void
swim_delete(struct swim *swim)
{
	swim_scheduler_destroy(&swim->scheduler);
	swim_ev_periodic_stop(loop(), &swim->round_tick);
	swim_ev_periodic_stop(loop(), &swim->wait_ack_tick);
	swim_task_destroy(&swim->round_step_task);
	mh_int_t node = mh_first(swim->members);
	while (node != mh_end(swim->members)) {
		struct swim_member *m =
			*mh_swim_table_node(swim->members, node);
		swim_member_delete(swim, m);
		node = mh_first(swim->members);
	}
	heap_wait_ack_destroy(&swim->heap_wait_ack);
	mh_swim_table_delete(swim->members);
}
