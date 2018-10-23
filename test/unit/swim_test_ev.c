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
#include "swim_test_ev.h"
#include "swim_test_transport.h"
#include "trivia/util.h"
#include "swim/swim_ev.h"
#include "tarantool_ev.h"
#define HEAP_FORWARD_DECLARATION
#include "salad/heap.h"
#include "assoc.h"
#include "say.h"
#include <stdbool.h>

static double watch = 0;

struct swim_test_watcher {
	struct ev_watcher *base;
	bool is_periodic;
	int revents_mask;
	struct rlist events;
	int refs;
};

struct swim_test_event {
	struct swim_test_watcher *watcher;
	double deadline;
	struct heap_node in_events_heap;
	struct rlist in_queue_watcher;
};

#define HEAP_NAME events_heap
#define HEAP_LESS(h, e1, e2) ((e1)->deadline < (e2)->deadline)
#define heap_value_t struct swim_test_event
#define heap_value_attr in_events_heap
#include "salad/heap.h"

static heap_t events_heap;

static struct mh_i64ptr_t *watchers = NULL;

static void
swim_test_event_new(struct swim_test_watcher *watcher, double delay)
{
	/*
	 * Create event. Push into the queue, and the watcher's
	 * list.
	 */
	struct swim_test_event *tmp, *e =
		(struct swim_test_event *) malloc(sizeof(*e));
	assert(e != NULL);
	e->watcher = watcher;
	e->deadline = swim_time() + delay;
	events_heap_insert(&events_heap, e);
	rlist_add_tail_entry(&watcher->events, e, in_queue_watcher);
}

static inline void
swim_test_event_delete(struct swim_test_event *e)
{
	/*
	 * Remove event from the queue, the list.
	 */
	events_heap_delete(&events_heap, e);
	rlist_del_entry(e, in_queue_watcher);
	free(e);
}

static inline struct swim_test_watcher *
swim_test_watcher_new(struct ev_watcher *base, bool is_periodic,
		      int revents_mask)
{
	/*
	 * Create watcher, store into the watchers hash.
	 */
	struct swim_test_watcher *w =
		(struct swim_test_watcher *) malloc(sizeof(*w));
	assert(w != NULL);
	w->base = base;
	w->is_periodic = is_periodic;
	w->revents_mask = revents_mask;
	w->refs = 1;
	rlist_create(&w->events);
	struct mh_i64ptr_node_t node = {(uint64_t) base, w};
	mh_int_t rc = mh_i64ptr_put(watchers, &node, NULL, NULL);
	(void) rc;
	assert(rc != mh_end(watchers));
	return w;
}

static inline void
swim_test_watcher_delete(struct swim_test_watcher *watcher)
{
	assert(watcher->refs == 0);
	/*
	 * Delete watcher's events. Delete the watcher.
	 */
	struct swim_test_event *e, *tmp;
	rlist_foreach_entry_safe(e, &watcher->events, in_queue_watcher, tmp)
		swim_test_event_delete(e);
	mh_int_t rc = mh_i64ptr_find(watchers, (uint64_t) watcher->base, NULL);
	assert(rc != mh_end(watchers));
	mh_i64ptr_del(watchers, rc, NULL);
	free(watcher);
}

static inline void
swim_test_watcher_ref(struct swim_test_watcher *watcher)
{
	watcher->refs++;
}

static inline void
swim_test_watcher_unref(struct swim_test_watcher *watcher)
{
	assert(watcher->refs > 0);
	if (--watcher->refs == 0)
		swim_test_watcher_delete(watcher);
}

static struct swim_test_watcher *
swim_test_watcher_by_ev(struct ev_watcher *watcher)
{
	mh_int_t rc = mh_i64ptr_find(watchers, (uint64_t) watcher, NULL);
	assert(rc != mh_end(watchers));
	return (struct swim_test_watcher *) mh_i64ptr_node(watchers, rc)->val;
}

double
swim_time(void)
{
	return watch;
}

void
swim_ev_periodic_start(struct ev_loop *loop, struct ev_periodic *base)
{
	/* Create the periodic watcher and one event. */
	struct swim_test_watcher *w =
		swim_test_watcher_new((struct ev_watcher *) base, true,
				      EV_PERIODIC);
	swim_test_event_new(w, base->interval);
}

void
swim_ev_periodic_stop(struct ev_loop *loop, struct ev_periodic *base)
{
	if (! ev_is_active(base))
		return;
	/*
	 * Delete the watcher and its events. Should be only one.
	 */
	struct swim_test_watcher *w =
		swim_test_watcher_by_ev((struct ev_watcher *) base);
	swim_test_watcher_unref(w);
}

void
swim_ev_io_start(struct ev_loop *loop, struct ev_io *io)
{
	/*
	 * Create a watcher. No events.
	 */
	swim_test_watcher_new((struct ev_watcher *) io, false,
			      io->events & (EV_READ | EV_WRITE));
	ev_io_start(loop, io);
}

void
swim_ev_io_stop(struct ev_loop *loop, struct ev_io *io)
{
	if (! ev_is_active(io))
		return;
	ev_io_stop(loop, io);
	/*
	 * Delete the watcher and its events.
	 */
	struct swim_test_watcher *w =
		swim_test_watcher_by_ev((struct ev_watcher *) io);
	swim_test_watcher_unref(w);
}

void
swim_do_loop_step(struct ev_loop *loop)
{
	/*
	 * Take next event. Update global watch. Execute its cb.
	 * Do one loop step for the transport.
	 */
	struct swim_test_event *e = events_heap_pop(&events_heap);
	if (e != NULL) {
		watch = e->deadline;
		swim_test_watcher_ref(e->watcher);
		ev_invoke(loop, e->watcher->base, e->watcher->revents_mask);
		int refs = e->watcher->refs;
		swim_test_watcher_unref(e->watcher);
		if (refs > 1 && e->watcher->is_periodic) {
			struct ev_periodic *p =
				(struct ev_periodic *) e->watcher->base;
			e->deadline = swim_time() + p->interval;
			events_heap_insert(&events_heap, e);
		} else if (refs > 1) {
			swim_test_event_delete(e);
		}
	}
	swim_transport_do_loop_step(loop);
	ev_invoke_pending(loop);
}

void
swim_test_ev_run_loop(struct ev_loop *loop)
{
	while (true)
		swim_do_loop_step(loop);
}

void
swim_test_ev_init(void)
{
	watchers = mh_i64ptr_new();
	assert(watchers != NULL);
	events_heap_create(&events_heap);
}

void
swim_test_ev_free(void)
{
	struct swim_test_event *e = events_heap_pop(&events_heap);
	if (e != NULL) {
		say_warn("SWIM events queue is not empty");
		do {
			swim_test_event_delete(e);
		} while ((e = events_heap_pop(&events_heap)) != NULL);
	}
	events_heap_destroy(&events_heap);
	mh_int_t rc = mh_first(watchers);
	while (rc != mh_end(watchers)) {
		swim_test_watcher_unref((struct swim_test_watcher *)
					mh_i64ptr_node(watchers, rc)->val);
		rc = mh_next(watchers, rc);
	}
	mh_i64ptr_delete(watchers);
}
