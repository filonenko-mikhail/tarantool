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
#include "swim_test_transport.h"
#include "swim/swim_transport.h"
#include "swim/swim_io.h"
#include "fiber.h"
#include <errno.h>

enum {
	FAKE_FD_BASE = 1000,
	FAKE_FD_NUMBER = 1000,
};


struct swim_test_packet {
	struct swim_packet base;
	struct sockaddr_in src;
	struct sockaddr_in dst;
	struct rlist in_queue;
};

static inline struct swim_test_packet *
swim_test_packet_new(const char *data, int size, const struct sockaddr_in *src,
		     const struct sockaddr_in *dst)
{
	struct swim_test_packet *p =
		(struct swim_test_packet *) malloc(sizeof(*p));
	assert(p != NULL);
	swim_packet_create(&p->base);
	rlist_create(&p->in_queue);
	p->src = *src;
	p->dst = *dst;
	char *body = swim_packet_alloc(&p->base, size);
	memcpy(body, data, size);
	return p;
}

struct swim_fd {
	bool is_opened;
	struct rlist recv_queue;
	struct rlist send_queue;
};

static inline int
swim_fd_open(struct swim_fd *fd)
{
	if (fd->is_opened) {
		errno = EADDRINUSE;
		diag_set(SocketError, "test_socket:1", "bind");
		return -1;
	}
	fd->is_opened = true;
	rlist_create(&fd->recv_queue);
	rlist_create(&fd->send_queue);
	return 0;
}

static inline void
swim_fd_close(struct swim_fd *fd)
{
	fd->is_opened = false;
	struct swim_test_packet *i, *tmp;
	rlist_foreach_entry_safe(i, &fd->recv_queue, in_queue, tmp)
		free(i);
	rlist_foreach_entry_safe(i, &fd->send_queue, in_queue, tmp)
		free(i);
}

static struct swim_fd swim_fd[FAKE_FD_NUMBER];

void
swim_test_transport_init(void)
{
	for (int i = 0; i < (int)lengthof(swim_fd); ++i) {
		swim_fd[i].is_opened = false;
		rlist_create(&swim_fd[i].recv_queue);
		rlist_create(&swim_fd[i].send_queue);
	}
}

void
swim_test_transport_free(void)
{
	struct swim_test_packet *p, *tmp;
	for (int i = 0; i < (int)lengthof(swim_fd); ++i)
		swim_fd_close(&swim_fd[i]);
}

ssize_t
swim_transport_send(struct swim_transport *transport, const void *data,
		    size_t size, const struct sockaddr *addr,
		    socklen_t addr_size)
{
	/*
	 * Create packet. Put into sending queue.
	 */
	(void) addr_size;
	assert(addr->sa_family == AF_INET);
	struct swim_test_packet *p =
		swim_test_packet_new(data, size, &transport->addr,
				     (const struct sockaddr_in *) addr);
	struct swim_fd *src = &swim_fd[transport->fd - FAKE_FD_BASE];
	rlist_add_tail_entry(&src->send_queue, p, in_queue);
	return size;
}

ssize_t
swim_transport_recv(struct swim_transport *transport, void *buffer, size_t size,
		    struct sockaddr *addr, socklen_t *addr_size)
{
	/*
	 * Pop a packet from a receving queue.
	 */
	struct swim_fd *dst = &swim_fd[transport->fd - FAKE_FD_BASE];
	struct swim_test_packet *p =
		rlist_shift_entry(&dst->recv_queue, struct swim_test_packet,
				  in_queue);
	*(struct sockaddr_in *) addr = p->src;
	*addr_size = sizeof(p->src);
	size_t copy_size = p->base.pos - p->base.buf;
	if (copy_size > size)
		copy_size = size;
	memcpy(buffer, p->base.buf, copy_size);
	free(p);
	return copy_size;
}

int
swim_transport_bind(struct swim_transport *transport,
		    const struct sockaddr *addr, socklen_t addr_len)
{
	assert(addr->sa_family == AF_INET);
	const struct sockaddr_in *new_addr = (const struct sockaddr_in *) addr;
	int new_fd = ntohs(new_addr->sin_port) + FAKE_FD_BASE;
	int old_fd = transport->fd;
	if (old_fd == new_fd)
		return 0;
	if (swim_fd_open(&swim_fd[new_fd - FAKE_FD_BASE]) != 0)
		return -1;
	transport->fd = new_fd;
	transport->addr = *new_addr;
	if (old_fd != -1)
		swim_fd_close(&swim_fd[old_fd - FAKE_FD_BASE]);
	return 0;
}

void
swim_transport_destroy(struct swim_transport *transport)
{
	if (transport->fd != -1)
		swim_fd_close(&swim_fd[transport->fd - FAKE_FD_BASE]);
}

void
swim_transport_create(struct swim_transport *transport)
{
	transport->fd = -1;
	memset(&transport->addr, 0, sizeof(transport->addr));
}

static inline void
swim_fd_send_packets(struct swim_fd *fd)
{
	if (rlist_empty(&fd->send_queue))
		return;
	struct swim_test_packet *p =
		rlist_shift_entry(&fd->send_queue, struct swim_test_packet,
				  in_queue);
	int dst_i = ntohs(p->dst.sin_port);
	rlist_add_tail_entry(&swim_fd[dst_i].recv_queue, p, in_queue);
}

void
swim_transport_do_loop_step(struct ev_loop *loop)
{
	/*
	 * Go through all sockets and emit EV_WRITE for each. Emit
	 * EV_READ for the ones, who has data.
	 */
	for (int i = 0, fd = FAKE_FD_BASE; i < FAKE_FD_NUMBER; ++i, ++fd) {
		if (swim_fd[i].is_opened) {
			swim_fd_send_packets(&swim_fd[i]);
			ev_feed_fd_event(loop, fd, EV_WRITE);
		}
	}
	for (int i = 0, fd = FAKE_FD_BASE; i < FAKE_FD_NUMBER; ++i, ++fd) {
		if (swim_fd[i].is_opened &&
		    !rlist_empty(&swim_fd[i].recv_queue))
			ev_feed_fd_event(loop, fd, EV_READ);
	}
}
