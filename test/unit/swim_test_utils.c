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
#include "swim_test_utils.h"
#include "swim/swim.h"
#include "tt_uuid.h"
#include "trivia/util.h"

struct swim_test_cluster {
	int size;
	struct swim *node[0];
};

struct swim_test_cluster *
swim_test_cluster_new(int size)
{
	struct swim_test_cluster *res;
	int bsize = sizeof(*res) + sizeof(res->node[0]) * size;
	res = (struct swim_test_cluster *) malloc(bsize);
	assert(res != NULL);
	res->size = size;
	struct tt_uuid uuid;
	memset(&uuid, 0, sizeof(uuid));
	char *uri = tt_static_buf();
	for (int i = 0; i < size; ++i) {
		res->node[i] = swim_new();
		assert(res->node[i] != NULL);
		sprintf(uri, "127.0.0.1:%d", i + 1);
		uuid.time_low = i + 1;
		int rc = swim_cfg(res->node[i], uri, -1, -1, &uuid);
		assert(rc == 0);
		(void) rc;
	}
	return res;
}

void
swim_test_cluster_delete(struct swim_test_cluster *cluster)
{
	for (int i = 0; i < cluster->size; ++i)
		swim_delete(cluster->node[i]);
	free(cluster);
}
