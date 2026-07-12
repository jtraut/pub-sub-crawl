#ifndef WORKER_POOL_H
#define WORKER_POOL_H

#include "msg_queue.h"
#include "pubsub.h"

#ifdef __cplusplus
extern "C" {
#endif

#define WORKER_POOL_DEFAULT_SIZE 3

// Topic names, shared with whatever publishes msg_type_t-tagged items onto
// the ingest queue, so the string only needs to be right in one place.
#define TOPIC_COUNT "count_topic"
#define TOPIC_STRING "string_topic"

typedef struct worker_pool worker_pool_t;

// Spawns thread_count worker threads (0 falls back to
// WORKER_POOL_DEFAULT_SIZE). Each worker pulls raw items off ingest_queue,
// does lightweight processing, and republishes into pubsub_ctx on the
// topic implied by the item's type. ingest_queue and pubsub_ctx are
// caller-owned and must outlive the pool. Returns NULL on allocation
// failure or if no worker thread could be started.
worker_pool_t *worker_pool_create(size_t thread_count, msg_queue_t *ingest_queue, pubsub_context_t *pubsub_ctx);

// Joins all worker threads and frees the pool. Workers block indefinitely
// on an empty ingest_queue, so callers should stop feeding it and unblock
// any waiting workers before calling this.
void worker_pool_destroy(worker_pool_t *pool);

#ifdef __cplusplus
}
#endif

#endif // WORKER_POOL_H
