#include "worker_pool.h"
#include "log.h"
#include <stdlib.h>
#include <pthread.h>

struct worker_pool {
    pthread_t *threads;
    size_t thread_count;
};

typedef struct {
    msg_queue_t *ingest_queue;
    pubsub_context_t *pubsub_ctx;
} worker_ctx_t;

static const char *topic_for_type(msg_type_t type) {
    switch (type) {
        case MSG_TYPE_COUNT:  return TOPIC_COUNT;
        case MSG_TYPE_STRING: return TOPIC_STRING;
        default:              return NULL;
    }
}

static void *worker_routine(void *arg) {
    worker_ctx_t *wctx = (worker_ctx_t *)arg;

    while (1) {
        msg_item_t item;
        if (!msg_queue_dequeue(wctx->ingest_queue, &item)) {
            break;
        }

        const char *topic = topic_for_type(item.type);
        if (!topic) {
            log_fprintf(stderr, "Worker: unknown message type %d, dropping\n", item.type);
            if (item.length > 0) {
                free(item.payload);
            }
            continue;
        }

        if (pubsub_publish(wctx->pubsub_ctx, topic, &item) < 0) {
            log_fprintf(stderr, "Worker: failed to publish to %s\n", topic);
        }

        // pubsub_publish deep-copies payload (length > 0) into each
        // subscriber's own queue before returning, so this worker's copy
        // is no longer needed once it's back.
        if (item.length > 0) {
            free(item.payload);
        }
    }

    free(wctx);
    return NULL;
}

worker_pool_t *worker_pool_create(size_t thread_count, msg_queue_t *ingest_queue, pubsub_context_t *pubsub_ctx) {
    if (!ingest_queue || !pubsub_ctx) {
        return NULL;
    }
    if (thread_count == 0) {
        thread_count = WORKER_POOL_DEFAULT_SIZE;
    }

    worker_pool_t *pool = malloc(sizeof(worker_pool_t));
    if (!pool) {
        return NULL;
    }

    pool->threads = malloc(sizeof(pthread_t) * thread_count);
    if (!pool->threads) {
        free(pool);
        return NULL;
    }
    pool->thread_count = 0;

    for (size_t i = 0; i < thread_count; i++) {
        worker_ctx_t *wctx = malloc(sizeof(worker_ctx_t));
        if (!wctx) {
            break;
        }
        wctx->ingest_queue = ingest_queue;
        wctx->pubsub_ctx = pubsub_ctx;

        if (pthread_create(&pool->threads[i], NULL, worker_routine, wctx) != 0) {
            free(wctx);
            break;
        }
        pool->thread_count++;
    }

    if (pool->thread_count == 0) {
        free(pool->threads);
        free(pool);
        return NULL;
    }

    return pool;
}

void worker_pool_destroy(worker_pool_t *pool) {
    if (!pool) {
        return;
    }

    for (size_t i = 0; i < pool->thread_count; i++) {
        pthread_join(pool->threads[i], NULL);
    }
    free(pool->threads);
    free(pool);
}
