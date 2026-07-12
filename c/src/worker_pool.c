#include "worker_pool.h"
#include "radio_msg.h"
#include "log.h"
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>

struct worker_pool {
    pthread_t *threads;
    size_t thread_count;
};

typedef struct {
    msg_queue_t *ingest_queue;
    pubsub_context_t *pubsub_ctx;
} worker_ctx_t;

// Sequence numbers are shared across every worker (and across both
// telemetry/alerts), so multiple workers draining the same ingest queue
// concurrently can hand out seq/ts in an order that doesn't match arrival
// order on the ingest queue -- that's expected, not a bug, given a flat
// worker pool with no per-topic ordering guarantee.
static pthread_mutex_t seq_mutex = PTHREAD_MUTEX_INITIALIZER;
static uint32_t next_seq = 1;

static uint32_t next_sequence(void) {
    pthread_mutex_lock(&seq_mutex);
    uint32_t seq = next_seq++;
    pthread_mutex_unlock(&seq_mutex);
    return seq;
}

static void *worker_routine(void *arg) {
    worker_ctx_t *wctx = (worker_ctx_t *)arg;

    while (1) {
        msg_item_t item;
        if (!msg_queue_dequeue(wctx->ingest_queue, &item)) {
            break;
        }

        switch (item.type) {
            case MSG_TYPE_TELEMETRY: {
                const telemetry_raw_t *raw = (const telemetry_raw_t *)item.payload;
                telemetry_msg_t msg = {
                    .seq = next_sequence(),
                    .ts = (int64_t)time(NULL),
                    .battery_pct = raw->battery_pct,
                    .rssi_dbm = raw->rssi_dbm,
                };
                msg_item_t pub_item = { .id = msg.seq, .type = item.type, .payload = &msg, .length = sizeof(msg) };
                if (pubsub_publish(wctx->pubsub_ctx, TOPIC_TELEMETRY, &pub_item) < 0) {
                    log_fprintf(stderr, "Worker: failed to publish to %s\n", TOPIC_TELEMETRY);
                }
                break;
            }
            case MSG_TYPE_ALERT: {
                const alert_raw_t *raw = (const alert_raw_t *)item.payload;
                alert_msg_t msg = {
                    .seq = next_sequence(),
                    .ts = (int64_t)time(NULL),
                    .antenna = raw->antenna,
                };
                memcpy(msg.code, raw->code, sizeof(msg.code));
                msg_item_t pub_item = { .id = msg.seq, .type = item.type, .payload = &msg, .length = sizeof(msg) };
                if (pubsub_publish(wctx->pubsub_ctx, TOPIC_ALERTS, &pub_item) < 0) {
                    log_fprintf(stderr, "Worker: failed to publish to %s\n", TOPIC_ALERTS);
                }
                break;
            }
            default:
                log_fprintf(stderr, "Worker: unknown message type %d, dropping\n", item.type);
                break;
        }

        // item.payload is always the producer's heap-allocated raw struct
        // (telemetry_raw_t/alert_raw_t); pubsub_publish deep-copies the
        // enriched msg onto each subscriber's own queue, so this copy is
        // done once the switch above returns.
        free(item.payload);
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
