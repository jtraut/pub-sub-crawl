#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>
#include "msg_queue.h"
#include "pubsub.h"
#include "worker_pool.h"
#include "log.h"

void* producer_routine(void* arg) {
    // Producer no longer talks to pubsub directly: it just drops raw
    // items on the shared ingest queue, and the worker pool routes them.
    msg_queue_t *ingest_queue = (msg_queue_t *)arg;
    msg_item_t item;
    static int counter = 0;
    // Infinite loop to produce messages every second
    while (1) {
        // Create a message (example: incrementing a counter)
        item.id = (uint32_t)(++counter);
        item.type = MSG_TYPE_COUNT;
        item.payload = (void *)(intptr_t)counter;
        item.length = 0; // int-in-pointer trick, so there's nothing to copy

        if (!msg_queue_enqueue(ingest_queue, &item)) {
            log_fprintf(stderr, "Failed to enqueue count message\n");
            break;
        }

        // Use multiple topics, with different data types, this time a string.
        // The ingest queue does a shallow struct copy (see msg_queue_enqueue),
        // so the payload has to be heap-allocated here, not a stack buffer -
        // a worker thread will read it later, after this stack frame's gone
        // around the loop again. The worker frees it once it's published.
        item.id = (uint32_t)(counter + 1000);
        item.type = MSG_TYPE_STRING;
        char msg_buf[32];
        snprintf(msg_buf, sizeof(msg_buf), "Hello, world! %d", counter);
        size_t msg_len = strlen(msg_buf) + 1;
        char *heap_msg = malloc(msg_len);
        if (!heap_msg) {
            log_fprintf(stderr, "Failed to allocate string message\n");
            break;
        }
        memcpy(heap_msg, msg_buf, msg_len);
        item.payload = heap_msg;
        item.length = msg_len;

        if (!msg_queue_enqueue(ingest_queue, &item)) {
            log_fprintf(stderr, "Failed to enqueue string message\n");
            free(heap_msg);
            break;
        }

        // Wait for a second before publishing the next message
        sleep(1);
    }
    return NULL;
}

// Define the consumer routine
void* consumer_routine(void* arg) {
    pubsub_context_t *ctx = (pubsub_context_t *)arg;
    // Initialize a queue per topic subscription...? For now at least.
    msg_queue_t *queue = msg_queue_create(0);
    // log_printf("Creating consumer queue\n");
    if (pubsub_subscribe(ctx, TOPIC_COUNT, queue)) {
        log_fprintf(stderr, "Failed to subscribe to topic\n");
        return NULL;
    }
    msg_queue_t *string_queue = msg_queue_create(0);
    if (pubsub_subscribe(ctx, TOPIC_STRING, string_queue)) {
        log_fprintf(stderr, "Failed to subscribe to string topic\n");
        return NULL;
    }
    // Infinite loop to dequeue messages and process them
    while (1) {
        msg_item_t item;
        if (!msg_queue_dequeue(queue, &item)) {
            log_fprintf(stderr, "Failed to dequeue message\n");
            break;
        }
        log_printf("Received message: %d\n", (int)(intptr_t)item.payload);
        // int-in-pointer trick: no need to free the payload, it's just an int in disguise.
        if (!msg_queue_dequeue(string_queue, &item)) {
            log_fprintf(stderr, "Failed to dequeue string message\n");
            break;
        }
        // Process the message (example: print it)
        log_printf("Received string message: %s\n", (char *)item.payload);
        // pubsub_publish deep-copies string payloads per subscriber (see
        // pubsub.h), so this subscriber owns it and must free it.
        free(item.payload);
    }
    return NULL;
}

int main(void) {
    // Initial minimal first pass; no networking yet...
    pthread_t producer_thread, consumer_thread;

    // Pubsub will manage the queues internally, so we just need a context to pass around
    pubsub_context_t *ctx = pubsub_create();

    // Shared ingest queue: the producer drops raw items here, and the
    // worker pool is what actually routes them into pubsub.
    msg_queue_t *ingest_queue = msg_queue_create(0);

    worker_pool_t *workers = worker_pool_create(WORKER_POOL_DEFAULT_SIZE, ingest_queue, ctx);
    if (!workers) {
        log_fprintf(stderr, "Failed to create worker pool\n");
        return 1;
    }

    // Spawn a producer thread that enqueues raw messages every second
    if (pthread_create(&producer_thread, NULL, producer_routine, ingest_queue) != 0) {
        log_fprintf(stderr, "Failed to create producer thread\n");
        return 1;
    }

    // Spawn a consumer thread that subscribes to the topic and dequeues messages
    if (pthread_create(&consumer_thread, NULL, consumer_routine, ctx) != 0) {
        log_fprintf(stderr, "Failed to create consumer thread\n");
        return 1;
    }

    pthread_join(producer_thread, NULL);
    pthread_join(consumer_thread, NULL);
    worker_pool_destroy(workers);

    pubsub_destroy(ctx);
    return 0;
}