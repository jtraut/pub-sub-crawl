#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>
#include "msg_queue.h"
#include "pubsub.h"
#include "log.h"

void* producer_routine(void* arg) {
    // Get the pubsub context
    pubsub_context_t *ctx = (pubsub_context_t *)arg;
    // No direct queue access required, publish will take care of that internally
    msg_item_t item;
    static int counter = 0;
    // Infinite loop to produce messages every second
    while (1) {
        // Create a message (example: incrementing a counter)
        // static int counter = 0;
        item.id = (uint32_t)(++counter);
        item.payload = (void *)(intptr_t)counter;
        item.length = 0; //sizeof(int); // Using the int-in-pointer trick, so length is actually 0

        // Enqueue the message
        if (pubsub_publish(ctx, "count_topic", &item) < 0) {
            log_fprintf(stderr, "Failed to publish message\n");
            break;
        }

        // Use multiple topics, with different data types, this time a string
        item.id = (uint32_t)(counter + 1000);
        char msg_buf[32];
        snprintf(msg_buf, sizeof(msg_buf), "Hello, world! %d", counter);
        item.payload = msg_buf;
        item.length = strlen(msg_buf) + 1;

        if (pubsub_publish(ctx, "string_topic", &item) < 0) {
            log_fprintf(stderr, "Failed to publish string message\n");
            break;
        }

        // Wait for a second before publishing the next message
        sleep(1);
        // log_printf("Published message: %d, ready for next\n", counter);
    }
    return NULL;
}

// Define the consumer routine
void* consumer_routine(void* arg) {
    pubsub_context_t *ctx = (pubsub_context_t *)arg;
    // Initialize a queue per topic subscription...? For now at least.
    msg_queue_t *queue = msg_queue_create(0);
    // log_printf("Creating consumer queue\n");
    if (pubsub_subscribe(ctx, "count_topic", queue)) {
        log_fprintf(stderr, "Failed to subscribe to topic\n");
        return NULL;
    }
    msg_queue_t *string_queue = msg_queue_create(0);
    if (pubsub_subscribe(ctx, "string_topic", string_queue)) {
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
        if (!msg_queue_dequeue(string_queue, &item)) {
            log_fprintf(stderr, "Failed to dequeue string message\n");
            break;
        }
        // Process the message (example: print it)
        log_printf("Received string message: %s\n", (char *)item.payload);
    }
    return NULL;
}

// TODO: seems like there's a race condition somewhere jumbling the printf outputs
// Added a custom log mutex wrapper to help.
int main(void) {
    // Initial minimal first pass; no networking yet...
    pthread_t producer_thread, consumer_thread;

    // Pubsub will manage the queues internally, so we just need a context to pass around
    pubsub_context_t *ctx = pubsub_create();

    // Spawn a producer thread that publishes messages to a topic every second
    if (pthread_create(&producer_thread, NULL, producer_routine, ctx) != 0) {
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

    pubsub_destroy(ctx);
    return 0;
}