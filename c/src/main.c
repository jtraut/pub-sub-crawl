#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>
#include "msg_queue.h"
#include "pubsub.h"

void* producer_routine(void* arg) {
    msg_queue_t *queue = (msg_queue_t *)arg;
    msg_item_t item;
    // Infinite loop to produce messages every second
    while (1) {
        // Create a message (example: incrementing a counter)
        static int counter = 0;
        item.id = (uint32_t)(++counter);
        item.payload = (void *)(intptr_t)counter;
        item.length = sizeof(int);

        // Enqueue the message
        if (!msg_queue_enqueue(queue, &item)) {
            fprintf(stderr, "Failed to enqueue message\n");
            break;
        }

        // Wait for a second before publishing the next message
        sleep(1);
    }
    return NULL;
}

// Define the consumer routine
void* consumer_routine(void* arg) {
    msg_queue_t *queue = (msg_queue_t *)arg;
    // Infinite loop to dequeue messages and process them
    while (1) {
        msg_item_t item;
        if (!msg_queue_dequeue(queue, &item)) {
            fprintf(stderr, "Failed to dequeue message\n");
            break;
        }

        // Process the message (example: print it)
        printf("Received message: %d\n", (int)(intptr_t)item.payload);
    }
    return NULL;
}

int main(void) {
    // Initial minimal first pass; no networking yet...
    pthread_t producer_thread, consumer_thread;

    msg_queue_t *queue = msg_queue_create(0);

    // Spawn a producer thread that publishes messages to a topic every second
    if (pthread_create(&producer_thread, NULL, producer_routine, queue) != 0) {
        fprintf(stderr, "Failed to create producer thread\n");
        return 1;
    }

    // Spawn a consumer thread that subscribes to the topic and dequeues messages
    if (pthread_create(&consumer_thread, NULL, consumer_routine, queue) != 0) {
        fprintf(stderr, "Failed to create consumer thread\n");
        return 1;
    }

    // TODO: wire this through pubsub instead of a raw shared queue

    pthread_join(producer_thread, NULL);
    pthread_join(consumer_thread, NULL);

    msg_queue_destroy(queue);
    return 0;
}