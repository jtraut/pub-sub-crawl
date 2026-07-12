#include "pubsub.h"
#include "log.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

typedef struct {
    char name[PUBSUB_TOPIC_NAME_MAX];
    msg_queue_t *subscribers[PUBSUB_MAX_SUBSCRIBERS_PER_TOPIC];
    size_t subscriber_count;
} topic_entry_t;

struct pubsub_context {
    pthread_mutex_t mutex;
    topic_entry_t topics[PUBSUB_MAX_TOPICS];
    size_t topic_count;
};

// Caller must hold ctx->mutex.
static topic_entry_t *find_topic(pubsub_context_t *ctx, const char *topic) {
    log_printf("Searching for topic: %s\n", topic);
    for (size_t i = 0; i < ctx->topic_count; i++) {
        if (strcmp(ctx->topics[i].name, topic) == 0) {
            return &ctx->topics[i];
        }
    }
    return NULL;
}

// Caller must hold ctx->mutex. Returns NULL if the topic registry is full.
static topic_entry_t *find_or_create_topic(pubsub_context_t *ctx, const char *topic) {
    topic_entry_t *entry = find_topic(ctx, topic);
    if (entry) {
        return entry;
    }

    if (ctx->topic_count >= PUBSUB_MAX_TOPICS) {
        return NULL;
    }
    log_printf("Creating new topic: %s\n", topic);
    entry = &ctx->topics[ctx->topic_count++];
    strncpy(entry->name, topic, PUBSUB_TOPIC_NAME_MAX - 1);
    entry->name[PUBSUB_TOPIC_NAME_MAX - 1] = '\0';
    entry->subscriber_count = 0;
    return entry;
}

pubsub_context_t *pubsub_create(void) {
    log_printf("Creating pubsub context\n");
    pubsub_context_t *ctx = (pubsub_context_t *)malloc(sizeof(pubsub_context_t));
    if (!ctx) {
        return NULL;
    }

    if (pthread_mutex_init(&ctx->mutex, NULL) != 0) {
        free(ctx);
        return NULL;
    }

    ctx->topic_count = 0;
    return ctx;
}

void pubsub_destroy(pubsub_context_t *ctx) {
    if (!ctx) {
        return;
    }

    // Subscriber queues are caller-owned; pubsub never frees them.
    pthread_mutex_destroy(&ctx->mutex);
    free(ctx);
}

int pubsub_subscribe(pubsub_context_t *ctx, const char *topic, msg_queue_t *queue) {
    if (!ctx || !topic || !queue || strlen(topic) >= PUBSUB_TOPIC_NAME_MAX) {
        return -1;
    }
    log_printf("Subscribing to topic: %s\n", topic);

    pthread_mutex_lock(&ctx->mutex);

    topic_entry_t *entry = find_or_create_topic(ctx, topic);
    if (!entry) {
        pthread_mutex_unlock(&ctx->mutex);
        return -1;
    }

    for (size_t i = 0; i < entry->subscriber_count; i++) {
        if (entry->subscribers[i] == queue) {
            pthread_mutex_unlock(&ctx->mutex);
            return 0;
        }
    }

    if (entry->subscriber_count >= PUBSUB_MAX_SUBSCRIBERS_PER_TOPIC) {
        pthread_mutex_unlock(&ctx->mutex);
        return -1;
    }

    entry->subscribers[entry->subscriber_count++] = queue;

    pthread_mutex_unlock(&ctx->mutex);
    return 0;
}

int pubsub_unsubscribe(pubsub_context_t *ctx, const char *topic, msg_queue_t *queue) {
    if (!ctx || !topic || !queue) {
        return -1;
    }

    pthread_mutex_lock(&ctx->mutex);

    topic_entry_t *entry = find_topic(ctx, topic);
    if (!entry) {
        pthread_mutex_unlock(&ctx->mutex);
        return -1;
    }

    for (size_t i = 0; i < entry->subscriber_count; i++) {
        if (entry->subscribers[i] == queue) {
            // Order doesn't matter: swap the last subscriber into this slot.
            entry->subscribers[i] = entry->subscribers[entry->subscriber_count - 1];
            entry->subscriber_count--;
            pthread_mutex_unlock(&ctx->mutex);
            return 0;
        }
    }

    pthread_mutex_unlock(&ctx->mutex);
    return -1;
}

int pubsub_publish(pubsub_context_t *ctx, const char *topic, const msg_item_t *item) {
    if (!ctx || !topic || !item) {
        return -1;
    }

    pthread_mutex_lock(&ctx->mutex);
    // Note this was find_topic at first, so maybe switch back if things break
    // But I feel like this is safer in case the publish comes before the subscribe
    topic_entry_t *entry = find_or_create_topic(ctx, topic);
    log_printf("Publishing to topic: %s\n", topic);
    if (!entry) {
        pthread_mutex_unlock(&ctx->mutex);
        return 0;
    }
    log_printf("Topic exists: %s\n", topic);
    int delivered = 0;
    for (size_t i = 0; i < entry->subscriber_count; i++) {
        msg_item_t clone = *item;
        log_printf("Attempting to deliver to subscriber %zu\n", i);

        if (item->payload && item->length > 0) {
            void *payload_copy = malloc(item->length);
            if (!payload_copy) {
                // Out of memory: skip this one subscriber, keep fanning out to the rest.
                continue;
            }
            memcpy(payload_copy, item->payload, item->length);
            clone.payload = payload_copy;
        }

        if (msg_queue_try_enqueue(entry->subscribers[i], &clone)) {
            delivered++;
        } else if (clone.payload != item->payload) {
            // Subscriber queue is full; drop the message and free the clone we just made.
            free(clone.payload);
        }
    }
    log_printf("Delivered to %d subscribers\n", delivered);

    pthread_mutex_unlock(&ctx->mutex);
    return delivered;
}
