#ifndef PUBSUB_H
#define PUBSUB_H

#include "msg_queue.h"

#ifdef __cplusplus
extern "C" {
#endif

#define PUBSUB_MAX_TOPICS 8
#define PUBSUB_MAX_SUBSCRIBERS_PER_TOPIC 16
#define PUBSUB_TOPIC_NAME_MAX 32

typedef struct pubsub_context pubsub_context_t;

pubsub_context_t *pubsub_create(void);
void pubsub_destroy(pubsub_context_t *ctx);

// Registers queue as a subscriber of topic, creating the topic on first
// use. queue is caller-owned: pubsub never creates, drains, or destroys
// subscriber queues, it only holds pointers to them.
// Returns 0 on success (re-subscribing the same queue is a no-op success),
// -1 if ctx/topic/queue is NULL, the topic name is too long, the topic
// registry is full, or that topic's subscriber list is full.
int pubsub_subscribe(pubsub_context_t *ctx, const char *topic, msg_queue_t *queue);

// Removes queue from topic's subscriber list.
// Returns 0 on success, -1 if ctx/topic/queue is NULL or not subscribed.
int pubsub_unsubscribe(pubsub_context_t *ctx, const char *topic, msg_queue_t *queue);

// Fans out item to every subscriber of topic. For each subscriber, the
// payload is deep-copied (malloc + memcpy) and non-blockingly enqueued via
// msg_queue_try_enqueue, so one full or slow subscriber can never stall
// another subscriber or the publishing thread. The caller keeps ownership
// of item->payload; each subscriber owns (and must free) its own cloned
// copy once it dequeues it.
// Returns the number of subscribers the message was delivered to (0 if
// the topic has no subscribers or doesn't exist), or -1 on invalid args.
int pubsub_publish(pubsub_context_t *ctx, const char *topic, const msg_item_t *item);

#ifdef __cplusplus
}
#endif

#endif // PUBSUB_H
