#ifndef MSG_QUEUE_H
#define MSG_QUEUE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

// C++ compatibility
#ifdef __cplusplus
extern "C" {
#endif

#define MSG_QUEUE_DEFAULT_CAPACITY 1024

// A bounded, thread-safe FIFO backed by a ring (circular) buffer: a fixed
// array of msg_item_t plus head/tail indices that wrap around modulo
// capacity, so enqueue/dequeue never shift elements, just advance an
// index and wrap. One mutex guards the buffer; two condition variables
// (not_empty, not_full) let producers/consumers block instead of busy-wait
// when the ring is empty/full respectively. See msg_queue.c for the
// index/wraparound details.
typedef struct msg_queue msg_queue_t;

// What kind of message this is, so a downstream worker can route it to the
// right pubsub topic without having to inspect the payload.
typedef enum {
    MSG_TYPE_TELEMETRY,
    MSG_TYPE_ALERT,
} msg_type_t;

typedef struct msg_item {
    uint32_t id;
    msg_type_t type;
    void *payload;
    size_t length;
} msg_item_t;

// capacity == 0 uses MSG_QUEUE_DEFAULT_CAPACITY
msg_queue_t *msg_queue_create(size_t capacity);
void msg_queue_destroy(msg_queue_t *queue);

// Blocks on not_full until space is available or the queue is shut down.
// Returns false if the queue was shut down before space opened up (the
// caller should treat this the same as any other "stop" signal, not as
// an error).
bool msg_queue_enqueue(msg_queue_t *queue, const msg_item_t *item);
// Returns false immediately if the queue is full, so a slow subscriber
// can never stall other subscribers or the thread publishing to it.
bool msg_queue_try_enqueue(msg_queue_t *queue, const msg_item_t *item);

// Blocks on not_empty until an item is available. A shut-down queue keeps
// returning already-queued items (true) until it's empty -- only then
// does it return false, so a caller still gets to drain whatever was
// already there before it has to stop.
bool msg_queue_dequeue(msg_queue_t *queue, msg_item_t *item);
bool msg_queue_try_dequeue(msg_queue_t *queue, msg_item_t *item);

size_t msg_queue_size(const msg_queue_t *queue);
void msg_queue_clear(msg_queue_t *queue);

// Marks the queue as shutting down and broadcasts both condition
// variables, so every thread currently blocked in msg_queue_enqueue/
// msg_queue_dequeue on it wakes up immediately instead of waiting for a
// producer/consumer that isn't coming. Idempotent. This is the real
// wakeup path for clean shutdown -- callers should never need to poll a
// queue's blocking calls against some other flag.
void msg_queue_shutdown(msg_queue_t *queue);

#ifdef __cplusplus
}
#endif

#endif // MSG_QUEUE_H
