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

typedef struct msg_item {
    uint32_t id;
    void *payload;
    size_t length;
} msg_item_t;

// capacity == 0 uses MSG_QUEUE_DEFAULT_CAPACITY
msg_queue_t *msg_queue_create(size_t capacity);
void msg_queue_destroy(msg_queue_t *queue);

// Blocks on not_full until space is available.
bool msg_queue_enqueue(msg_queue_t *queue, const msg_item_t *item);
// Returns false immediately if the queue is full, so a slow subscriber
// can never stall other subscribers or the thread publishing to it.
bool msg_queue_try_enqueue(msg_queue_t *queue, const msg_item_t *item);

// Blocks on not_empty until an item is available.
bool msg_queue_dequeue(msg_queue_t *queue, msg_item_t *item);
bool msg_queue_try_dequeue(msg_queue_t *queue, msg_item_t *item);

size_t msg_queue_size(const msg_queue_t *queue);
void msg_queue_clear(msg_queue_t *queue);

#ifdef __cplusplus
}
#endif

#endif // MSG_QUEUE_H
