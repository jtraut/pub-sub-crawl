#include "msg_queue.h"
#include <stdlib.h>
#include <string.h>
#include <pthread.h>


// Ring (circular) buffer: `items` is a fixed-size array of length
// `capacity`. `head` is the index of the oldest item (next one dequeue
// will read); `tail` is the index of the next empty slot (where enqueue
// will write). Both indices only ever move forward and wrap back to 0
// via `(index + 1) % capacity` once they hit the end of the array, so
// the "circle": once space frees up at the front, new items can wrap
// around and reuse it without ever shifting existing elements.
//
// `head == tail` is ambiguous on its own, it means either "empty" or
// "completely full", so `size` is tracked as a separate counter instead
// of being inferred from head/tail. That's what enqueue/dequeue actually
// check against `capacity`/0, not the index positions.
typedef struct {
    msg_item_t *items;
    size_t head;
    size_t tail;
    size_t size;
    size_t capacity;
} queue_buffer_t;

struct msg_queue {
    queue_buffer_t buffer;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
};

msg_queue_t *msg_queue_create(size_t capacity) {
    if (capacity == 0) {
        capacity = MSG_QUEUE_DEFAULT_CAPACITY;
    }

    msg_queue_t *queue = (msg_queue_t *)malloc(sizeof(msg_queue_t));
    if (!queue) {
        return NULL;
    }

    queue->buffer.capacity = capacity;
    queue->buffer.items = (msg_item_t *)malloc(sizeof(msg_item_t) * capacity);
    if (!queue->buffer.items) {
        free(queue);
        return NULL;
    }

    queue->buffer.head = 0;
    queue->buffer.tail = 0;
    queue->buffer.size = 0;

    if (pthread_mutex_init(&queue->mutex, NULL) != 0) {
        free(queue->buffer.items);
        free(queue);
        return NULL;
    }

    if (pthread_cond_init(&queue->not_empty, NULL) != 0) {
        pthread_mutex_destroy(&queue->mutex);
        free(queue->buffer.items);
        free(queue);
        return NULL;
    }

    if (pthread_cond_init(&queue->not_full, NULL) != 0) {
        pthread_cond_destroy(&queue->not_empty);
        pthread_mutex_destroy(&queue->mutex);
        free(queue->buffer.items);
        free(queue);
        return NULL;
    }

    return queue;
}

void msg_queue_destroy(msg_queue_t *queue) {
    if (!queue) {
        return;
    }

    pthread_cond_destroy(&queue->not_full);
    pthread_cond_destroy(&queue->not_empty);
    pthread_mutex_destroy(&queue->mutex);
    free(queue->buffer.items);
    free(queue);
}

bool msg_queue_enqueue(msg_queue_t *queue, const msg_item_t *item) {
    if (!queue || !item) {
        return false;
    }

    pthread_mutex_lock(&queue->mutex);

    // Wait while queue is full. "Full" for a ring buffer means every slot
    // between head and tail (wrapping) is occupied, size == capacity,
    // rather than head == tail, which is also true when empty.
    while (queue->buffer.size >= queue->buffer.capacity) {
        pthread_cond_wait(&queue->not_full, &queue->mutex);
    }

    // Write at tail, then advance tail one slot, wrapping around to 0 if
    // it just fell off the end of the ring.
    queue->buffer.items[queue->buffer.tail] = *item;
    queue->buffer.tail = (queue->buffer.tail + 1) % queue->buffer.capacity;
    queue->buffer.size++;

    // Signal that queue is not empty
    pthread_cond_signal(&queue->not_empty);

    pthread_mutex_unlock(&queue->mutex);
    return true;
}

bool msg_queue_try_enqueue(msg_queue_t *queue, const msg_item_t *item) {
    if (!queue || !item) {
        return false;
    }

    pthread_mutex_lock(&queue->mutex);

    if (queue->buffer.size >= queue->buffer.capacity) {
        pthread_mutex_unlock(&queue->mutex);
        return false;
    }

    // Write at tail, then advance tail one slot, wrapping around to 0 if
    // it just fell off the end of the ring.
    queue->buffer.items[queue->buffer.tail] = *item;
    queue->buffer.tail = (queue->buffer.tail + 1) % queue->buffer.capacity;
    queue->buffer.size++;

    // Signal that queue is not empty
    pthread_cond_signal(&queue->not_empty);

    pthread_mutex_unlock(&queue->mutex);
    return true;
}

bool msg_queue_dequeue(msg_queue_t *queue, msg_item_t *item) {
    if (!queue || !item) {
        return false;
    }

    pthread_mutex_lock(&queue->mutex);

    // Wait while queue is empty
    while (queue->buffer.size == 0) {
        pthread_cond_wait(&queue->not_empty, &queue->mutex);
    }

    // Read from head, then advance head one slot, wrapping around to 0 if
    // it just fell off the end of the ring. This is the slot enqueue's
    // wraparound will eventually write into again once the ring comes
    // back around.
    *item = queue->buffer.items[queue->buffer.head];
    queue->buffer.head = (queue->buffer.head + 1) % queue->buffer.capacity;
    queue->buffer.size--;

    // Signal that queue is not full
    pthread_cond_signal(&queue->not_full);

    pthread_mutex_unlock(&queue->mutex);
    return true;
}

bool msg_queue_try_dequeue(msg_queue_t *queue, msg_item_t *item) {
    if (!queue || !item) {
        return false;
    }

    pthread_mutex_lock(&queue->mutex);

    if (queue->buffer.size == 0) {
        pthread_mutex_unlock(&queue->mutex);
        return false;
    }

    // Read from head, then advance head one slot, wrapping around to 0 if
    // it just fell off the end of the ring. This is the slot enqueue's
    // wraparound will eventually write into again once the ring comes
    // back around.
    *item = queue->buffer.items[queue->buffer.head];
    queue->buffer.head = (queue->buffer.head + 1) % queue->buffer.capacity;
    queue->buffer.size--;

    // Signal that queue is not full
    pthread_cond_signal(&queue->not_full);

    pthread_mutex_unlock(&queue->mutex);
    return true;
}

size_t msg_queue_size(const msg_queue_t *queue) {
    if (!queue) {
        return 0;
    }

    pthread_mutex_lock((pthread_mutex_t *)&queue->mutex);
    size_t size = queue->buffer.size;
    pthread_mutex_unlock((pthread_mutex_t *)&queue->mutex);

    return size;
}

void msg_queue_clear(msg_queue_t *queue) {
    if (!queue) {
        return;
    }

    pthread_mutex_lock(&queue->mutex);

    // Reset the ring to empty: head and tail can both just snap back to
    // index 0 (their positions only matter relative to each other and to
    // `size`, not in absolute terms), and the stale items between the old
    // head/tail no longer matter since size says there's nothing there.
    queue->buffer.head = 0;
    queue->buffer.tail = 0;
    queue->buffer.size = 0;

    // Signal all waiting threads
    pthread_cond_broadcast(&queue->not_full);

    pthread_mutex_unlock(&queue->mutex);
}
