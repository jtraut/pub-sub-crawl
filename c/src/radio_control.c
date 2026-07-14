#include "radio_control.h"
#include <stdlib.h>
#include <pthread.h>

struct radio_control {
    pthread_mutex_t mutex;
    uint32_t interval_ms;
};

radio_control_t *radio_control_create(uint32_t initial_interval_ms) {
    if (initial_interval_ms == 0) {
        initial_interval_ms = RADIO_CONTROL_DEFAULT_INTERVAL_MS;
    }

    radio_control_t *control = malloc(sizeof(*control));
    if (!control) {
        return NULL;
    }

    if (pthread_mutex_init(&control->mutex, NULL) != 0) {
        free(control);
        return NULL;
    }

    control->interval_ms = initial_interval_ms;
    return control;
}

void radio_control_destroy(radio_control_t *control) {
    if (!control) {
        return;
    }
    pthread_mutex_destroy(&control->mutex);
    free(control);
}

uint32_t radio_control_get_interval_ms(radio_control_t *control) {
    pthread_mutex_lock(&control->mutex);
    uint32_t interval_ms = control->interval_ms;
    pthread_mutex_unlock(&control->mutex);
    return interval_ms;
}

void radio_control_set_interval_ms(radio_control_t *control, uint32_t interval_ms) {
    if (interval_ms == 0) {
        return;
    }
    pthread_mutex_lock(&control->mutex);
    control->interval_ms = interval_ms;
    pthread_mutex_unlock(&control->mutex);
}
