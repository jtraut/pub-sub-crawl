#ifndef RADIO_CONTROL_H
#define RADIO_CONTROL_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define RADIO_CONTROL_DEFAULT_INTERVAL_MS 1000u

// Mutex-guarded knob shared between the producer thread (reads the
// interval between ticks every cycle) and a connected client's
// command-listener thread (writes it via a SET_INTERVAL command) -- the
// "desired" half of the desired/reported shape described in DESIGN.md
// section 2.
typedef struct radio_control radio_control_t;

// initial_interval_ms == 0 falls back to RADIO_CONTROL_DEFAULT_INTERVAL_MS.
radio_control_t *radio_control_create(uint32_t initial_interval_ms);
void radio_control_destroy(radio_control_t *control);

uint32_t radio_control_get_interval_ms(radio_control_t *control);
// interval_ms == 0 is ignored (rejects a command that would spin the
// producer in a busy loop).
void radio_control_set_interval_ms(radio_control_t *control, uint32_t interval_ms);

#ifdef __cplusplus
}
#endif

#endif // RADIO_CONTROL_H
