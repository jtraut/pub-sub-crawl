#ifndef RADIO_MSG_H
#define RADIO_MSG_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define ALERT_CODE_MAX 32

// What the producer drops on the ingest queue: raw sensor/event data, no
// seq/ts yet. Assigning those is a worker's job (DESIGN.md section 2), so a
// producer never has to coordinate with other producers over a shared
// counter.
typedef struct {
    uint32_t battery_pct;
    int32_t rssi_dbm;
} telemetry_raw_t;

typedef struct {
    char code[ALERT_CODE_MAX];
    uint32_t antenna;
} alert_raw_t;

// What a worker publishes onto the telemetry/alerts pubsub topics once it's
// assigned seq/ts, and what the transport thread serializes to v1 JSON
// (DESIGN.md section 3).
typedef struct {
    uint32_t seq;
    int64_t ts;
    uint32_t battery_pct;
    int32_t rssi_dbm;
} telemetry_msg_t;

typedef struct {
    uint32_t seq;
    int64_t ts;
    char code[ALERT_CODE_MAX];
    uint32_t antenna;
} alert_msg_t;

#ifdef __cplusplus
}
#endif

#endif // RADIO_MSG_H
