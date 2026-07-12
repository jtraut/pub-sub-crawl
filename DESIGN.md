# pub-sub-crawl: Architecture & Design

**Purpose:** a small, hands-on project for practicing multi-threaded async
messaging in C well enough to explain the actual mechanics (not just wave
at "pthreads"), connection-level protocol versioning, and protobuf as a
wire format. Secondary goal: real hands-on MQTT pub/sub experience.

This isn't meant to be production code. It's meant to be small, real, and
something you can explain line by line under questioning.

---

## 1. The shape of the thing

A common pattern in embedded-to-cloud bridge architectures — a device-side
process, a bridge layer, then app-layer clients downstream — scaled down to
something buildable in a weekend:

```
┌─────────────────────┐        Unix domain socket        ┌──────────────────┐        MQTT         ┌──────────────────┐
│   radio_sim (C)      │  ───────────────────────────►    │   bridge.py       │  ──────────────►    │  mock_client.py   │
│                       │  ◄───────────────────────────    │   (Python)        │  ◄──────────────    │  (Python)          │
│ pthreads, queues,     │      framed JSON messages,        │ paho-mqtt client, │   local broker       │ pretty-prints       │
│ pub/sub, transport    │      simple text commands back    │ republishes to    │   (Mosquitto)        │ what it receives,   │
│ thread                │                                   │ MQTT topics       │                       │ stands in for a     │
│                       │                                   │                   │                       │ mobile/app client   │
└─────────────────────┘                                    └──────────────────┘                       └──────────────────┘
```

Mosquitto here is a stand-in open-source broker for practice, not a
statement about any particular production stack.

---

## 2. What each piece is for

### `radio_sim` (C) — the main event, the actual practice target

Simulates an embedded radio process. Internally:

- **Producer threads** generate simulated events:
  - A periodic telemetry producer (timer-driven, every N ms: fake battery %,
    RSSI, uptime)
  - A stdin-driven "trigger" producer, so you can type commands at the
    running process to simulate external events (button press, antenna
    unplugged, alert condition) without needing real hardware
- **A thread-safe bounded queue** (mutex + two condition variables:
  `not_empty`, `not_full`) is the one core primitive everything else builds
  on.
- **A small worker thread pool** (2-4 threads) pulls from a shared ingest
  queue, does lightweight "processing" (validate, timestamp, assign a
  sequence number), and publishes into the pub/sub layer based on message
  type.
- **A pub/sub layer**: topics (`telemetry`, `alerts`, `status`), each topic
  keeps a list of subscriber queues. Publishing clones the message into
  every subscriber's own queue rather than calling handlers directly, so
  one slow subscriber can never stall another.
- **A transport thread** subscribes to `telemetry` and `alerts`, drains
  those queues, serializes each message, and writes it out over a Unix
  domain socket to whatever's connected (the Python bridge).
- **A command-listener thread** reads simple text commands back from the
  same socket (e.g. `SET_INTERVAL 500`, `TRIGGER_ALERT`) and applies them —
  a bidirectional story, not just one-way telemetry, conceptually similar
  in shape to a desired/reported device-shadow pattern.
- **Clean shutdown**: a `SIGINT` handler that signals all threads to stop,
  drains queues, and joins every thread before exiting. Worth building
  deliberately — "how do you shut down a multithreaded system cleanly" is a
  very plausible question in its own right.

### `bridge.py` (Python) — the bridge layer

- Connects to `radio_sim`'s Unix domain socket.
- Reads framed messages in a loop, parses them, republishes each onto an
  MQTT topic (`radio/telemetry`, `radio/alerts`) via `paho-mqtt` against a
  local Mosquitto broker.
- Optionally subscribes to `radio/commands` on MQTT and forwards anything
  received down to `radio_sim` over the same socket, closing the loop.

### `mock_client.py` (Python, optional/stretch) — stands in for a mobile/app client

- Subscribes to `radio/#` over MQTT, pretty-prints what it receives. Not
  meant to be real UI, just something that proves the whole chain works end
  to end and gives a second vantage point to demo from.

---

## 3. Protocol version handshake, and message format

A connection-level negotiation pattern, distinct from per-message
versioning schemes used on connectionless transports (e.g. UDP). Because
the transport here is a persistent Unix socket, connection-level
negotiation fits cleanly — there's a well-defined moment (connection setup)
to agree on a version once, rather than tagging every individual message.

### The handshake sequence

Plain text, line-based, deliberately not JSON, so there's no
chicken-and-egg problem of needing to already agree on a format before
you've agreed on a format:

```
1. Client connects.
2. Server sends:   HELLO SERVER_VERSIONS=1
3. Client sends:   HELLO CLIENT_VERSIONS=1
4. Server computes the highest version in common, replies:
                    HELLO_ACK VERSION=1
5. From here on, every message on this connection uses that negotiated version.
```

If there's no overlap (e.g. a future client only speaks version 3, server
only offers 1-2):

```
4b. Server replies:  HELLO_NACK REASON=no_common_version
5b. Server closes the connection.
```

Designing the rejection path deliberately matters: "what happens when
negotiation fails" is exactly the kind of edge case worth being able to
describe from having actually built it, not just imagined it.

### Per-connection state

The server needs to remember which version it negotiated with *this*
client specifically, since it might later have multiple clients (or
reconnect) negotiating different versions. Keep it simple: a small
connection struct holding the socket fd, the negotiated version, and a
pointer/index into whichever subscriber queues this connection's transport
thread drains.

```c
typedef struct {
    int client_fd;
    int negotiated_version;
    msg_queue_t *telemetry_sub;
    msg_queue_t *alerts_sub;
} client_conn_t;
```

The transport thread's serialization function branches on
`negotiated_version` when building each outgoing message — this is the one
place version-awareness actually lives.

### Version 1 (build this first)

```json
{"schema_version": 1, "type": "telemetry", "seq": 42, "ts": 1731289200, "battery_pct": 87, "rssi_dbm": -63}
```

```json
{"schema_version": 1, "type": "alert", "seq": 43, "ts": 1731289201, "code": "ANTENNA_UNPLUGGED", "antenna": 2}
```

### Version 2 (add later, once v1 and the handshake both work)

Deliberately includes both kinds of change worth being able to tell apart:

```json
{"schema_version": 2, "type": "telemetry", "seq": 42, "ts": "2026-07-13T11:02:00Z", "battery_pct": 87, "rssi_dbm": -63, "temperature_c": 41.2}
```

- **Additive, backward-compatible change:** the new `temperature_c` field.
  A v1-only client would simply never see it, nothing breaks.
- **Breaking change:** `ts` moves from a Unix epoch integer to an ISO-8601
  string. A v1 client parsing this as an integer would fail outright — this
  is exactly why the handshake matters.

Once v2 exists, the negotiation logic can be tested both ways: have
`bridge.py` advertise `CLIENT_VERSIONS=1` only and confirm the server
correctly falls back to v1 formatting even though it supports v2, then
advertise `CLIENT_VERSIONS=1,2` and confirm it negotiates up to v2. That's a
real, demonstrable test of the mechanism, not just code that happens to
run.

Commands going the other direction can stay plain text regardless of
version — no need for a parser on the C side for something this small:

```
SET_INTERVAL 500
TRIGGER_ALERT ANTENNA_UNPLUGGED 2
```

---

## 4. Protobuf, as a wire-format upgrade once JSON works

Build the JSON version first, get the core pieces solid, then swap the
wire format to protobuf as its own deliberate exercise. Migrating a working
wire format is itself a real skill, and it's genuinely useful to compare
both approaches directly having built both.

### Schema

```protobuf
syntax = "proto3";

message Telemetry {
  uint32 seq = 1;
  int64 ts = 2;
  uint32 battery_pct = 3;
  int32 rssi_dbm = 4;
  optional float temperature_c = 5;   // added in v2, additive
}

message Alert {
  uint32 seq = 1;
  int64 ts = 2;
  string code = 3;
  uint32 antenna = 4;
}

message Envelope {
  uint32 schema_version = 1;
  oneof payload {
    Telemetry telemetry = 2;
    Alert alert = 3;
  }
}
```

### Toolchain

- **C side:** [`protobuf-c`](https://github.com/protobuf-c/protobuf-c), the
  standard C code generator for protobuf. Run
  `protoc --c_out=. radio_messages.proto` to generate
  `radio_messages.pb-c.h/.c`, link against `libprotobuf-c`. The generated
  code gives you `Telemetry__init()`, `envelope__pack()`, etc. — build the
  struct, then pack it to bytes.
- **Python side:** the standard `protobuf` package
  (`pip install protobuf`), `protoc --python_out=. radio_messages.proto`
  generates the Python bindings, `Envelope.ParseFromString(bytes)` on the
  way in.
- **This replaces JSON as serialization only, not the transport.** Still a
  raw Unix domain socket, not gRPC — gRPC would add an HTTP/2 RPC layer on
  top that's out of scope here.

### The one real gotcha: framing

Protobuf gives you compact bytes, not messages with built-in boundaries —
on a stream socket you still need to know where one message ends and the
next begins. Standard fix: a 4-byte big-endian length prefix before each
serialized `Envelope`:

```
[ 4-byte length ][ protobuf-encoded Envelope bytes ][ 4-byte length ][ ... ]
```

Both the C writer and the Python reader need to agree on this framing —
it's a small thing, but "how do you delimit messages over a byte stream" is
a legitimate embedded/networking question in its own right.

### Why this is worth doing even with the handshake already in place

Protobuf has its own, different answer to schema evolution: add a new
field with a new field number (like `temperature_c = 5` above) and old code
simply ignores field numbers it doesn't recognize — no version bump needed
for additive changes. That's genuinely different from the JSON version's
`schema_version` field, and worth contrasting directly: protobuf handles
additive change for free via field numbers, but a real breaking change
(like the `ts` type change) still isn't safe to make in place — you'd
retire the old field number and add a new one, or still lean on the
connection-level handshake to pick a compatible message shape entirely.
Having built both gives a real, comparative answer instead of a textbook
one.

---

## 5. Suggested build order

Build in this order so there's a demonstrable, explainable artifact at
every stage, even if you stop before the last phase.

**Phase 1 — the foundational piece:**
- `msg_queue.c/.h`: the bounded thread-safe queue, mutex + two cond vars.
- `pubsub.c/.h`: topic registry, subscribe/publish, fan-out to
  per-subscriber queues.
- A minimal `main.c`: spawn one producer thread, one consumer thread, wire
  them through the queue, print what comes out. No networking yet.
- **Stop here and it's already worth something.** `pthread_create`, the
  mutex/cond-var discipline, and pub/sub fan-out are all demonstrable from
  code that actually runs.

**Phase 2:**
- Add the worker thread pool.
- Add the transport thread and a Unix domain socket server
  (`socket()`/`bind()`/`listen()`/`accept()` on `AF_UNIX`).
- Add `handshake.c/.h`: the `HELLO`/`HELLO_ACK`/`HELLO_NACK` exchange,
  computed against a hardcoded list of server-supported versions, stored
  per-connection in `client_conn_t`.
- Add the command-listener thread and the `SIGINT`-driven clean shutdown.

**Phase 3:**
- `bridge.py`: connect to the socket, perform the handshake (advertise
  `CLIENT_VERSIONS=1`), parse frames, republish to Mosquitto via
  `paho-mqtt`.
- Get Mosquitto running locally (`mosquitto` package, or the official
  Docker image), default port 1883, no auth needed for local practice.

**Phase 4, stretch:**
- `mock_client.py` subscribing over MQTT.
- A short README section with a GIF or terminal recording of it running
  end to end.
- Basic unit tests for the queue (push/pop under concurrent load, e.g. with
  a stress test spawning many producer/consumer threads).

**Phase 5, stretch, only after Phase 1-2 are solid:**
- Add version 2 telemetry formatting server-side (branching on
  `negotiated_version` in the transport thread's serializer).
- Update `bridge.py` to parse both v1 and v2 telemetry.
- Actually test the negotiation both directions: force `CLIENT_VERSIONS=1`
  and confirm v1 formatting, then `CLIENT_VERSIONS=1,2` and confirm it
  negotiates up to v2.

**Phase 6, stretch, do this last — it's a wire-format swap, not new
architecture:**
- Write `radio_messages.proto`, generate C and Python bindings.
- Swap the transport thread's JSON serialization for `protobuf-c` packing,
  with 4-byte length-prefix framing.
- Swap `bridge.py`'s parsing to the generated Python protobuf bindings.
- Confirm the same v1/v2 behavior still works, now over protobuf instead of
  JSON.

**Phase 7, stretch, optional, do this only if Phases 1-2 are solid — a
hardening exercise, not new architecture (see section 10 for the design):**
- Replace the `malloc`/`free` calls in the message hot path (the producer's
  per-tick telemetry/alert payloads, `pubsub_publish`'s per-subscriber deep
  copy) with acquire/release against a fixed-size, pre-allocated message
  pool.
- Decide and implement a pool-exhaustion policy explicitly (drop the
  message and count/log it, matching the drop-under-backpressure spirit of
  `msg_queue_try_enqueue`) rather than blocking or silently falling back to
  `malloc`.
- Confirm the rest of the system still behaves the same — this phase
  changes *how* memory for a message is obtained, not the pub/sub or
  transport logic built on top of it.

---

## 6. Repo layout

```
pub-sub-crawl/
├── README.md
├── DESIGN.md
├── proto/
│   └── radio_messages.proto
├── c/
│   ├── Makefile
│   ├── include/
│   │   ├── msg_queue.h
│   │   ├── pubsub.h
│   │   └── handshake.h
│   └── src/
│       ├── msg_queue.c
│       ├── pubsub.c
│       ├── main.c
│       ├── worker_pool.c
│       ├── transport.c
│       └── handshake.c
└── python/
    ├── requirements.txt
    ├── bridge.py
    └── mock_client.py
```

---

## 7. Tech choices, and the reasoning

| Choice | Why |
|---|---|
| POSIX pthreads, not a higher-level C++ concurrency lib | The actual point of the exercise. |
| Unix domain socket for C↔Python | Local IPC, simpler than a TCP port, realistic for a same-machine bridge process. |
| Plain JSON lines first, protobuf as a later swap | JSON gets the core pieces working fast with zero extra dependencies; protobuf is deliberately a separate phase so it's practiced properly rather than fought alongside the concurrency work. |
| Plain text for the handshake specifically, not JSON/protobuf | Nothing's been negotiated yet at that point in the connection, so you can't assume the other side agrees on any structured format until after `HELLO_ACK`. Keeps the bootstrap step dependency-free. |
| `protobuf-c` on the C side, not hand-rolled binary encoding | Real, standard tooling that generates the pack/unpack code; the useful part is knowing the schema/field-number model and the framing problem, not hand-rolling a binary format. |
| 4-byte length-prefix framing for protobuf | Protobuf messages don't self-delimit on a stream socket; length-prefixing is the standard fix, and both sides need to agree on it explicitly. |
| Mosquitto for local MQTT | Free, standard, trivial to run locally, real hands-on MQTT broker/pub-sub experience. |
| A Makefile, not CMake | Small enough project that CMake would be overhead; worth knowing you'd reach for CMake past a certain size. |
| C11, `-Wall -Wextra -pthread` | Catch real warnings; `-pthread` is required for correct linking/threading behavior on Linux, worth knowing why it's there, not just copying it into the Makefile. |

---

## 8. Design rationale worth being able to explain

- `pthread_create`/`join`/`detach`, and when you'd choose each.
- Why the queue needs a mutex *and* two condition variables, not just a
  mutex, and what `pthread_cond_wait` actually does under the hood
  (atomically unlocks while sleeping, re-locks on wake).
- Why pub/sub fans out to per-subscriber queues instead of calling
  subscriber callbacks directly.
- Sync vs. async in concrete terms using this codebase: the producer
  threads writing to the ingest queue is essentially synchronous from
  their own point of view (blocks briefly if the queue's full), while the
  *system* as a whole is async — nothing downstream blocks anything
  upstream once the message is queued.
- Connection-level protocol negotiation vs. per-message versioning, and
  when each fits better (connection-oriented transports vs. connectionless
  ones like UDP).
- The handshake itself: why a plain-text `HELLO`/`HELLO_ACK` exchange
  rather than JSON for negotiation specifically (nothing's been agreed on
  yet, so you can't assume the other side can parse JSON until after
  negotiation succeeds), and what happens on `HELLO_NACK`.
- The difference between an additive/backward-compatible protocol change
  (v2's new `temperature_c` field) and a breaking one (v2's `ts` format
  change), and why only the handshake, not per-message versioning, cleanly
  handles a breaking change on a persistent connection.
- Protobuf's own schema-evolution model (field numbers, additive fields
  ignored gracefully by old code) versus the JSON version's explicit
  `schema_version` handshake, and why a breaking change still isn't free
  even with protobuf.
- Why a stream socket needs explicit message framing (the length-prefix)
  when a datagram protocol like UDP wouldn't — each UDP packet is already a
  discrete message; a TCP/Unix-socket byte stream has no such boundary.
- How this would extend toward what a real embedded radio + RTOS
  environment would need differently, e.g., bounded memory (no `malloc`
  churn, a fixed message pool instead of arbitrary heap allocations — see
  section 10 for a concrete sketch of what that looks like in this
  codebase), ISR-safe hand-off patterns if this were interrupt-driven
  instead of thread-driven, and priority-aware scheduling instead of a flat
  thread pool.

That last point matters: this project is deliberately built on pthreads and
a general-purpose OS (embedded Linux, not RTOS). Being able to say clearly
what would change in a true RTOS/bare-metal context, rather than pretending
this project *is* that, is itself a stronger answer than overclaiming.

---

## 9. Milestones

Realistic minimum bar: **Phase 1 fully working and explainable.** Everything
past that is upside, including the version-2 negotiation work in Phase 5
and the protobuf migration in Phase 6 — both genuinely valuable, but Phase
1's queue and pub/sub mechanics are the part that matters most on their
own. Don't let chasing the handshake, v2, or protobuf cost the ability to
talk fluently about Phase 1.

---

## 10. Optional stretch: a pre-allocated message pool

Everything so far uses ordinary `malloc`/`free` for message payloads: the
producer heap-allocates a `telemetry_raw_t`/`alert_raw_t` per tick,
`pubsub_publish` heap-allocates a fresh deep copy per subscriber, and each
side frees its copy once it's done with it. That's the right amount of
complexity for the core exercise — pthreads, queues, and pub/sub fan-out
are the point, not memory management.

But it's worth naming explicitly, as an optional exercise for later, where
this diverges from common embedded firmware practice: a real embedded
target typically avoids `malloc`/`free` entirely once the system is past
init, for two concrete reasons — allocation/free time on a general-purpose
heap allocator isn't bounded (bad for deterministic, real-time-ish
behavior), and a long-running process that keeps allocating and freeing
variable-lifetime, similarly-sized chunks is exactly the pattern that
fragments a heap over time. Neither is disqualifying for a weekend
project, but both are real, and "how would you avoid this on an actual
embedded target" is a fair follow-up question to be able to answer with
more than "you'd use a memory pool."

### The idea

Replace the mid-process `malloc`/`free` calls with acquire/release against
a fixed-size pool of pre-allocated slots, sized once at startup:

```c
#define MSG_POOL_CAPACITY 64
// Sized to the largest payload this pool needs to hand out; today that's
// whichever of telemetry_msg_t / alert_msg_t is bigger.
#define MSG_POOL_SLOT_SIZE 64

typedef struct {
    uint8_t data[MSG_POOL_SLOT_SIZE];
    bool in_use;
} msg_pool_slot_t;

typedef struct {
    msg_pool_slot_t slots[MSG_POOL_CAPACITY];
    pthread_mutex_t mutex;
} msg_pool_t;

// Returns NULL if the pool is exhausted -- see "Exhaustion policy" below,
// this must not fall back to malloc.
void *msg_pool_acquire(msg_pool_t *pool);
void msg_pool_release(msg_pool_t *pool, void *slot);
```

Where this would slot into the existing code:
- The producer would `msg_pool_acquire()` instead of `malloc()` for each
  tick's `telemetry_raw_t`/`alert_raw_t`, and the worker would
  `msg_pool_release()` it instead of `free()` once published (see
  `worker_pool.c`).
- `pubsub_publish`'s per-subscriber deep copy (`pubsub.c`) is the harder
  case: it currently mallocs a copy sized to `item->length` for an
  arbitrary topic. A pool-backed version only works cleanly because
  `msg_type_t` is now a small, closed set of known-size payloads
  (telemetry/alert) — a pool can be sized to the larger of the two. That's
  a real constraint worth surfacing on its own: a fixed-size pool implies
  either a closed set of message shapes or a slot size sized to the worst
  case (wasting space on smaller messages). The old `MSG_TYPE_STRING` demo
  topic from Phase 1, with its arbitrary-length payload, is exactly the
  shape a pool can't cleanly serve without a cap on message size.

### Exhaustion policy

A bounded pool needs an explicit answer for "what happens when it's
empty," the same way the bounded queue already needed one for "what
happens when it's full." Match the existing precedent
(`msg_queue_try_enqueue`'s non-blocking, drop-and-move-on semantics) rather
than introducing a new one: `msg_pool_acquire` returns `NULL` on
exhaustion, the caller drops the message and logs/counts it, and — this is
the part that actually matters — nothing falls back to `malloc` on the
exhaust path. A fallback would quietly defeat the entire point (you'd
still see occasional unbounded allocations, just rarer and harder to
reason about), so the caller has to be able to just lose a message
instead, the same way a slow subscriber can already lose one via
`msg_queue_try_enqueue`.

### Why this is explicitly optional

This is a hardening/realism exercise, not new architecture, and it doesn't
change the pub/sub or transport behavior built on top of it — it only
changes where a message's memory comes from. Worth doing after Phases 1-2
are solid (see Phase 7 in section 5), and worth being able to explain even
if never built: what a fixed-size pool buys you (deterministic
acquire/release time, no long-run fragmentation), what it costs
(pre-committed worst-case memory, a closed/size-capped set of message
shapes), and why that trade makes sense on an embedded target but wouldn't
be the default choice for, say, a general-purpose backend service.
