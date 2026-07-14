# pub-sub-crawl: Multi-threaded Async Messaging in C

A small, hand-built simulation of an embedded radio process: producer threads
generate events, a bounded thread-safe queue moves them between threads, and
a pub/sub layer fans messages out to per-topic subscribers. Built to be
explainable line by line, not production code.

Full design doc and build roadmap: [`DESIGN.md`](DESIGN.md).

## Status

**Phase 1 (done):** the core concurrency primitives, wired together in a
minimal demo.

- `msg_queue` — a bounded, thread-safe FIFO (ring buffer + mutex + two
  condition variables: `not_empty`, `not_full`).
- `pubsub` — a topic registry with subscribe/publish, fanning out to each
  subscriber's own queue (a deep copy per subscriber, non-blocking enqueue)
  so one slow subscriber can never stall another.
- `main.c` — spawns one producer thread and one consumer thread wired
  through a queue, printing what comes out.

**Phase 2 (done):** networking, protocol negotiation, and clean shutdown.

- `worker_pool` — a small pool of worker threads pulling raw items off the
  ingest queue, assigning `seq`/`ts`, and republishing into `pubsub`.
- `transport` — an `AF_UNIX` socket server: one thread per accepted
  connection serves telemetry/alerts as newline-delimited JSON.
- `handshake` — the `HELLO`/`HELLO_ACK`/`HELLO_NACK` version negotiation
  against a hardcoded list of server-supported versions
  (`client_conn_t`, DESIGN.md section 3).
- `command` — a second, sibling thread per connection reads
  `SET_INTERVAL <ms>` / `TRIGGER_ALERT <code> <antenna>` text commands back
  over the same socket and applies them live (`radio_control`'s
  mutex-guarded interval, and a direct enqueue onto the ingest queue) — the
  bidirectional half of the transport.
- Clean `SIGINT` shutdown — `main` blocks `SIGINT` in every thread before
  spawning any of them and waits on it synchronously via `sigwait()`, then
  orchestrates teardown explicitly: a real condition-variable broadcast
  (`msg_queue_shutdown`) wakes every thread blocked in a queue
  enqueue/dequeue, a small polled flag (`shutdown_requested()`) covers the
  handful of waits a queue can't reach (the producer's between-tick sleep,
  transport's/the command listener's `poll()` loops), and everything is
  joined before the process exits.

**Not built yet:** the Python bridge/Mosquitto side (phase 3), v2 telemetry
negotiation (phase 5), and the protobuf wire format (phase 6). See
[`DESIGN.md`](DESIGN.md) section 5 for the full build order.

## Architecture

```
producer thread ──enqueue──► msg_queue (ring buffer, mutex + 2 cond vars) ──dequeue──► consumer thread
```

`pubsub` sits on top of `msg_queue`: instead of one shared queue, each
subscriber gets its own `msg_queue_t`, and `pubsub_publish` clones the
message into every subscriber queue subscribed to a topic.

```
                     ┌──────────────┐
publisher ──publish─►│  pubsub_ctx  │
                     │ (topic →     │
                     │  subscriber  │
                     │  queue list) │
                     └──────┬───────┘
                            │ clone + try_enqueue (non-blocking)
              ┌─────────────┼─────────────┐
              ▼             ▼             ▼
        subscriber Q   subscriber Q   subscriber Q
```

## Requirements

- GCC with C11 and pthread support
- GNU Make
- **On Windows:** this code needs real POSIX headers (`sys/socket.h`,
  `sys/un.h` for `AF_UNIX` sockets, a `SIGPIPE`-having `signal.h`) that a
  native Windows/MinGW-w64 `gcc` (e.g. from Git Bash or a plain `scoop`/
  `winget` install) doesn't provide, no matter what flags you pass. Build
  and run it inside **WSL** instead — this repo was developed and tested
  against WSL2 Ubuntu. If you have a WSL distro installed, you don't even
  need to leave PowerShell/Git Bash: see `make wsl-build` below, which
  shells out to `wsl.exe` for you.

## Build

Inside WSL (or any real Linux/POSIX box):

```sh
cd c
make          # builds ./radio_sim
make clean    # removes build/ and the radio_sim binary
```

From PowerShell or Git Bash on Windows, with a WSL distro installed:

```sh
cd c
make wsl-build   # builds ./radio_sim inside WSL against this same source tree
make wsl-clean   # make clean, inside WSL
make wsl-run     # wsl-build, then runs ./radio_sim inside WSL
```

## Run

```sh
cd c
./radio_sim
```

The producer thread enqueues telemetry once a second by default (an
alert every fifth tick too) and the consumer thread dequeues and prints
each one. Connect a client to the `AF_UNIX` socket at
`/tmp/radio_sim.sock` to also negotiate a protocol version and receive the
same messages as JSON lines, and to send back `SET_INTERVAL <ms>` /
`TRIGGER_ALERT <code> <antenna>` commands. Stop it with `Ctrl+C` --
`SIGINT` now triggers a clean shutdown: every thread is signaled to stop,
queues are drained, and everything is joined before the process exits.

## Project layout

```
c/
├── Makefile
├── include/
│   ├── msg_queue.h
│   ├── pubsub.h
│   ├── worker_pool.h
│   ├── transport.h
│   ├── handshake.h
│   ├── command.h
│   ├── radio_control.h
│   ├── shutdown.h
│   ├── radio_msg.h
│   └── log.h
└── src/
    ├── msg_queue.c
    ├── pubsub.c
    ├── worker_pool.c
    ├── transport.c
    ├── handshake.c
    ├── command.c
    ├── radio_control.c
    ├── shutdown.c
    ├── log.c
    └── main.c
```
