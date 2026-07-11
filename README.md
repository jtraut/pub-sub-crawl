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

**Not built yet:** the worker thread pool, the Unix domain socket transport
to a Python bridge, a version-handshake protocol, clean `SIGINT` shutdown,
and a protobuf wire format. Right now the demo runs until you `Ctrl+C` it.

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
- **On Windows:** this code targets POSIX (pthreads, and later `AF_UNIX`
  sockets), so build and run it inside WSL, not natively. This repo was
  developed and tested against WSL2 Ubuntu.

## Build

```sh
cd c
make          # builds ./radio_sim
make clean    # removes build/ and the radio_sim binary
```

## Run

```sh
cd c
./radio_sim
```

You'll see a message printed once a second as the producer thread enqueues
and the consumer thread dequeues and prints it. Stop it with `Ctrl+C`
(there's no graceful shutdown yet).

## Project layout

```
c/
├── Makefile
├── include/
│   ├── msg_queue.h
│   └── pubsub.h
└── src/
    ├── msg_queue.c
    ├── pubsub.c
    └── main.c
```
