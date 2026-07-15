# Graph Report - .  (2026-07-14)

## Corpus Check
- Corpus is ~12,158 words - fits in a single context window. You may not need a graph.

## Summary
- 139 nodes · 280 edges · 10 communities
- Extraction: 79% EXTRACTED · 21% INFERRED · 0% AMBIGUOUS · INFERRED: 60 edges (avg confidence: 0.82)
- Token cost: 67,537 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_Protocol & Bridge Design (docs)|Protocol & Bridge Design (docs)]]
- [[_COMMUNITY_Module File Structure|Module File Structure]]
- [[_COMMUNITY_Message Queue Core|Message Queue Core]]
- [[_COMMUNITY_Design Rationale (concepts)|Design Rationale (concepts)]]
- [[_COMMUNITY_PubSub Topic Registry|Pub/Sub Topic Registry]]
- [[_COMMUNITY_Transport Connection Serving|Transport Connection Serving]]
- [[_COMMUNITY_Producer Loop & Runtime Controls|Producer Loop & Runtime Controls]]
- [[_COMMUNITY_Version Handshake|Version Handshake]]
- [[_COMMUNITY_Command Listener|Command Listener]]

## God Nodes (most connected - your core abstractions)
1. `main()` - 17 edges
2. `handshake_perform()` - 11 edges
3. `log_fprintf()` - 11 edges
4. `transport_routine()` - 10 edges
5. `log_printf()` - 9 edges
6. `serve_client()` - 9 edges
7. `transport_create()` - 8 edges
8. `radio_sim (C process)` - 8 edges
9. `apply_command()` - 7 edges
10. `producer_routine()` - 7 edges

## Surprising Connections (you probably didn't know these)
- `radio_control (mutex-guarded interval)` --conceptually_related_to--> `radio_sim (C process)`  [INFERRED]
  README.md → DESIGN.md
- `worker_pool` --conceptually_related_to--> `Worker thread pool`  [INFERRED]
  README.md → DESIGN.md
- `main()` --calls--> `shutdown_request()`  [INFERRED]
  c/src/main.c → c/src/shutdown.c
- `msg_queue (bounded thread-safe FIFO)` --conceptually_related_to--> `Bounded thread-safe message queue`  [INFERRED]
  README.md → DESIGN.md
- `pubsub (topic registry, subscribe/publish)` --conceptually_related_to--> `Pub/sub layer (topic registry, fan-out)`  [INFERRED]
  README.md → DESIGN.md

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **radio_sim's concurrency architecture (threads + primitives)** — design_radio_sim, design_msg_queue, design_worker_pool, design_pubsub, design_transport_thread, design_command_listener, design_sigint_shutdown [INFERRED 0.85]
- **Protobuf schema, framing, and toolchain (section 4)** — design_protobuf_wire_format, design_protobuf_c, design_envelope_message, design_telemetry_message, design_alert_message, design_length_prefix_framing [EXTRACTED 1.00]
- **Suggested build order, Phases 1-7 (section 5)** — design_phase_1, design_phase_2, design_phase_3, design_phase_4, design_phase_5, design_phase_6, design_phase_7 [EXTRACTED 1.00]

## Communities (10 total, 0 thin omitted)

### Community 0 - "Protocol & Bridge Design (docs)"
Cohesion: 0.10
Nodes (27): Alert (protobuf message), bridge.py (Python bridge layer), client_conn_t (per-connection state struct), Envelope (protobuf message), HELLO/HELLO_ACK/HELLO_NACK protocol version handshake, 4-byte length-prefix framing, mock_client.py (Python mock client), Mosquitto (local MQTT broker) (+19 more)

### Community 1 - "Module File Structure"
Cohesion: 0.22
Nodes (5): sig_atomic_t, command_listener_routine(), read_command_line(), next_sequence(), worker_routine()

### Community 2 - "Message Queue Core"
Cohesion: 0.19
Nodes (18): consumer_routine(), main(), msg_item_t, msg_queue_t, msg_queue_clear(), msg_queue_create(), msg_queue_dequeue(), msg_queue_destroy() (+10 more)

### Community 3 - "Design Rationale (concepts)"
Cohesion: 0.16
Nodes (18): Command-listener thread, Pool exhaustion policy, Pre-allocated fixed-size message pool, msg_pool_acquire(), Bounded thread-safe message queue, Phase 1 -- foundational queue + pub/sub, Phase 7, stretch -- message pool hardening, Pub/sub layer (topic registry, fan-out) (+10 more)

### Community 4 - "Pub/Sub Topic Registry"
Cohesion: 0.30
Nodes (13): log_printf(), msg_item_t, msg_queue_t, pubsub_context_t, find_or_create_topic(), find_topic(), pubsub_create(), pubsub_destroy() (+5 more)

### Community 5 - "Transport Connection Serving"
Cohesion: 0.15
Nodes (13): alert_msg_t, msg_queue_t, pubsub_context_t, radio_control_t, sig_atomic_t, format_alert(), format_telemetry(), serve_client() (+5 more)

### Community 6 - "Producer Loop & Runtime Controls"
Cohesion: 0.27
Nodes (9): producer_routine(), producer_sleep(), radio_control_t, radio_control_create(), radio_control_destroy(), radio_control_get_interval_ms(), radio_control_set_interval_ms(), shutdown_request() (+1 more)

### Community 7 - "Version Handshake"
Cohesion: 0.44
Nodes (8): handshake_perform(), highest_common_version(), parse_client_versions(), read_line(), send_hello(), send_nack(), write_all(), client_conn_t

### Community 8 - "Command Listener"
Cohesion: 0.48
Nodes (7): apply_command(), apply_set_interval(), apply_trigger_alert(), msg_queue_t, radio_control_t, log_fprintf(), FILE

## Knowledge Gaps
- **8 isolated node(s):** `protobuf-c toolchain`, `Telemetry (protobuf message)`, `Alert (protobuf message)`, `Phase 4, stretch -- mock_client.py + tests`, `Phase 7, stretch -- message pool hardening` (+3 more)
  These have ≤1 connection - possible missing edges or undocumented components.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `main()` connect `Message Queue Core` to `Module File Structure`, `Pub/Sub Topic Registry`, `Transport Connection Serving`, `Producer Loop & Runtime Controls`, `Command Listener`?**
  _High betweenness centrality (0.096) - this node is a cross-community bridge._
- **Why does `log_fprintf()` connect `Command Listener` to `Module File Structure`, `Message Queue Core`, `Pub/Sub Topic Registry`, `Transport Connection Serving`, `Producer Loop & Runtime Controls`, `Version Handshake`?**
  _High betweenness centrality (0.059) - this node is a cross-community bridge._
- **Why does `handshake_perform()` connect `Version Handshake` to `Command Listener`, `Pub/Sub Topic Registry`?**
  _High betweenness centrality (0.049) - this node is a cross-community bridge._
- **Are the 16 inferred relationships involving `main()` (e.g. with `log_fprintf()` and `log_printf()`) actually correct?**
  _`main()` has 16 INFERRED edges - model-reasoned connections that need verification._
- **Are the 3 inferred relationships involving `handshake_perform()` (e.g. with `log_fprintf()` and `log_printf()`) actually correct?**
  _`handshake_perform()` has 3 INFERRED edges - model-reasoned connections that need verification._
- **Are the 9 inferred relationships involving `log_fprintf()` (e.g. with `apply_command()` and `apply_set_interval()`) actually correct?**
  _`log_fprintf()` has 9 INFERRED edges - model-reasoned connections that need verification._
- **Are the 8 inferred relationships involving `transport_routine()` (e.g. with `handshake_perform()` and `log_fprintf()`) actually correct?**
  _`transport_routine()` has 8 INFERRED edges - model-reasoned connections that need verification._