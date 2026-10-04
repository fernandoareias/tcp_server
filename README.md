# tcp_server

A sharded TCP server written in C++23 for Linux. It runs one event loop per CPU core, pins each loop to its core, and uses coroutines for per-connection logic.

## Project layout

```
tcp_server/
├── CMakeLists.txt
└── src/
    ├── main.cpp                       entry point, reads the port from argv
    ├── core/
    │   ├── reactor.h / reactor.cpp    epoll event loop for one shard
    │   ├── shard_mesh.h               SPSC queues and wake-ups between shards
    │   ├── envelope_pool.h            pooled buffers for payloads
    │   └── task.h                     fire-and-forget coroutine type
    ├── net/
    │   ├── connection.h               per-socket state
    │   ├── connection_layer.h         session rules and frame routing
    │   └── connection_listener.h/.cpp starts the shards and handles signals
    ├── protocol/
    │   └── protocol.h                 framing (header-only)
    └── concurrency/
        ├── spsc_queue.h / .cpp        lock-free single-producer, single-consumer queue
        └── thread_pool_dispatcher.h/.cpp
```

Includes are relative to `src/`, for example `#include "core/reactor.h"`.

## Requirements

- Linux (uses `epoll`, `eventfd`, `SO_REUSEPORT` and CPU affinity)
- A C++23 compiler with coroutine support (GCC 13+ or Clang 17+)
- CMake 3.16 or newer

## Build

```sh
cmake -S . -B build/cli
cmake --build build/cli -j$(nproc)
```

The binary is created at `build/cli/tcp_server`.

## Run

```sh
./build/cli/tcp_server          # listens on port 8080
./build/cli/tcp_server 9000     # listens on port 9000
```

The server starts one shard per CPU core the process is allowed to use. Each shard has its own listening socket on the same port. Press `Ctrl+C` to stop. The server prints per-shard statistics on exit.

## How it works

### Shards

- `ConnectionListener` detects the allowed CPUs and creates one `Reactor` per CPU.
- Each reactor runs in its own thread, pinned to its CPU.
- Every reactor opens its own listening socket with `SO_REUSEPORT`, so the kernel spreads new connections across shards. No thread accepts connections and hands them off.
- Each reactor runs an `epoll` loop. Client sockets are edge-triggered.

### Connections and coroutines

- Each accepted connection gets a coroutine (`Reactor::serve`). It reads from the socket, parses frames, and parks itself when it has to wait for data or for the output buffer to drain.
- The coroutine owns the connection's lifetime. It closes the socket and releases the slot when it finishes.
- A connection that reads too fast without consuming its replies is paused until its output buffer drains.

### Wire protocol

Every frame has a 4-byte big-endian length followed by a 12-byte header and the payload.

```
+------------+------------------------------+---------+
| LENGTH u32 | HEADER (12 bytes)            | PAYLOAD |
+------------+------------------------------+---------+

HEADER:
  version   u8
  type      u8
  flags     u16
  sessionId u32
  streamId  u32
```

`LENGTH` counts the header and the payload, not itself. Frames larger than 16 MiB are rejected.

Frame types:

| Type  | Value  | Meaning                                  |
|-------|--------|------------------------------------------|
| Hello | `0x01` | Opens the session, sets `sessionId`      |
| Data  | `0x02` | Application payload                      |
| Bye   | `0x03` | Closes the connection                    |
| Reply | `0x82` | Sent by the server with the response     |

The protocol version is `1`. Frames with any other version are rejected.

### Session rules

`ConnectionLayer` checks each frame header:

- The first frame must be `Hello`. A second `Hello` on the same connection is rejected.
- A `Data` frame is rejected unless the connection has said `Hello` and its `sessionId` matches the one from `Hello`.
- Each session is assigned to a shard by `sessionId % number_of_shards`. All data for a session is handled by the same shard, so session state is never shared between threads.
- A `Bye` frame closes the connection.

### Routing payloads between shards

- If the target shard is the current one, the payload goes straight to the handler.
- Otherwise the payload is copied into a pooled envelope and pushed into a lock-free SPSC queue for the target shard. The target shard is woken with an `eventfd`.
- If a queue is full, the message waits in an overflow queue and is retried. The event loop never blocks.

### Handler

`main.cpp` passes a handler that echoes the payload back with `reactor.reply(...)`. The reply goes to the shard that owns the connection and is sent as a `Reply` frame. To change the behavior of the server, change the lambda in `main.cpp`.

## Notes

- `ThreadPoolDispatcher` is an empty placeholder and is not used by the server yet.
- `src/net/connection_layer.cpp` is not in the build. `connection_layer.h` contains the full implementation.
