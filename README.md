# Redis-compatible server in Zig

Maintained by **Derek Ko**.

An experimental in-memory server implementing a subset of the Redis protocol
and command set in Zig.

## Features

- RESP command parsing and concurrent client connections
- Strings, lists, sorted sets, streams, and geospatial operations
- Key expiration, blocking reads, and transaction command queues
- Publish/subscribe, authentication, and ACL commands
- Replication handshake, command propagation, and replica acknowledgments
- Loading supported RDB data from a configured file

## Build and run

Requires Zig 0.15.1 or 0.15.2. Run the server on the default port, 6379:

```sh
./your_program.sh
```

To use another port:

```sh
./your_program.sh --port 6380
```

Use `redis-cli -p 6380` to connect. Direct builds use `zig build`; the executable
is `zig-out/bin/main`. Source code is in `src/main.zig`.

This implements a subset of Redis behavior and is intended for local
experimentation. It is not a production Redis replacement.
