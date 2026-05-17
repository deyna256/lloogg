# lloogg

Event logging server with persistent ring buffers per user.

## Prerequisites

- Rust 1.81+
- `just` — command runner (`cargo install just` or `apt install just`)

## Quick start

```bash
# Build and start the server
just run-dev

# In another terminal — PUSH some events
python3 -c "
import socket, struct
s = socket.socket()
s.connect(('127.0.0.1', 7379))
# PUSH uid=42, event_type=1, timestamp=1000, url_hash=2000
frame = struct.pack('<HBIB', 0xAE01, 0x01, 42, 17) + struct.pack('<BQQ', 1, 1000, 2000)
s.sendall(frame)
print('PUSH:', s.recv(5).hex())
# GET last 3 events for uid=42
frame = struct.pack('<HBIBH', 0xAE01, 0x02, 42, 2, 3)
s.sendall(frame)
resp = s.recv(1024)
print('GET:', resp.hex())
"
```

## Build

```bash
# Default COMPILED_N=100
just build

# Custom COMPILED_N (must match max_records in config)
just build LLOOGG_N=500

# Debug build
just build-dev
```

## Run

```bash
# Release mode (config: lloogg.toml)
just run

# Debug mode (config: lloogg.dev.toml)
just run-dev
```

By default the server listens on `127.0.0.1:7379`. Configure via `lloogg.toml`.

## Tests

```bash
just test               # unit tests
just test-integration   # integration tests
```

## Config

See `lloogg.toml`:

| Key | Default | Description |
|-----|---------|-------------|
| `max_records` | `100` | Ring buffer capacity per user (must equal `COMPILED_N`) |
| `ttl_seconds` | `3600` | Time-to-live for idle user buffers |
| `aof_path` | `data/lloogg.aof` | Append-only file path |
| `snapshot_path` | `data` | Snapshot directory |
| `snapshot_interval` | `120` | Snapshot interval in seconds |
| `listen_port` | `7379` | TCP listen port |
| `memory_pool_slots` | `1200000` | Pre-allocated slots (used when `COMPILED_N > 200`) |
| `aof_fsync` | `"everysec"` | `"always"` / `"everysec"` / `"no"` |

## Protocol

Binary TCP protocol on port 7379 (default):

| Opcode | Name  | Direction | Payload |
|--------|-------|-----------|---------|
| `0x01` | PUSH  | client→server | 17 bytes: event_type(1) + timestamp(8) + url_hash(8) |
| `0x02` | GET   | client→server | 2 bytes: count (1..COMPILED_N) |
| `0x03` | DEL   | client→server | (none) |

### PUSH frame (26 bytes)
```
[magic:2][opcode:1][uid:4][payload_len:2][event_type:1][timestamp:8][url_hash:8]
```

### GET frame (11 bytes)
```
[magic:2][opcode:1][uid:4][payload_len:2][count:2]
```

### GET response (7 + count*17 bytes)
```
[magic:2][status:1][payload_len:2][count:2][records:count*17]
```

### Wire magic: `0xAE01`

## Lint

```bash
just check
just clippy
```