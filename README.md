# lloogg

Event logging server with persistent ring buffers per user.

## Build

```bash
# Default COMPILED_N=100
cargo build --release

# Custom COMPILED_N (must match max_records in config)
LLOOGG_N=500 cargo build --release
```

## Run

```bash
cp lloogg.toml.example lloogg.toml
mkdir -p data
RUST_LOG=info ./target/release/lloogg
```

## Protocol

Binary TCP protocol on port 7379 (default):

| Opcode | Name  | Direction | Payload |
|--------|-------|-----------|---------|
| 0x01   | PUSH  | client→server | 17 bytes: event_type(1) + timestamp(8) + url_hash(8) |
| 0x02   | GET   | client→server | 2 bytes: count (1..COMPILED_N) |
| 0x03   | DEL   | client→server | (none) |

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

## Tests

```bash
cargo test                # unit tests
cargo test --test integration  # integration tests
```