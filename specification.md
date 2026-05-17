# lloogg — Specification

## Table of Contents

1. [[#Overview]]
   - [[#Idea & Goal]]
   - [[#Scope]]
   - [[#Architectural Requirements]]
   - [[#Architectural Constraints]]
   - [[#Build Dependencies]]

2. [[#Entities & Data Types]]
   - [[#Record]]
   - [[#RingBuffer]]
   - [[#MemoryPool]]
   - [[#Store\<N\>|Store<N>]]
   - [[#Connection]]
   - [[#AOFEntry]]
   - [[#AOFWriter]]
   - [[#SnapshotFile]]
   - [[#SnapshotManager]]
   - [[#Candidate]]
   - [[#SPSCQueue]]
   - [[#TTLEvictionWorker]]
   - [[#Config]]
   - [[#FsyncMode]]
   - [[#Named Constants]]

3. [[#Processes & Functions]]
   - [[#EventLoop]]
   - [[#PushCommand]]
   - [[#GetCommand]]
   - [[#DelCommand]]
   - [[#AOFAppend]]
   - [[#AOFFlush]]
   - [[#SnapshotDump]]
   - [[#TTLCollect]]
   - [[#TTLEvict]]
   - [[#DrainExpiredKeys]]
   - [[#RecoveryLoad]]
   - [[#Startup]]
   - [[#ConfigLoad]]
   - [[#TimeOps]]
   - [[#RingBufferOps]]
   - [[#StoreOps]]
   - [[#MemoryPoolOps]]

4. [[#Public Contract]]
   - [[#PUSH]]
   - [[#GET]]
   - [[#DEL]]
   - [[#Wire Byte Order]]

---

## Overview

### Idea & Goal
lloogg is a single-threaded in-memory analytics engine that stores per-user event histories as bounded ring buffers, providing microsecond-latency writes (~11 μs end-to-end, ~100 ns in-memory) and sub-millisecond reads over a binary TCP protocol — purpose-built for internal service mesh event tracking.

### Scope

**In scope:**
- PUSH / GET / DEL operations over binary TCP wire protocol
- Per-user ring buffer storage keyed by `user_id` (uint32)
- AOF (Append-Only File) persistence with three configurable fsync modes
- Snapshot persistence via `fork()` + Linux Copy-on-Write
- TTL-based key eviction via background worker and SPSC queues
- Crash recovery: snapshot load + AOF replay
- Adaptive HashMap with compile-time capacity selection

**Non-goals:**
- Clustering, replication, or master-replica topology
- TLS, encryption, or authentication (internal service mesh only)
- SQL, query engine, filtering, or aggregation (PUSH/GET/DEL only)
- Client SDK or client library (raw TCP binary protocol only)
- Dynamic reallocation or schema changes at runtime

### Architectural Requirements

| Requirement | Value |
|-------------|-------|
| Write latency | < 1 ms |
| End-to-end request latency | ~11 μs (loopback, `everysec` AOF) |
| Single-core throughput | ~90,000 RPS |
| RPO (`everysec` AOF) | ≤ 1 second data loss |
| RTO (1 GB AOF + snapshot) | ≤ 3 minutes |
| Record wire/memory size | 17 bytes |
| Snapshot interval | 120 s |
| TTL eviction lag (worst-case) | ≤ TTL_COLLECT_INTERVAL_S (10 s) above configured `ttl_seconds`; every collect cycle walks the entire Store, so all keys are checked within one 10 s window regardless of Store size; callers MUST configure `ttl_seconds` such that this lag is tolerable |

### Architectural Constraints

- **Single-threaded event loop** — NO locks on hot path; all Store mutations on one thread
- **No TLS** — internal service mesh deployment; plaintext TCP only
- **No dynamic allocation on hot path** — `StoreAlloc::Slab` (pre-allocated via `std::alloc::alloc`) for N ≤ 200; [MemoryPool](#memorypool) (`mmap MAP_POPULATE`) for N > 200; in both cases all slots are allocated at startup and `RingBuffer.data` always points to an external slot — never an inline array
- **Store access isolation** — `Store<N>` MUST only be accessed from event loop thread
- **TTL worker isolation** — [TTLEvictionWorker](#ttlevictionworker) MUST NOT access Store directly; SPSC queues only
- **AOF non-blocking** — event loop performs memcpy only; fdatasync runs on flush thread
- **Linux-only** — uses `epoll(7)`, `fork(2)` + CoW, `MAP_POPULATE`

### Build Dependencies

| Dependency | Version | Usage |
|------------|---------|-------|
| Rust edition | 2021 | All source |
| Linux kernel | ≥ 2.6.17 | `epoll`, `MAP_POPULATE`; ≥ 3.9 for `EPOLLEXCLUSIVE` (not used) |
| [hashbrown](https://crates.io/crates/hashbrown) | ≥ 0.15 | `hashbrown::HashMap` — used for all N |
| [libc](https://crates.io/crates/libc) | ≥ 0.2 | Linux syscall bindings |
| [log](https://crates.io/crates/log) / [env_logger](https://crates.io/crates/env_logger) | ≥ 0.4 / ≥ 0.11 | Logging |
| [serde](https://crates.io/crates/serde) + [toml](https://crates.io/crates/toml) | ≥ 1.0 / ≥ 0.8 | Config deserialization |
| [thiserror](https://crates.io/crates/thiserror) | ≥ 1.0 | Error types |
| Cargo | ≥ 1.70 | Build system |

> **Note:** `COMPILED_N` is set at build time via the `LLOOGG_N` environment variable (e.g. `LLOOGG_N=255 cargo build`). The `build.rs` script reads this variable, defaults to 100 if absent, and emits `generated_constants.rs` containing `pub const COMPILED_N: usize = N;`. All compile-time capacity decisions derive from this constant.

---

## Entities & Data Types

### Record

| Field | Type | Justification |
|-------|------|---------------|
| `event_type` | uint8 | → §1 req: 256 event categories; 1 B avoids any padding |
| `timestamp` | uint64 | → §1 req: event ordering and client-side time attribution; unix seconds; NOT used for server-side TTL (TTL uses `AOFEntry.write_ts`) |
| `url_hash` | uint64 | → §1 req: URL/project tracking per event; fixed-width hash avoids variable-length strings |

**Invariants:**
- `size_of::<Record>() == 17` and `align_of::<Record>() == 1` enforced by compile-time `assert!`
- Wire format == in-memory format (`#[repr(C, packed)]`, no padding)
- `user_id` is NOT stored inside Record — it lives as the HashMap key
- 3 Records fit in one 64-byte CPU cache line (3 × 17 = 51 B < 64 B)

**Relationships:** Contained in `RingBuffer.data[]`.

---

### RingBuffer

| Field | Type | Justification |
|-------|------|---------------|
| `data` | `*mut Record` | → §1 req: per-user event log; points to a [StoreAlloc](#storen) Slab slot (N ≤ 200) or [MemoryPool](#memorypool) slot (N > 200); never an inline array |
| `capacity` | uint16 | → §1 constraint: bounded memory per key; equals compile-time N; practical maximum = MAX_RECORDS_PROTOCOL_LIMIT (3854) enforced by static_assert |
| `head` | uint16 | → §1 req: O(1) write via modular advance; always in [0, capacity) |
| `count` | uint16 | → §1 req: correct GET response before buffer is full; range [0, capacity] |
| `last_write_ts` | uint64 | → §1 req: TTL false-eviction guard; server-assigned unix nanoseconds at the moment of PUSH (from `AOFEntry.write_ts`), NOT from `Record.timestamp` — isolates TTL from client-controlled data |

**Invariants:**
- `head` is always in range [0, capacity)
- `count` is always in range [0, capacity]
- `last_write_ts` equals the **server-side** `write_ts` of the most recently appended [AOFEntry](#aofentry), never `Record.timestamp`; this prevents client-supplied future/past timestamps from affecting eviction
- `data` is never null after initialization
- When `count == capacity`, next push silently overwrites oldest entry; no allocation, no GC

**Relationships:** One RingBuffer per `user_id` in [Store\<N\>](#storen); stored inline (N ≤ 200) or via [MemoryPool](#memorypool) (N > 200).

---

### MemoryPool

| Field | Type | Justification |
|-------|------|---------------|
| `base` | `void*` | → §1 constraint: single `mmap()` at init; no dynamic alloc on hot path |
| `slot_size` | `size_t` | → §1 req: fixed = `max_records × sizeof(Record)`; no fragmentation |
| `free_list` | intrusive ptr | → §1 req: O(1) alloc/free; first 8 bytes of each free slot store next-pointer |
| `total_slots` | `size_t` | → [Config](#config): = `memory_pool_slots` |

**Invariants:**
- Allocated once at startup with `MAP_POPULATE` — zero page-fault latency on hot path
- `slot_size` never changes at runtime
- All slots are the same size — no fragmentation possible
- `alloc_slot()` is O(1); `free_slot()` is O(1)

**Relationships:** Used only when N > 200 in [Store\<N\>](#storen).

---

### Store\<N\>

| Field | Type | Justification |
|-------|------|---------------|
| `map` | `hashbrown::HashMap<u32, RingBuffer>` | → §1 req: open-addressing hash map used for ALL N; stores RingBuffer values directly (pointer to external slot inside each value) |
| `alloc` | `StoreAlloc` (enum) | → §1 constraint: `StoreAlloc::Slab` (pre-allocated via `std::alloc::alloc`) for N ≤ 200; `StoreAlloc::Pool(MemoryPool)` (mmap MAP_POPULATE) for N > 200 |
| `capacity` | `u16` | → §1 req: = COMPILED_N; cached to avoid reading global on hot path |

**StoreAlloc variants:**

| Variant | When | Backing |
|---------|------|---------|
| `Slab { base, layout, free_list, … }` | N ≤ 200 | Contiguous heap allocation via `std::alloc::alloc`; intrusive free list; O(1) alloc/free |
| `Pool(MemoryPool)` | N > 200 | `mmap(MAP_POPULATE)`; intrusive free list; O(1) alloc/free |

**Invariants:**
- COMPILED_N is fixed at build time via `LLOOGG_N` env var; runtime `Config.max_records` must match
- All map access is ONLY from event loop thread (single-threaded invariant)
- `for_each` callback executes in event loop thread context
- `RingBuffer.data` is always a pointer to an external slot; never an inline embedded array

**Relationships:** Contains all [RingBuffer](#ringbuffer) instances; mutated by [EventLoop](#eventloop); read-only by SnapshotChild (forked CoW copy).

---

### Connection

| Field | Type | Justification |
|-------|------|---------------|
| `fd` | int | → §1 req: epoll-registered file descriptor |
| `state` | `State` enum | → §1 req: non-blocking I/O state machine |
| `rbuf[RBUF_SIZE]` | uint8[32] | → §1 req: max request = 9 B header + 17 B payload (PUSH) = 26 B; GET = 9 B + 2 B = 11 B; 32 B rounded |
| `rbuf_pos` | size_t | → §1 req: partial read tracking for non-blocking I/O |
| `wbuf` | `Vec<u8>` (capacity WBUF_SIZE) | → §1 req: max GET response = 5 B + 2 B + N×17 B; WBUF_SIZE = `⌈(7 + N×17) / 64⌉ × 64` — compile-time derived from N; pre-allocated in `Connection::new` via `Vec::with_capacity(WBUF_SIZE)` + `set_len`, no allocation on hot path |
| `wbuf_len` | size_t | → §1 req: total bytes pending write |
| `wbuf_pos` | size_t | → §1 req: partial write tracking for non-blocking I/O |

**Invariants:**
- `rbuf` never overflows: max protocol request is 9 B header + 17 B payload = 26 B < RBUF_SIZE (32 B); GET request is 9 B + 2 B = 11 B < 32 B
- `wbuf` never overflows: max GET response = 5 B + 2 B + N×17 B = WBUF_SIZE by construction (compile-time guarantee)
- State transitions are strictly sequential: `READ_HEADER → READ_PAYLOAD → PROCESS → WRITE_RESPONSE → READ_HEADER`
- `READ_PAYLOAD` is entered even when `payload_len = 0` (e.g., DEL); in this case the state immediately transitions to `PROCESS` without waiting for any bytes — `rbuf_pos` stays unchanged from after header read

**State values:** `READ_HEADER`, `READ_PAYLOAD`, `PROCESS`, `WRITE_RESPONSE`

**Relationships:** One per accepted TCP connection; owned and managed exclusively by [EventLoop](#eventloop).

---

### AOFEntry

| Field | Type | Justification |
|-------|------|---------------|
| `opcode` | uint8 | → §1 req: identifies PUSH (0x01) or DEL (0x03) for replay |
| `user_id` | uint32 | → §1 req: HashMap key for replay |
| `write_ts` | uint64 | → §1 req: AOF replay cutoff; skip if `write_ts ≤ snapshot_ts`; unix nanoseconds — nanosecond precision eliminates the replay window race (event loop is single-threaded: no write can occur during `fork()`, so all post-fork writes have `write_ts > snapshot_ts` by construction) |
| `payload_len` | uint16 | → §1 req: variable-length payload framing |
| `payload` | bytes | → §1 req: [Record](#record) (17 B) for PUSH, empty for DEL |

**Invariants:**
- `write_ts` equals event loop wall clock at append time in **unix nanoseconds** (`CLOCK_REALTIME` or `CLOCK_TAI`); uint64 sufficient until year 2554
- Minimum entry size: 15 B (header only, payload_len = 0)
- Truncated entry at EOF during replay → stop iteration; not a data loss (client never received ACK)
- Only PUSH and DEL are written to AOF; GET has no side effects

**Relationships:** Written to `AOFWriter.buf_`; read during [RecoveryLoad](#recoveryload).

---

	### AOFWriter

| Field | Type | Justification |
|-------|------|---------------|
| `buf` | `Arc<AOFBuf>` (`UnsafeCell<Box<[u8; AOF_BUF_SIZE]>>`) | → §1 constraint: AOF non-blocking; ring buffer absorbs write bursts without stalling event loop; boxed to avoid stack overflow; `Arc` for sharing with flush thread |
| `head` | `Arc<AtomicU64>` | → §1 constraint: single-writer (event loop); release store makes entry visible to flush thread |
| `tail` | `Arc<AtomicU64>` | → §1 constraint: single-consumer (flush thread); **release store** after write — event loop acquire-loads `tail` to check free space; release/acquire pair required for correct visibility on weakly-ordered architectures |
| `aof_fd` | `RawFd` | → §1 req: AOF file descriptor opened at startup; used by flush thread (`everysec`/`no` modes); used directly by event loop thread in `always` mode (write + fdatasync per entry, bypassing ring buffer) |
| `fsync_mode` | [FsyncMode](#fsyncmode) | → §1 req: configurable durability; controls whether flush loop calls fdatasync |
| `running` | `Arc<AtomicBool>` | → §1 req: cooperative shutdown (`everysec`/`no` modes only); `store(false, Release)` by EventLoop on SIGTERM; flush thread loads with Acquire in loop condition; in `always` mode no flush thread is started at all |
| `flush_thread` | `Option<JoinHandle<()>>` | → §1 req: handle to flush thread; `None` in `always` mode (no flush thread started) |

**Invariants:**
- `head` is written only by event loop thread; `tail` is written only by flush thread — no lock needed on either path
- `(head - tail) ≤ AOF_BUF_SIZE` at all times — enforced by spin in [AOFAppend](#aofappend) before every write
- `head` stored with release ordering after the **last** memcpy fragment — flush thread must not load `head` until both fragments of a wrap-around entry are written; release store on `head` guarantees this
- An entry straddling the `buf` boundary is written in two `ptr::copy_nonoverlapping` calls by [AOFAppend](#aofappend); flushed in two sequential `write(2)` calls by [AOFFlush](#aofflush) — bytes land contiguously on disk
- `aof_fd` is opened at startup and closed only at shutdown; never inherited by snapshot child (`O_CLOEXEC`)
- In `always` mode `aof_fd` is accessed exclusively from the event loop thread (ring buffer and flush thread are bypassed entirely); in `everysec`/`no` modes it is accessed exclusively from the flush thread — no concurrent access in either case

**Shutdown sequence:**
1. EventLoop signals shutdown → `running.store(false, Release)`
2. `flush_thread.join()` — blocks until flush loop exits naturally (after one final drain pass)
3. After join completes: `flush_sync()` — synchronous drain of any remaining bytes from `buf` (handles race between last `running` check and loop exit)
4. `close(aof_fd)` — guaranteed all bytes are on disk (or in OS page cache for `everysec`/`no` modes)
5. Destroy `AOFWriter` only after `aof_fd` is closed (`Drop` impl calls `shutdown()` if `aof_fd >= 0`)

**Relationships:** Written by [AOFAppend](#aofappend) (event loop thread); drained to disk by [AOFFlush](#aofflush) (flush thread); replayed by [RecoveryLoad](#recoveryload) at startup.

---

### SnapshotFile

| Field | Type | Justification |
|-------|------|---------------|
| `magic` | uint32 | → §1 req: file integrity check; value = MAGIC_SNAPSHOT |
| `version` | uint8 | → §1 req: future format evolution without breaking recovery |
| `snapshot_ts` | uint64 | → §1 req: AOF replay cutoff; unix nanoseconds at the moment `fork()` was called; nanosecond precision ensures `write_ts > snapshot_ts` for all post-fork writes |
| `key_count` | uint32 | → §1 req: bounded iteration during load |
| `user_id` (per key) | uint32 | → §1 req: HashMap key |
| `last_write_ts` (per key) | uint64 | → §1 req: restore `RingBuffer.last_write_ts` after snapshot load; without it, all keys would have `last_write_ts=0` and be immediately TTL-evicted if no AOF entries follow |
| `record_cnt` (per key) | uint16 | → §1 req: record count to read per key |
| `records` (per key) | `Record[]` | → §1 req: serialized ring buffer contents |

**Invariants:**
- File is always complete or absent — atomic `rename(snapshot.tmp, snapshot.bin)` per POSIX
- `snapshot_ts` is the unix nanosecond timestamp captured immediately before `fork()` is called; all AOF entries written after `fork()` have `write_ts > snapshot_ts` by construction (single-threaded event loop)
- `last_write_ts` per key equals `RingBuffer.last_write_ts` at fork time (server-assigned nanoseconds, not client Record.timestamp)
- Records stored **oldest-first** (push order: oldest entry first, newest entry last); this allows RecoveryLoad to call `RingBuffer::push` in file order and reconstruct the correct ring buffer state — pushing newest-first would invert `get_last` output
- `key_count` entries follow the file header with no gaps

**Relationships:** Written by [SnapshotDump](#snapshotdump) process; read by [RecoveryLoad](#recoveryload) process at startup.

---

### SnapshotManager

| Field | Type | Justification |
|-------|------|---------------|
| `child_pid_` | pid_t | → §1 req: tracks forked child to prevent concurrent snapshots; checked with `waitpid(WNOHANG)` before every fork |
| `last_snapshot_ts_` | uint64 | → §1 req: determines when next snapshot is due; unix nanoseconds captured immediately before `fork()` — same value written to [SnapshotFile](#snapshotfile).`snapshot_ts` |
| `snapshot_path_` | string | → §1 req: base path for `snapshot.bin` and `snapshot.tmp`; from [Config](#config) |

**Invariants:**
- At most one snapshot child process alive at any time — `waitpid(WNOHANG)` check is mandatory before `fork()`
- `last_snapshot_ts_` is set to unix nanoseconds immediately before `fork()` is called, not when child completes; this value is written into [SnapshotFile](#snapshotfile).`snapshot_ts` by the child
- Child always calls `_exit(0)`, never `exit()` — prevents flushing parent's stdio buffers
- `snapshot_path_` never changes at runtime

**Relationships:** Called by [EventLoop](#eventloop) (`trigger_snapshot`); forks child that reads [Store\<N\>](#storen) via CoW; child writes [SnapshotFile](#snapshotfile); `load()` called by [RecoveryLoad](#recoveryload) at startup.

---

### Candidate

| Field | Type | Justification |
|-------|------|---------------|
| `uid` | uint32 | → §1 req: identifies the key to check for expiry |
| `last_write_ts` | uint64 | → §1 req: false-eviction guard; compared against live `RingBuffer.last_write_ts` |

**Invariants:**
- `last_write_ts` is a snapshot of `RingBuffer.last_write_ts` at collection time
- If `RingBuffer.last_write_ts != Candidate.last_write_ts` at drain time, eviction is cancelled — zero false evictions

**Relationships:** Flows [EventLoop](#eventloop) → candidates_in_ [SPSCQueue](#spscqueue) → [TTLEvictionWorker](#ttlevictionworker) → expired_keys_out_ [SPSCQueue](#spscqueue) → [EventLoop](#eventloop).

---

### SPSCQueue

| Field | Type | Justification |
|-------|------|---------------|
| `buf_` | T[Cap] | → §1 constraint: no dynamic allocation on hot path; bounded ring pre-allocated at startup |
| `head_` | atomic\<uint64\> | → §1 constraint: single-producer write index; release store makes item visible to consumer |
| `tail_` | atomic\<uint64\> | → §1 constraint: single-consumer read index; **release store** after consuming item — producer acquire-loads `tail_` to check free space; relaxed store would allow stale reads on weakly-ordered architectures (ARM), causing producer to see a full queue when space is available |

**Invariants:**
- `Cap` MUST be a power of two — enforced by `static_assert(Cap && (Cap & (Cap - 1)) == 0)`; guarantees `head_ % Cap` compiles to a bitmask (`head_ & (Cap - 1)`) with zero division cost
- Exactly one producer thread and one consumer thread — no locks, no CAS needed
- `push()` drops item silently and returns false if full — caller handles backpressure (next collect cycle re-samples)
- `pop()` returns false immediately if empty — never blocks
- `head_` written only by producer (release store); read by consumer (acquire load)
- `tail_` written only by consumer (release store); read by producer (acquire load) — release/acquire pair required on weakly-ordered architectures

**Concrete instances:**

| Instance | T | Cap | Producer | Consumer |
|----------|---|-----|----------|----------|
| `candidates_in_` | [Candidate](#candidate) | CANDIDATES_IN_CAP (8192) | [EventLoop](#eventloop) | [TTLEvictionWorker](#ttlevictionworker) |
| `expired_keys_out_` | [Candidate](#candidate) | EXPIRED_KEYS_CAP (4096) | [TTLEvictionWorker](#ttlevictionworker) | [EventLoop](#eventloop) |

**Relationships:** Bridges [EventLoop](#eventloop) ([Store\<N\>](#storen)-owning thread) and [TTLEvictionWorker](#ttlevictionworker) (eviction thread) without shared memory or locks.

---

### TTLEvictionWorker

| Field | Type | Justification |
|-------|------|---------------|
| `running` | `Arc<AtomicBool>` | → §1 req: cooperative shutdown; `store(false, Release)` by caller (`stop()`); eviction loop loads with Acquire in spin condition |
| `thread` | `Option<JoinHandle<()>>` | → §1 req: handle to background eviction thread; `take()`d in `stop()` to allow join |

> **Note:** The SPSC queues and `ttl_ns` are NOT stored as struct fields. They are passed by `Arc` to the `TTLEvictionWorker::new()` constructor and captured by move into the spawned thread closure. The struct owns only the control primitives needed for shutdown.

**Invariants:**
- NEVER accesses [Store\<N\>](#storen) directly — all Store mutations happen exclusively on [EventLoop](#eventloop) thread
- Runs on a dedicated background thread; `eviction_loop()` spins until `running_.load(acquire) == false`
- Eviction condition: `now_ns() - candidate.last_write_ts > ttl_ns_` — all values in unix nanoseconds; no unit conversion on hot path
- Only writes to `expired_keys_out_`; never reads from it; never writes to `candidates_in_`

**Shutdown sequence:**
1. EventLoop receives SIGTERM → `running_.store(false, release)`
2. EventLoop calls `thread_.join()` — blocks until `eviction_loop()` exits naturally
3. Only after join completes: SPSC queues, Store, MemoryPool destroyed
- No forced kill; `eviction_loop()` drains at most one batch then exits on next spin check

**Relationships:** Consumes [Candidate](#candidate)s from `candidates_in_` (fed by [EventLoop](#eventloop)); produces expired [Candidate](#candidate)s into `expired_keys_out_` (drained by [EventLoop](#eventloop) via [DrainExpiredKeys](#drainexpiredkeys)).

---

### Config

| Field | Type | Justification |
|-------|------|---------------|
| `max_records` | uint16 | → §1 req: compile-time N; [RingBuffer](#ringbuffer) capacity per user |
| `ttl_seconds` | uint64 | → §1 req: key eviction threshold in seconds |
| `aof_path` | string | → §1 req: AOF file location on disk |
| `snapshot_path` | string | → §1 req: snapshot binary location on disk |
| `snapshot_interval` | uint32 | → §1 req: seconds between automatic snapshots; default = SNAPSHOT_INTERVAL_S |
| `listen_port` | uint16 | → §1 req: TCP listen port; default = PORT_DEFAULT |
| `memory_pool_slots` | uint32 | → §1 constraint: pre-allocated [MemoryPool](#memorypool) slots (used only when N > 200) |
| `aof_fsync` | `FsyncMode` | → §1 req: durability vs latency tradeoff; values: `always`, `everysec`, `no` |

**FsyncMode values and behavior:** see [FsyncMode](#fsyncmode).

**Invariants:**
- `max_records` value at runtime MUST equal the compiled N; checked at startup before any subsystem initializes: if `config.max_records != COMPILED_N` → log fatal + `exit(1)`; never silently proceed with mismatched N (would cause silent memory corruption in RingBuffer slot sizing)
- `max_records` MUST NOT exceed MAX_RECORDS_PROTOCOL_LIMIT (3854); `COMPILED_N > 3854` is a compile-time error enforced by `static_assert(COMPILED_N <= MAX_RECORDS_PROTOCOL_LIMIT)`; runtime check `config.max_records > 3854` → `exit(1)` is a secondary guard for config/binary mismatch
- `listen_port` default = PORT_DEFAULT (7379)
- `snapshot_interval` default = SNAPSHOT_INTERVAL_S (120 s)
- `memory_pool_slots` default = MEMORY_POOL_SLOTS_DEFAULT (when N > 200)

---

### FsyncMode

| Value | Mechanism | Max data loss |
|-------|-----------|---------------|
| `always` | Event loop calls `write(aof_fd_) + fdatasync(aof_fd_)` synchronously per entry; bypasses ring buffer and flush thread; +50–200 μs/op latency | 0 |
| `everysec` | Ring buffer + flush thread; `write(2)` each flush iteration; `fdatasync(2)` every AOF_FLUSH_INTERVAL_MS | ≤ 1 s |
| `no` | Ring buffer + flush thread; `write(2)` each flush iteration; no `fdatasync(2)` | OS crash = loss |

> **Performance note:** The ~11 μs end-to-end latency and ~90 000 RPS throughput figures stated in [Architectural Requirements](#architectural-requirements) are benchmarked under `everysec` mode. Under `always` mode, each PUSH blocks the event loop on fdatasync (~50–200 μs depending on storage), reducing throughput to ~5 000–20 000 RPS. Use `always` only when zero data loss is a hard requirement.

**Invariants:**
- Set at startup via `Config.aof_fsync`; never changed at runtime
- Determines whether and how often [AOFFlush](#aofflush) calls `fdatasync(2)`

**Relationships:** Stored in `Config.aof_fsync` and `AOFWriter.fsync_mode_`; governs [AOFFlush](#aofflush) behaviour.

---

### Named Constants

| Constant | Value | Defined in |
|----------|-------|------------|
| `COMPILED_N` | compile-time uint16 | [Store\<N\>](#storen) / [Config](#config) — the value of N baked into the binary at build time; must equal `Config.max_records` at startup |
| `PORT_DEFAULT` | 7379 | Config defaults |
| `AOF_BUF_SIZE` | 4,194,304 (4 MB) | [AOFWriter](#aofwriter) |
| `AOF_FLUSH_INTERVAL_MS` | 1000 | [AOFWriter](#aofwriter) |
| `AOF_OVERFLOW_WARN_S` | 5 | [AOFWriter](#aofwriter) |
| `SNAPSHOT_INTERVAL_S` | 120 | [SnapshotManager](#snapshotmanager) |
| `TTL_COLLECT_INTERVAL_S` | 10 | [EventLoop](#eventloop) |
| `CANDIDATES_IN_CAP` | 8192 | [TTLEvictionWorker](#ttlevictionworker) |
| `EXPIRED_KEYS_CAP` | 4096 | [TTLEvictionWorker](#ttlevictionworker) |
| `MAX_EPOLL_EVENTS` | 1024 | [EventLoop](#eventloop) |
| `RBUF_SIZE` | 32 | [Connection](#connection) |
| `WBUF_SIZE` | `⌈(7 + N×17) / 64⌉ × 64` | [Connection](#connection) — compile-time derived from N; e.g. N=255→4352 B, N=500→8544 B, N=1000→17024 B |
| `MAGIC_WIRE` | 0xAE01 | Wire protocol |
| `MAGIC_SNAPSHOT` | 0x574C4F58 | [SnapshotFile](#snapshotfile) |
| `SNAPSHOT_VERSION` | 1 | [SnapshotFile](#snapshotfile) |
| `OPCODE_PUSH` | 0x01 | Wire protocol |
| `OPCODE_GET` | 0x02 | Wire protocol |
| `OPCODE_DEL` | 0x03 | Wire protocol |
| `STATUS_OK` | 0x00 | Wire protocol |
| `STATUS_NOT_FOUND` | 0x02 | Wire protocol |
| `MEMORY_POOL_SLOTS_DEFAULT` | 1,200,000 | Config defaults |
| `MAX_RECORDS_PROTOCOL_LIMIT` | 3854 | Wire protocol — derived: `⌊(65535 − 2) / 17⌋`; max N for which GET response `payload_len` (uint16) does not overflow; startup `exit(1)` if `max_records > MAX_RECORDS_PROTOCOL_LIMIT` |
| `LISTEN_BACKLOG` | 128 | [EventLoop](#eventloop) — `listen(2)` backlog; controls depth of the OS accept queue |
| `CONFIG_PATH_DEFAULT` | `"lloogg.toml"` | [ConfigLoad](#configload) — default config file path when no CLI argument is provided |

---

## Processes & Functions

### EventLoop

**Input:** [Config](#config) — server configuration at startup
**Output:** void (runs until SIGTERM/SIGINT)
**Invariants:**
- All [Store\<N\>](#storen) mutations occur exclusively on this thread — no exceptions
- Per-tick order is fixed: `drain_expired_keys` → `collect_ttl_candidates` (if due) → `trigger_snapshot` (if due) → `epoll_wait`
- `epoll_wait` timeout ≤ TTL_COLLECT_INTERVAL_S × 1000 ms so periodic tasks fire on schedule
- Maximum MAX_EPOLL_EVENTS (1024) events processed per epoll_wait call
- Protocol violations (magic ≠ MAGIC_WIRE, unknown opcode, invalid payload_len) → `handle_close` called immediately; no response is sent

**SIGTERM/SIGINT shutdown sequence:**
1. Signal handler sets a `sig_atomic_t shutdown_flag = 1` (async-signal-safe); EventLoop checks flag at top of each tick
2. `close(listen_fd)` — stops accepting new connections
3. Finish the current `epoll_wait` batch (at most MAX_EPOLL_EVENTS events): complete any in-progress `WRITE_RESPONSE` state (flush pending `wbuf`) for each connection; connections in `READ_HEADER` / `READ_PAYLOAD` / `PROCESS` are closed without response — client receives TCP RST and must retry
4. Call `handle_close` on all remaining open connections
5. `TTLEvictionWorker::running_.store(false, release)` → `thread_.join()`
6. `AOFWriter::aof_running_.store(false, release)` → final flush → `thread_.join()`
7. `waitpid(snapshot_child, WNOHANG)` — reap child if already done; if still running, wait (child holds no locks and exits via `_exit(0)`)
8. Destroy Store, MemoryPool, AOFWriter in that order

**State:** `last_ttl_collect` : u64 — unix nanosecond timestamp of the last TTL collect cycle; initialized to 0; updated each time `collect_ttl_candidates` runs; owned exclusively by EventLoop thread. (There is no cursor-based pagination — every collect cycle iterates the full Store.)

**Side Effects:** Reads/writes [Store\<N\>](#storen); reads [SPSCQueue](#spscqueue) queues; writes `Connection.wbuf`; accepts TCP connections; forks child for snapshots.

#### Functions
- `accept_connections() → void` — loops `accept4(listen_fd, SOCK_NONBLOCK | SOCK_CLOEXEC)` until EAGAIN; sets `TCP_NODELAY` on each accepted fd; `epoll_ctl EPOLL_CTL_ADD EPOLLIN`; inserts `Connection::new(fd)` into connections map
- `handle_read(Connection*) → void` — read into `rbuf`, advance state machine; after the 9 B header is fully read (transition out of `READ_HEADER`): validate `magic == MAGIC_WIRE`, `opcode ∈ {OPCODE_PUSH, OPCODE_GET, OPCODE_DEL}`, and `payload_len` against expected value per opcode (PUSH: 17, GET: 2, DEL: 0); also validate GET `count` field (must be in [1, COMPILED_N]) after `READ_PAYLOAD` completes; any violation → `handle_close` immediately, no response sent
- `handle_write(Connection*) → void` — write from `wbuf`, re-arm EPOLLIN when wbuf drained
- `handle_close(Connection*) → void` — close(fd), epoll_ctl EPOLL_CTL_DEL, free [Connection](#connection)
- `drain_expired_keys() → void` — pop all [Candidate](#candidate)s from `expired_keys_out_`; guard check; conditionally erase from [Store\<N\>](#storen)
- `collect_ttl_candidates() → void` — every TTL_COLLECT_INTERVAL_S (checked by comparing `now_ns()` against `last_ttl_collect`): iterates the **entire** Store via `Store::for_each`, pushing `{uid, last_write_ts}` [Candidate](#candidate)s to `candidates_in_`; items are silently dropped if the queue is full (next cycle will re-sample); updates `last_ttl_collect`; no cursor — full Store is walked every cycle
- `trigger_snapshot() → void` — if `now_ns() - last_snapshot_ts_ ≥ Config.snapshot_interval × 1_000_000_000` and no child running: call `SnapshotManager::maybe_snapshot()`; unit conversion (s → ns) done inline, not cached

---

### PushCommand

**Input:** [Connection](#connection)* (rbuf holds 9 B header + 17 B [Record](#record)), [Store\<N\>](#storen)&
**Output:** void (fills `Connection.wbuf` with 5 B STATUS_OK response)
**Invariants:**
- `get_or_create` called before `push` — user always has a [RingBuffer](#ringbuffer) after PUSH
- `RingBuffer.last_write_ts` updated to the server-side `write_ts` (unix nanoseconds from event loop clock), passed alongside `Record` into `push` — NOT `Record.timestamp`
- AOF entry written to `AOFWriter` ring buffer BEFORE response sent (write-ahead ordering); durability guarantee depends on [FsyncMode](#fsyncmode): `always` → ACK implies fsync'd to disk; `everysec` → ACK implies in-memory AOF buffer, ≤ 1 s data loss on crash; `no` → OS decides
- Total PUSH path latency: ~5–15 μs (dominated by socket I/O)

**Side Effects:** Mutates [Store\<N\>](#storen); memcpy to `AOFWriter.buf_`; sets `Connection.wbuf` and `wbuf_len`.

#### Functions
Steps execute **strictly in this order**:
1. `write_ts = now_ns()` — captured **once** before any AOF or Store mutation; passed to both `append_push` and `push` to guarantee `RingBuffer.last_write_ts == AOFEntry.write_ts`; separate `now_ns()` calls would produce different values and break RecoveryLoad
2. `AOFWriter::append_push(uid, record, write_ts) → void` — WAL step: log before apply; behaviour depends on `fsync_mode`: `Always` → `write+fdatasync` directly from event loop; `Everysec`/`No` → spin-if-overflow, then single or two-fragment `ptr::copy_nonoverlapping` to ring buffer, `head.store(Release)`
3. `Store::get_or_create(user_id) → *mut RingBuffer` — returns existing or newly created [RingBuffer](#ringbuffer); O(1) amortised; executed AFTER AOF write to keep WAL ordering simple (Store mutation after WAL, not before)
4. `RingBuffer::push(record: Record, write_ts: u64) → void` — apply step: write to `data[head]`; `head = (head+1) % capacity`; `count = min(count+1, capacity)`; `last_write_ts = write_ts`; ~0.1 μs total

---

### GetCommand

**Input:** [Connection](#connection)* (rbuf holds 9 B header + 2 B count), [Store\<N\>](#storen)&
**Output:** void (fills `Connection.wbuf` with response: 5 B header + 2 B `actual_count` + `actual_count` × 17 B)
**Invariants:**
- If user not found: status = STATUS_NOT_FOUND (0x02), payload_len = 0
- `actual_count = min(requested_count, RingBuffer.count)` — never exceeds available records
- Records returned newest-first: `out[i] = data[(head-1-i+capacity) % capacity]` for i in [0, actual_count)
- Maximum response size: 5 + 2 + N×17 B = WBUF_SIZE — full ring buffer always fits in one response regardless of N

**Side Effects:** Reads [Store\<N\>](#storen) (no mutation); writes `Connection.wbuf` and `wbuf_len`.

#### Functions
- `Store::get(user_id) → RingBuffer*` — returns pointer or nullptr; O(1)
- `RingBuffer::get_last(uint16 count, Record* out) → uint16` — fills `out[]` newest-first; returns actual_count; ~0.1–0.5 μs

---

### DelCommand

**Input:** [Connection](#connection)* (rbuf holds 9 B header, no payload), [Store\<N\>](#storen)&
**Output:** void (fills `Connection.wbuf` with 5 B STATUS_OK response)
**Invariants:**
- DEL on a non-existent `user_id` returns STATUS_OK — idempotent
- AOF entry written BEFORE Store mutation (write-ahead)
- For N > 200: [MemoryPool](#memorypool) slot freed immediately after `Store::erase`

**Side Effects:** Writes `AOFWriter.buf_`; mutates [Store\<N\>](#storen) (erase); returns [MemoryPool](#memorypool) slot for N > 200; sets `Connection.wbuf`.

#### Functions
- `AOFWriter::append(OPCODE_DEL, user_id, now_ns(), {}) → void` — write_ts captured inline; DEL has no RingBuffer to synchronise with, so timestamp ordering only matters for AOF replay cutoff
- `Store::erase(user_id) → void` — removes key; if N > 200 calls `MemoryPool::free_slot(RingBuffer.data)`

---

### AOFAppend

**Input:** `uint8 opcode`, `uint32 user_id`, `uint64 write_ts`, [Record](#record) payload (empty for DEL)
**Output:** void
**Invariants:**
- Called ONLY from event loop thread
- `write_ts` is always the value captured by the caller (PushCommand/DelCommand) via `now_ns()` — never re-captured inside append; ensures `AOFEntry.write_ts == RingBuffer.last_write_ts` for PUSH
- **`always` mode** — bypasses ring buffer and flush thread entirely: serialises the [AOFEntry](#aofentry) to a stack-local buffer, calls `write(aof_fd_, ...)` then `fdatasync(aof_fd_)` synchronously from the event loop thread; blocks ~50–200 μs per call; no spin, no wrap-around concern, no interaction with `head_`/`tail_`
- **`everysec` / `no` modes** — writes to ring buffer:
  - Spins (busy-wait) if `(head_ - tail_.load(acquire) + required) > AOF_BUF_SIZE`
  - Logs critical if spin exceeds AOF_OVERFLOW_WARN_S (5 s); continues spinning indefinitely — WAL invariant preserved: client receives timeout, not a false OK
  - `head_.store(release)` issued after the **last** memcpy fragment — flush thread sees the complete entry only after both fragments are written
  - **Wrap-around:** an entry may straddle the `buf_` boundary when `(head_ % AOF_BUF_SIZE) + required > AOF_BUF_SIZE`; write uses **two memcpy calls**: first fragment to `buf_[head_ % AOF_BUF_SIZE .. AOF_BUF_SIZE-1]`, second fragment to `buf_[0 .. remaining-1]`; AOFFlush reads with `writev(2)` in two segments — bytes land contiguously on disk

**Side Effects:** `always` mode: writes to `aof_fd_` and calls `fdatasync` from event loop thread. `everysec`/`no` modes: writes to `AOFWriter.buf_`; updates `head_` atomic.

#### Functions
- `AOFWriter::append_push(uid: u32, record: Record, write_ts: u64) → void` — serialises a 32-byte PUSH [AOFEntry](#aofentry) (`1 + 4 + 8 + 2 + 17 = 32` bytes) then calls `write_entry`
- `AOFWriter::append_del(uid: u32, write_ts: u64) → void` — serialises a 15-byte DEL [AOFEntry](#aofentry) (`1 + 4 + 8 + 2 = 15` bytes, no payload) then calls `write_entry`
- `AOFWriter::write_entry(data: &[u8]) → void` — if `fsync_mode == Always`: `write(aof_fd, data)` + `fdatasync(aof_fd)` synchronously from event loop thread; else: calls `ring_write(data)`
- `AOFWriter::ring_write(data: &[u8]) → void` — spin until `head - tail.load(Acquire) + required ≤ AOF_BUF_SIZE`; compute `offset = head % AOF_BUF_SIZE`; if `offset + required > AOF_BUF_SIZE`: two `ptr::copy_nonoverlapping` (end fragment then start fragment); else: single `ptr::copy_nonoverlapping`; `head.store(head + required, Release)`

---

### AOFFlush

**Input:** [AOFWriter](#aofwriter)& (background thread; wakes every AOF_FLUSH_INTERVAL_MS)
**Output:** void
**Invariants:**
- Runs exclusively on flush thread; NEVER accesses [Store\<N\>](#storen)
- Active only in `everysec` and `no` modes; in `always` mode the flush thread exits immediately at startup (AOFAppend handles all I/O directly from the event loop)
- `tail_` updated with `store(release)` AFTER write/fdatasync completes — event loop acquire-loads `tail_` to check free space; release store ensures updated position is visible
- Buffer wrap-around handled with two sequential `write(2)` calls (matching the two-fragment layout written by [AOFAppend](#aofappend))
- `everysec` mode: `write(2)` each iteration; `fdatasync(2)` every AOF_FLUSH_INTERVAL_MS (1000 ms)
- `no` mode: `write(2)` each iteration; no `fdatasync(2)` — OS decides when to flush to disk

**Side Effects:** Reads `AOFWriter.buf_`; writes to `aof_fd_`; optionally calls `fdatasync(2)`; updates `tail_` atomic.

#### Functions
- `AOFWriter::flush_loop() → void` — loops while `running.load(Acquire)`; every AOF_FLUSH_INTERVAL_MS: `pending = head.load(Acquire) - tail`; if pending > 0: if wrap-around: two sequential `write(aof_fd, ...)` calls; else: one `write(aof_fd, ...)`; if `everysec`: `fdatasync(aof_fd)`; `tail.store(tail + pending, Release)`; on loop exit: one final `flush_once()` drain pass before returning

---

### SnapshotDump

**Input:** [Store\<N\>](#storen)& (forked child's CoW address space), [Config](#config)
**Output:** void (writes `snapshot.tmp`, then renames to `snapshot.bin`)
**Invariants:**
- Runs in forked child process ONLY; parent continues serving at full throughput
- Parent pauses 10–50 ms during `fork()` for kernel page table copy (between `epoll_wait` iterations, not during Store mutation)
- Child calls `_exit(0)` — never `exit()` to avoid flushing parent's stdio buffers
- `rename(snapshot.tmp, snapshot.bin)` is atomic per POSIX — readers never see partial file
- If previous child still running on `waitpid(WNOHANG)`: skip this cycle entirely
- Physical pages duplicated lazily by CoW only for pages the parent modifies after fork — plan ~1.5× RAM headroom at high write rates

**Side Effects:** Creates `snapshot.tmp` on disk; renames to `snapshot.bin`; calls `fsync(2)`; terminates child process.

#### Functions
- `SnapshotManager::maybe_fork(store: &Store, interval_ns: u64) → void` — checks elapsed time; `waitpid(child_pid, WNOHANG)` to reap previous child if done; if interval elapsed and no child running: `fork()`; parent records `child_pid`; child calls `dump_to_path` then `_exit(0)`
- `dump_to_path(store: &Store, path: &str, snapshot_ts: u64) → io::Result<()>` (free function) — opens `snapshot.tmp` via `BufWriter`; writes [SnapshotFile](#snapshotfile) header; `Store::for_each`: writes `user_id`, `last_write_ts`, `record_cnt`, then `records[]` in **oldest-first order**; `file.sync_all()`; `rename(tmp, final)`
- `SnapshotManager::dump_sync(store: &Store, path: &str, snapshot_ts: u64) → void` — synchronous wrapper around `dump_to_path` for tests (no fork)

---

### TTLCollect

**Input:** [Store\<N\>](#storen)&
**Output:** void (pushes [Candidate](#candidate)s to `candidates_in_` [SPSCQueue](#spscqueue) queue)
**Invariants:**
- Called from event loop thread every TTL_COLLECT_INTERVAL_S (10 s)
- Iterates the **entire** Store every cycle via `Store::for_each`; pushes `{uid, last_write_ts}` candidates to `candidates_in_`; silently drops if queue is full (TTL worker will re-sample on next cycle)
- Does NOT evict — only samples current `last_write_ts`
- No cursor — full Store is always walked; worst-case TTL scan latency ≤ TTL_COLLECT_INTERVAL_S (10 s) per full pass

**Side Effects:** Reads [Store\<N\>](#storen) (no mutation); writes to `candidates_in_` [SPSCQueue](#spscqueue) queue; updates `EventLoop::ttl_collect_cursor_`.

#### Functions
- `EventLoop::collect_ttl_candidates() → void` — iterate entire Store via `Store::for_each`; push `{uid, last_write_ts}` entries to `candidates_in_`; update `last_ttl_collect`

---

### TTLEvict

**Input:** `candidates_in_` [SPSCQueue](#spscqueue) queue
**Output:** void (pushes expired [Candidate](#candidate)s to `expired_keys_out_` [SPSCQueue](#spscqueue) queue)
**Invariants:**
- Runs on dedicated eviction thread; NEVER accesses [Store\<N\>](#storen) directly
- Condition for eviction: `now_ns() - candidate.last_write_ts > ttl_ns_` — nanoseconds throughout; no unit mismatch
- Only pushes to `expired_keys_out_` when above condition is true
- `expired_keys_out_` capacity = EXPIRED_KEYS_CAP (4096); drops silently if full — next collect cycle will re-sample

**Side Effects:** Reads `candidates_in_`; writes `expired_keys_out_`.

#### Functions
- `TTLEvictionWorker::eviction_loop() → void` — pop all from `candidates_in_`; for each: if TTL expired, push to `expired_keys_out_`

---

### DrainExpiredKeys

**Input:** `expired_keys_out_` [SPSCQueue](#spscqueue) queue, [Store\<N\>](#storen)&
**Output:** void
**Invariants:**
- Called from event loop thread every epoll tick
- False-eviction guard: if `Store::get(uid)->last_write_ts != Candidate.last_write_ts` → skip eviction; zero false positives guaranteed
- Erase is identical to [DelCommand](#delcommand) path ([MemoryPool](#memorypool) slot freed for N > 200)

**Side Effects:** Reads `expired_keys_out_`; conditionally mutates [Store\<N\>](#storen) (erase).

#### Functions
- `EventLoop::drain_expired_keys() → void` — pop all expired [Candidate](#candidate)s; for each: `buf = Store::get(uid)`; if `buf && buf->last_write_ts == candidate.last_write_ts`: `Store::erase(uid)`

---

### RecoveryLoad

**Input:** [Config](#config) (aof_path, snapshot_path)
**Output:** [Store\<N\>](#storen)& (fully populated, ready to serve)
**Invariants:**
- Step 1 (snapshot load) always completes before Step 2 (AOF replay)
- AOF records with `write_ts ≤ snapshot_ts` are skipped; both values are unix nanoseconds — nanosecond precision guarantees no post-fork write shares a timestamp with `snapshot_ts`
- Truncated AOF record at EOF → stop replay — not data loss (client never received ACK for it)
- Subsystems ([AOFWriter](#aofwriter) flush thread, [TTLEvictionWorker](#ttlevictionworker), [EventLoop](#eventloop)) start ONLY after RecoveryLoad completes
- Recovery time: ≤ 3 minutes for 1 GB AOF + snapshot (RTO requirement)

**Side Effects:** Reads `Config.snapshot_path/snapshot.bin`; reads `Config.aof_path`; mutates [Store\<N\>](#storen). Subsystems (AOFWriter flush thread, TTLEvictionWorker) are started by the caller (Startup) after `recover()` returns — not inside `recover()` itself.

#### Functions
- `SnapshotManager::load(path, Store&) → uint64` — check magic + version; for each key: `Store::get_or_create(uid)`; push all `record_cnt` records **in file order** (oldest-first, matching [SnapshotFile](#snapshotfile) layout) via `RingBuffer::push(record, last_write_ts_from_file)` — passing `last_write_ts_from_file` for every push ensures `RingBuffer.last_write_ts` is set correctly after the final push; return `snapshot_ts`
- `AOFWriter::replay(path, snapshot_ts, Store&) → void` — for records with `write_ts > snapshot_ts`: PUSH → `get_or_create + push(record, AOFEntry.write_ts)`; DEL → `Store::erase`; stop on truncated record

---

### Startup

**Input:** [Config](#config) (produced by [ConfigLoad](#configload)); no other external input
**Output:** void (runs until SIGTERM/SIGINT; exits process on any fatal error)
**Invariants:**
- Strict initialization order: ConfigLoad → `validate_config` → `Store<N>` construction → `MemoryPool::init` (N > 200 only) → `bind_listen_socket` → `AOFWriter` (open `aof_fd_`) → `install_signal_handlers` → `RecoveryLoad` → `TTLEvictionWorker::thread_.start` → `AOFWriter::flush_thread_.start` → `EventLoop`; no step may be skipped or reordered
- If any step fails: log fatal + `exit(1)` before proceeding — partial initialisation is never left running
- `config.max_records != COMPILED_N` check happens inside `validate_config`, before any heap allocation; binary/config N mismatch is caught early
- `bind_listen_socket` runs before `RecoveryLoad` so the OS port is reserved while the store is being populated; connections are not accepted (not added to epoll) until `EventLoop` starts
- Signal handlers installed after `bind_listen_socket` and before `RecoveryLoad`; SIGPIPE blocked process-wide so `write(2)` returns `EPIPE` instead of killing the process

**Side Effects:** calls `mmap(MAP_POPULATE)` for [MemoryPool](#memorypool) (N > 200); opens `aof_fd_` (`O_CREAT|O_WRONLY|O_APPEND|O_CLOEXEC`); binds TCP port; starts flush thread; starts TTL eviction thread.

#### Functions
- `bind_listen_socket(uint16 port) → int` — `socket(AF_INET6, SOCK_STREAM|SOCK_NONBLOCK, 0)`; `IPV6_V6ONLY=0` (dual-stack IPv4+IPv6); `SO_REUSEADDR=1`; `bind` to `::` on given port; `listen(LISTEN_BACKLOG)`; return fd; `exit(1)` on any syscall failure — no fallback
- `install_signal_handlers() → void` — register async-signal-safe handler for SIGTERM and SIGINT that writes `1` to a `sig_atomic_t shutdown_flag`; block SIGPIPE via `sigaction(SIGPIPE, SIG_IGN)` — eliminates broken-pipe process termination on closed client connections

---

### ConfigLoad

**Input:** `argc`, `argv` (command-line arguments)
**Output:** [Config](#config) with all fields populated
**Invariants:**
- Config file path = first CLI argument if provided; otherwise `CONFIG_PATH_DEFAULT`
- If config file is absent at `CONFIG_PATH_DEFAULT` and no CLI path given: apply all defaults, log warning — not a fatal error; all required fields must have defaults or the missing-field validation will abort
- Unknown keys in the config file: log warning, ignore — forward-compatible parsing
- All numeric fields validated for range after parsing; out-of-range value → log fatal + `exit(1)`
- `max_records` has no default — its absence in the config file is a fatal error (binary is always compiled for a specific N; guessing is silent corruption)

**Side Effects:** reads file from disk; no other side effects.

#### Functions
- `Config::from_args(args: &[String]) → Result<Config>` — resolve config file path from `args[1]` if present, else `CONFIG_PATH_DEFAULT`; call `from_file(path)`
- `Config::from_file(path: &str) → Result<Config>` — reads file, calls `toml::from_str` into `RawConfig`, then calls `validate`; `Err` on TOML parse error; `RawConfig` fields: `max_records` (u16, required), `ttl_seconds` (u64, required), `aof_path` (String, required), `snapshot_path` (String, required), `snapshot_interval` (u32, default `SNAPSHOT_INTERVAL_S`), `listen_port` (u16, default `PORT_DEFAULT`), `memory_pool_slots` (u32, default `MEMORY_POOL_SLOTS_DEFAULT`), `aof_fsync` (String, default `"everysec"`; must be `"always"` | `"everysec"` | `"no"`)
- `Config::validate(raw: RawConfig) → Result<Config>` — `max_records != COMPILED_N` → `Err` (binary/config N mismatch); `max_records > MAX_RECORDS_PROTOCOL_LIMIT` → `Err`; `ttl_seconds == 0` → `Err`; `aof_path.is_empty()` → `Err`; `snapshot_path.is_empty()` → `Err`; returns `Ok(Config)` on success

---

### TimeOps

**Input:** void
**Output:** uint64 (current time in unix nanoseconds)
**Invariants:**
- Always uses `CLOCK_REALTIME`, not `CLOCK_MONOTONIC` — `write_ts` values survive process restarts: AOF replay compares `write_ts` against `snapshot_ts` across a restart boundary; `CLOCK_MONOTONIC` resets on reboot and would make all AOF entries appear post-snapshot after a system restart
- Result unit is unix nanoseconds: `seconds × 1_000_000_000 + nanoseconds`; uint64 sufficient until year 2554
- `now_ns()` is called exactly once per operation at the moment specified by the caller — never stored and reused across multiple operations within the same call

**Side Effects:** none (single `clock_gettime` syscall via vDSO — no kernel entry on modern Linux).

#### Functions
- `now_ns() → uint64` — `clock_gettime(CLOCK_REALTIME, &ts)`; return `(uint64_t)ts.tv_sec * 1'000'000'000ULL + (uint64_t)ts.tv_nsec`; resolution is kernel-dependent (~1–50 ns on modern Linux with vDSO); called by [PushCommand](#pushcommand), [DelCommand](#delcommand), [EventLoop](#eventloop) (`trigger_snapshot`, `collect_ttl_candidates`), and [TTLEvict](#ttlevict)

---

### RingBufferOps

**Input:** [RingBuffer](#ringbuffer)*, [Record](#record), uint64 `write_ts` (push); uint16 `n`, `Record*` out (get_last)
**Output:** void (push); uint16 actual_count (get_last)
**Invariants:**
- `push` never allocates; never fails; always O(1)
- When `count == capacity` at push time the oldest record is silently overwritten — no error, no notification, no return value indicating overflow
- `get_last` never mutates any field; safe to call without side effects
- `last_write_ts` is updated only inside `push`, always to the server-assigned `write_ts` argument — never derived from `Record.timestamp`; this is the false-eviction guard for [DrainExpiredKeys](#drainexpiredkeys)
- Index arithmetic uses modular reduction; `capacity` is always > 0 after construction (enforced at [StoreOps](#storeops) `get_or_create`)
- Both operations run exclusively on the event loop thread

**Side Effects:** `push` writes `data[head % capacity]`, advances `head`, updates `count`, updates `last_write_ts`. `get_last` is read-only.

#### Functions
- `RingBuffer::push(const Record& r, uint64 write_ts) → void` — write `r` into `data[head]`; `head = (head + 1) % capacity`; if `count < capacity`: `count++`; `last_write_ts = write_ts`; when buffer is already full the slot at the old `head` held the oldest record — it is overwritten silently; `head` after advance points to the next oldest slot on the next push
- `RingBuffer::get_last(uint16 n, Record* out) → uint16` — `actual = min(n, count)`; for i in `[0, actual)`: `out[i] = data[(head + capacity - 1 - i) % capacity]`; return `actual`; out[0] is the most recently pushed record (newest-first); if `count == 0` returns 0 and writes nothing to `out`

---

### StoreOps

**Input:** `uint32 user_id`, [Store\<N\>](#storen)&, [MemoryPool](#memorypool)* (N > 200 only)
**Output:** `RingBuffer*` (get_or_create, get); void (erase, for_each)
**Invariants:**
- ALL operations MUST run on the event loop thread only — no exceptions, no external synchronisation permitted
- `get_or_create` for N > 200 calls `MemoryPool::alloc_slot`; if pool exhausted: log fatal + `exit(1)` — pool exhaustion is a configuration error (`memory_pool_slots` too small), not a recoverable runtime condition
- `erase` for N > 200 calls `MemoryPool::free_slot` **before** `map.erase` — slot freed while pointer is still valid; reversed order would dereference a dangling iterator
- `get` returns `nullptr` if key absent — all callers must null-check before dereferencing
- `for_each` callback MUST NOT call `erase` or `get_or_create` on the same map during iteration — iterator invalidation; used only by [SnapshotDump](#snapshotdump) child (read-only, CoW address space)

**Side Effects:** `get_or_create` may insert into map and call `MemoryPool::alloc_slot` (N > 200). `erase` removes from map and calls `MemoryPool::free_slot` (N > 200). `get` and `for_each` are read-only.

#### Functions
- `Store::get_or_create(uint32 uid) → RingBuffer*` — `auto it = map.find(uid)`; if found: return pointer; else for N ≤ 200: `map.emplace(uid, RingBuffer<N>{data=inline, capacity=N, head=0, count=0, last_write_ts=0})`, return pointer to new value; for N > 200: `slot = pool->alloc_slot()`, construct `RingBuffer` in place (`data=slot`, `capacity=N`, `head=0`, `count=0`, `last_write_ts=0`), `map.emplace(uid, &ring_buf)`, return pointer
- `Store::get(uint32 uid) → RingBuffer*` — `auto it = map.find(uid)`; return `it == map.end() ? nullptr : pointer_to_ringbuf(it)`; never modifies map
- `Store::erase(uint32 uid) → void` — `auto it = map.find(uid)`; if `it == map.end()` return (no-op); for N > 200: `pool->free_slot(it->second->data)`; `map.erase(it)` — erase after free to avoid dangling dereference
- `Store::for_each(callback) → void` — range-for over map; invoke `callback(uid, ring_buf_ref)` for each entry; execution order is unspecified (hash map); used exclusively by [SnapshotDump](#snapshotdump) child — never called in parent while mutations are in flight

---

### MemoryPoolOps

**Input:** [MemoryPool](#memorypool)*; `void*` ptr (free_slot only)
**Output:** `void*` (alloc_slot); void (free_slot, init)
**Invariants:**
- `alloc_slot` and `free_slot` are O(1) — intrusive free list pop/push, no iteration, no locking
- `alloc_slot` MUST NOT be called when `free_list == nullptr`; pool exhaustion triggers fatal exit — not recoverable
- `free_slot` MUST be called with a pointer originally returned by `alloc_slot` for this pool; passing a foreign pointer corrupts the free list with undefined behaviour
- `free_slot` MUST NOT be called twice for the same slot — double-free corrupts the free list
- NOT thread-safe; both operations called exclusively from event loop thread
- Slot contents are NOT zeroed on `free_slot` — `alloc_slot` returns a dirty slot; caller initialises all fields before first use

**Side Effects:** `init` calls `mmap(MAP_POPULATE)` once at startup — all pages pre-faulted, zero page-fault latency on hot path. `alloc_slot` pops free list head. `free_slot` writes `sizeof(void*)` into the returned slot and pushes to free list head.

#### Functions
- `MemoryPool::init(size_t total_slots, size_t slot_size) → void` — `base = mmap(nullptr, total_slots × slot_size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS|MAP_POPULATE, -1, 0)`; assert `base != MAP_FAILED` else fatal; `this->slot_size = slot_size`; `this->total_slots = total_slots`; build intrusive free list: for i in `[0, total_slots-1)`: `*reinterpret_cast<void**>(base + i×slot_size) = base + (i+1)×slot_size`; last slot's first 8 bytes = `nullptr`; `free_list = base`
- `MemoryPool::alloc_slot() → void*` — if `free_list == nullptr`: log fatal + `exit(1)` (increase `memory_pool_slots` in config); `ptr = free_list`; `free_list = *reinterpret_cast<void**>(free_list)`; return `ptr`
- `MemoryPool::free_slot(void* ptr) → void` — `*reinterpret_cast<void**>(ptr) = free_list`; `free_list = ptr`; O(1); slot memory is not zeroed

---

## Public Contract

All operations over TCP. Default port = PORT_DEFAULT (7379). Binary framing. No TLS. No authentication.

### Wire Byte Order

All multi-byte integer fields in both request and response frames use **little-endian** byte order (native byte order on x86-64 Linux). Clients send and receive fields as native integers with no byte-swapping.

| Field | Type | Scope |
|-------|------|-------|
| `magic` | uint16 | all frames |
| `user_id` | uint32 | request header |
| `payload_len` | uint16 | request and response headers |
| `count` | uint16 | GET request payload |
| `actual_count` | uint16 | GET response payload |
| `timestamp` | uint64 | [Record](#record) in PUSH payload / GET response |
| `url_hash` | uint64 | [Record](#record) in PUSH payload / GET response |

`event_type` (uint8) and `status` (uint8) are single-byte fields — byte order does not apply.

**Rationale:** Little-endian eliminates `htonl`/`ntohl` overhead on x86-64 and ARM (Linux ABI is little-endian on both). This is an internal service mesh protocol; no internet-facing interoperability is required.

**Example:** MAGIC_WIRE = 0xAE01 → bytes `[0x01, 0xAE]` at wire offset 0 (little-endian, LSB first).

---

**Protocol-level errors (checked before command dispatch, apply to all operations):**

| Condition | Behaviour |
|-----------|-----------|
| `magic` ≠ MAGIC_WIRE | Connection closed, no response sent |
| `opcode` not in {OPCODE_PUSH, OPCODE_GET, OPCODE_DEL} | Connection closed, no response sent |

**Request frame layout (all commands):**

| Offset | Field | Type | Size |
|--------|-------|------|------|
| 0 | `magic` | uint16 | 2 B |
| 2 | `opcode` | uint8 | 1 B |
| 3 | `user_id` | uint32 | 4 B |
| 7 | `payload_len` | uint16 | 2 B |
| 9 | `payload` | bytes | 0–65535 B |

Request header = 9 B fixed.

**Response frame layout (all commands):**

| Offset | Field | Type | Size |
|--------|-------|------|------|
| 0 | `magic` | uint16 | 2 B |
| 2 | `status` | uint8 | 1 B |
| 3 | `payload_len` | uint16 | 2 B |
| 5 | `payload` | bytes | 0–65535 B |

Response header = 5 B fixed.

---

### PUSH

**Direction:** Client → Server → Client
**Input:**
| Field | Type | Description |
|-------|------|-------------|
| `magic` | uint16 | MAGIC_WIRE (0xAE01) |
| `opcode` | uint8 | OPCODE_PUSH (0x01) |
| `user_id` | uint32 | target user |
| `payload_len` | uint16 | 17 (sizeof [Record](#record)) |
| `event_type` | uint8 | event category [0, 255] |
| `timestamp` | uint64 | unix seconds |
| `url_hash` | uint64 | URL or project identifier hash |

**Output:**
| Field | Type | Description |
|-------|------|-------------|
| `magic` | uint16 | MAGIC_WIRE (0xAE01) |
| `status` | uint8 | STATUS_OK (0x00) |
| `payload_len` | uint16 | 0 |

**Errors:**
| Condition | Behaviour |
|-----------|-----------|
| `magic` ≠ MAGIC_WIRE | Connection closed, no response sent |
| `payload_len` ≠ 17 | Connection closed, no response sent |

**Constants:** MAGIC_WIRE, OPCODE_PUSH, STATUS_OK

**Calls:** [PushCommand](#pushcommand) → `Store::get_or_create` → [AOFAppend](#aofappend) → `RingBuffer::push` — AOF written before in-memory state update (WAL); crash after AOFAppend but before push → record recovered from AOF replay

---

### GET

**Direction:** Client → Server → Client
**Input:**
| Field | Type | Description |
|-------|------|-------------|
| `magic` | uint16 | MAGIC_WIRE (0xAE01) |
| `opcode` | uint8 | OPCODE_GET (0x02) |
| `user_id` | uint32 | target user |
| `payload_len` | uint16 | 2 |
| `count` | uint16 | requested record count [1, N]; single GET returns up to N records — full ring buffer in one request |

**Output (user found):**
| Field | Type | Description |
|-------|------|-------------|
| `magic` | uint16 | MAGIC_WIRE (0xAE01) |
| `status` | uint8 | STATUS_OK (0x00) |
| `payload_len` | uint16 | 2 + actual_count × 17 |
| `actual_count` | uint16 | records returned; = min(count, RingBuffer.count) |
| `records` | [Record](#record)[] | newest-first, `actual_count` entries × 17 B each |

**Output (user not found):**
| Field | Type | Description |
|-------|------|-------------|
| `magic` | uint16 | MAGIC_WIRE (0xAE01) |
| `status` | uint8 | STATUS_NOT_FOUND (0x02) |
| `payload_len` | uint16 | 0 |

**Errors:**
| Condition | Behaviour |
|-----------|-----------|
| `magic` ≠ MAGIC_WIRE | Connection closed, no response sent |
| `payload_len` ≠ 2 | Connection closed, no response sent |
| `count` = 0 or `count` > N | Connection closed, no response sent |
| `user_id` not in [Store\<N\>](#storen) | STATUS_NOT_FOUND, empty payload |

**Constants:** MAGIC_WIRE, OPCODE_GET, STATUS_OK, STATUS_NOT_FOUND

**Calls:** [GetCommand](#getcommand) → `Store::get` → `RingBuffer::get_last`

---

### DEL

**Direction:** Client → Server → Client
**Input:**
| Field | Type | Description |
|-------|------|-------------|
| `magic` | uint16 | MAGIC_WIRE (0xAE01) |
| `opcode` | uint8 | OPCODE_DEL (0x03) |
| `user_id` | uint32 | target user |
| `payload_len` | uint16 | 0 |

**Output:**
| Field | Type | Description |
|-------|------|-------------|
| `magic` | uint16 | MAGIC_WIRE (0xAE01) |
| `status` | uint8 | STATUS_OK (0x00) always (idempotent) |
| `payload_len` | uint16 | 0 |

**Errors:**
| Condition | Behaviour |
|-----------|-----------|
| `magic` ≠ MAGIC_WIRE | Connection closed, no response sent |
| `payload_len` ≠ 0 | Connection closed, no response sent |
| `user_id` not in [Store\<N\>](#storen) | STATUS_OK returned (idempotent) |

**Constants:** MAGIC_WIRE, OPCODE_DEL, STATUS_OK

**Calls:** [DelCommand](#delcommand) → [AOFAppend](#aofappend) → `Store::erase`
