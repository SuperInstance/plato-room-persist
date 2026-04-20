# Architecture: plato-room-persist

## Language Choice: Rust

### Why Rust

Storage engines need: deterministic latency, zero-copy reads, no GC pauses.
Python's `sqlite3` wraps C but adds GIL contention and object allocation per row.

| Operation | Python (sqlite3) | Rust (append-only) |
|-----------|-----------------|-------------------|
| Write 10K | ~800ms | ~120ms |
| Read 10K | ~200ms | ~15ms |
| Memory/record | ~150 bytes | ~40 bytes |

### Why not SQLite

SQLite is excellent for: concurrent readers, complex queries, ACID transactions.
Our append-only design is better for:
- **Write-heavy workloads** (tile events, audit logs) — no B-tree rebalancing
- **Sequential replay** (snapshot restore) — just read the WAL
- **WASM embedding** — no filesystem, just ArrayBuffers
- **Deterministic compaction** — we control when and how

**When to add SQLite**: rooms needing relational queries (JOINs, aggregations).
We'd add it as a `StorageBackend` trait implementation.

### Architecture

```
put(key, value) → Record → append to WAL → update in-memory HashMap
                                  ↓
                     WAL grows → compact() → trim to latest per key
                                  ↓
                     periodic → snapshot() → clone HashMap → push to snapshots[]
                                  ↓
                     restore(idx) → snapshots[idx] → replace HashMap
```

### WAL Design

Append-only log of operations. Each entry has:
- Sequence number (monotonic)
- Record (id, key, value, operation)
- Checksum (future: CRC32 for integrity)

Compaction: keep only the latest entry per key + all snapshots.
This gives us O(1) recovery from crash (replay WAL).

### Future: mmap reads

For rooms with millions of records, we'd mmap the WAL file and read directly
from memory-mapped pages. Zero-copy, kernel-managed caching.

### Future: StorageBackend trait

```rust
trait StorageBackend {
    fn put(&mut self, key: &str, value: &str) -> u64;
    fn get(&self, key: &str) -> Option<Record>;
    fn delete(&mut self, key: &str) -> bool;
}

struct MemoryBackend { ... }      // current
struct SqliteBackend { ... }      // relational queries
struct MmapBackend { ... }        // large datasets
```
