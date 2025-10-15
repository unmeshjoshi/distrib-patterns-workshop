
In production systems, there are two variants used for 
persissting log entries.

* **Option 1 — WAL + in‑memory map (etcd‑style):** Keep a compact, append‑only **Write‑Ahead Log** on disk and rebuild an in‑memory `TreeMap<Integer, PaxosState>` on startup. Persist state transitions (PROMISE/ACCEPT/COMMIT) and an applied high‑water‑mark (HWM). Simple, robust, great for bring‑up.
* **Option 2 — KV‑backed Log (Dragonboat / CockroachDB / TiKV‑style):** Store log entries as **key‑value pairs** in a RocksDB/Pebble‑like engine using big‑endian, index‑ordered keys (optionally `[groupId]|index`). Engine WAL + memtable + SSTs give fast random access, range scans, and easy truncation.


## Option 1 — WriteAheadLog + in‑memory `TreeMap` (etcd‑style)

**What:**
* We use a WAL similar to [day1/section03-wal](../day1/section03-wal/)
* Append on disk: `ENTRY(index, term, value)`, `STATE(term,vote,commit)`, `SNAPSHOT(meta)`, `CRC`, `METADATA`.
* In memory: `TreeMap<Integer, PaxosState>` as the serving index; replay WAL at startup.

**Trade‑offs:**

* Replay time grows with history (mitigate via periodic snapshots/segment pruning).
* 
**Good references:**

* etcd WAL package (segmented WAL, CRC chaining, torn‑tail repair):
    * [https://pkg.go.dev/go.etcd.io/etcd/server/v3/wal](https://pkg.go.dev/go.etcd.io/etcd/server/v3/wal)
* etcd Raft library (storage contract: FirstIndex/LastIndex/Entries/Term/Snapshot):
    * [https://pkg.go.dev/go.etcd.io/etcd/raft/v3](https://pkg.go.dev/go.etcd.io/etcd/raft/v3)

---

## Option 2 — Log abstraction over a KV store (Dragonboat / CockroachDB / TiKV‑style)

**What:**

* Keys are **lexicographically sortable** so byte order == numeric order. Examples:
    * Single group: `log:<be64(index)> → entryBytes`
    * Multi‑group: `[kind=entry][be64(groupId)][be64(nodeId)][be64(index)] → entryBytes`
* Separate meta keys: `hardstate`, `firstIndex` (post‑snapshot), `tail` (next index).

**Why:**

* **Random access & range scans** (iterate `lo..hi` efficiently).
* **Fast cold‑start** with large logs (little/no replay needed).
* **Multi‑tenant ready** (many raft groups in one DB via composite keys).

**Trade‑offs:**

* More pieces to tune (flush/compaction, CFs, prefix extractors).
* Potentially higher write amplification (WAL + compaction), especially with frequent truncation unless you use coarse range deletes/segmentation.

**Good references:**

* Dragonboat LogDB (KV backends: Pebble/RocksDB): [https://pkg.go.dev/github.com/lni/dragonboat/v3](https://pkg.go.dev/github.com/lni/dragonboat/v3) (see `logdb/kv/...`).
* CockroachDB on Pebble (RocksDB‑compatible engine): [https://www.cockroachlabs.com/blog/pebble-rocksdb-kv-store/](https://www.cockroachlabs.com/blog/pebble-rocksdb-kv-store/)
* TiKV storage architecture and Raft logs:
    * Overview: [https://tikv.org/docs/dev/reference/architecture/storage/](https://tikv.org/docs/dev/reference/architecture/storage/)
    * Historical RocksDB `raftdb` usage: [https://tikv.org/docs/6.1/deploy/configure/rocksdb/](https://tikv.org/docs/6.1/deploy/configure/rocksdb/)
    * Raft Engine (purpose‑built log store for multi‑Raft): [https://github.com/tikv/raft-engine](https://github.com/tikv/raft-engine)