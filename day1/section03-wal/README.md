# Section 3 - Write-Ahead Log (WAL)

This lab explores the **Write-Ahead Log** pattern - a fundamental building block for durability and crash recovery in distributed systems.

---

## The Core Problem

From Section 2, we learned that **failures are constant at scale**. But how do we ensure that:
- Data is not lost when a server crashes?
- The system can recover to a consistent state after a failure?
- Writes are durable (survive crashes)?

**Solution**: Write-Ahead Log (WAL)

---

## What is a Write-Ahead Log?

A Write-Ahead Log is an **append-only file** where every change is written **before** it's applied to the in-memory state.

### Key Principle:

```
BEFORE modifying state:  Write the change to the log (on disk)
AFTER the log write:     Apply the change to memory
ON CRASH:               Replay the log to reconstruct state
```

###The Write-Ahead Log pattern is fundamental to understanding how distributed systems work, and you'll find that similar concepts keep appearing across different contexts. For instance:

- Event Sourcing in Domain-Driven Design
- Change Data Capture (CDC) from databases
- Transaction log in relational databases
- Commit log in Apache Kafka

### Why This Works:

- **Durability**: Changes written to disk survive crashes
- **Sequential writes**: Fast (disk is optimized for sequential I/O)
- **Recovery**: Replay the log to rebuild state
- **Ordering**: Log preserves the order of operations

---

## How WAL Connects to Previous Sections

| Section 1: Little's Law | Section 2: Failures | Section 3: WAL |
|-------------------------|---------------------|----------------|
| Disk I/O performance (iostat) | Failures happen constantly | Must survive failures |
| Sequential writes are fast | Need durability | WAL uses sequential writes |
| Measured IOPS and latency | Recovery after crash | WAL enables recovery |

**Key Insight**: The disk I/O measurements from Section 1 explain why WAL uses **sequential writes** - they're much faster than random writes!

---

## Project Structure

```
section03-wal/
├── src/
│   ├── main/java/replicate/
│   │   ├── common/
│   │   │   ├── Config.java           # WAL configuration
│   │   │   └── JsonSerDes.java       # JSON serialization
│   │   └── wal/
│   │       ├── WriteAheadLog.java    # Main WAL implementation
│   │       ├── WALSegment.java       # Individual log file
│   │       ├── WALEntry.java         # Single log entry
│   │       ├── Command.java          # Base command class
│   │       ├── SetValueCommand.java  # Key-value set operation
│   │       ├── DurableKVStore.java   # WAL-backed key-value store
│   │       ├── SnapShot.java         # State snapshot
│   │       ├── LogCleaner.java       # Log compaction
│   │       └── ...
│   └── test/java/replicate/
│       ├── common/
│       │   └── DiskWritePerformanceTest.java  # Measure WAL performance
│       └── wal/
│           └── DurableKVStoreTest.java        # Test WAL recovery
```

---

## Build and Run

### Prerequisites

- Java 11 or later
- The project includes Gradle wrapper (no need to install Gradle)

### Build the Project

```bash
cd /opt/workshop/day1/section03-wal
./gradlew build
```

### Run the Tests

```bash
# Run all tests
./gradlew test

# Run a specific test
./gradlew test --tests DurableKVStoreTest

# Run with verbose output
./gradlew test --info
```

---

## Key Components

### 1. WriteAheadLog.java

The core WAL implementation:
- Appends entries sequentially to log files
- Manages log segments (files split at size threshold)
- Provides log replay for recovery

**Key methods**:
```java
WALEntry writeEntry(byte[] data)  // Append to log
void truncate(long logIndex)      // Remove entries after index
List<WALEntry> readAll()          // Read entire log for recovery
```

### 2. DurableKVStore.java

A key-value store backed by WAL:
- Every `put()` operation is logged before being applied
- On startup, replays the log to restore state
- Demonstrates the WAL pattern in action

**Recovery flow**:
```java
1. Open the WAL
2. Read all entries from the log
3. Replay each entry to rebuild in-memory state
4. System is now recovered!
```

### 3. DurableKVStoreTest.java

Demonstrates crash recovery:
1. Write some data
2. Close the store (simulating a crash)
3. Reopen the store
4. Verify all data is recovered

---

## Optimized WAL Implementation

The project includes an **OptimizedWriteAheadLog** that showcases production-grade optimizations from Apache BookKeeper:

### Key Optimizations:

#### 1. Buffered Writes
- **Problem**: Each write() system call has overhead (~1-10μs)
- **Solution**: Buffer writes in memory (64KB buffer)
- **Impact**: 100-1000× reduction in system calls

#### 2. Group Commit
- **Problem**: fsync() is expensive (~1-10ms per call)
- **Solution**: Batch multiple writes, do one fsync for all
- **Impact**: N× reduction in fsync calls (100× in tests!)

#### 3. Pre-allocation
- **Problem**: File growth causes filesystem metadata updates
- **Solution**: Pre-allocate file space in 16MB chunks
- **Impact**: Prevents fragmentation, predictable performance

### Performance Comparison

Run the performance comparison test to see the impact:

```bash
./gradlew test --tests WALPerformanceComparisonTest --info
```

**Results (1000 writes):**
```
Naive WAL:     4191ms  (238 writes/sec)   - 1000 fsyncs
Optimized WAL:   56ms  (17,857 writes/sec) - 10 fsyncs

Speedup: 74.8× faster!
```

**Why this matters:**
- Section 1 showed us disk I/O characteristics (iostat measurements)
- Optimized WAL leverages sequential writes for maximum throughput
- Group commit amortizes expensive fsync cost
- Real-world systems (BookKeeper, Kafka, PostgreSQL) use these patterns

### View the Optimizations

See `OPTIMIZATIONS.md` for detailed analysis of each optimization and how Apache BookKeeper implements them.

---

## Experiments

### Experiment 1: Compare Naive vs Optimized WAL

```bash
./gradlew test --tests WALPerformanceComparisonTest.compareNaiveVsOptimized --info
```

**What it shows**:
- Naive: fsync after every write (slow!)
- Optimized: group commit + buffering (fast!)
- Quantifies the performance difference

### Experiment 2: Test Crash Recovery

```bash
./gradlew test --tests DurableKVStoreTest
```

**What it does**:
- Writes key-value pairs to the store
- Closes the store (simulating crash)
- Reopens and verifies data is recovered

**Key observation**: Every write was logged to disk before being applied, so nothing is lost!

### Experiment 3: Measure WAL Write Performance

```bash
./gradlew test --tests DiskWritePerformanceTest
```

**What it measures**:
- How fast can we write to the WAL?
- Compare with iostat measurements from Section 1
- Observe that sequential writes are very fast

**Expected results**:
- WAL writes should achieve high throughput
- Sequential writes leverage disk optimization
- Much faster than random writes

### Experiment 4: Detailed Optimization Breakdown

```bash
./gradlew test --tests WALPerformanceComparisonTest.showDetailedOptimizations
```

**What it shows**:
- Explains each optimization in detail
- Shows the cost-benefit of each technique
- Connects back to Section 1 (Little's Law and iostat measurements)

---

## Key Concepts

### 1. Sequential vs Random I/O

**Sequential writes** (WAL pattern):
- Append to end of file
- Disk head stays in one area
- **Very fast** (optimized by OS and hardware)

**Random writes**:
- Write to different file locations
- Disk head must seek
- **Much slower** (10-100× slower on HDDs)

### 2. Log Segmentation

The WAL splits into multiple files (segments):
- Each segment has a maximum size (e.g., 100MB)
- Old segments can be cleaned/compacted
- Prevents single file from growing forever

### 3. Log Compaction

Two strategies included:
- **Time-based**: Delete segments older than X hours
- **Index-based**: Delete segments before a snapshot index

**Why needed**: Log grows forever without compaction!

### 4. Snapshots

Periodically save entire state to a snapshot:
- Speeds up recovery (replay from snapshot, not from beginning)
- Allows deletion of old log segments
- Trade-off: Snapshot time vs recovery time

---

## Real-World Usage

### Databases

**PostgreSQL**:
- Uses WAL for crash recovery and replication
- WAL files in `pg_wal/` directory
- Achieves durability with fsync

**MySQL (InnoDB)**:
- Redo log is a WAL
- Provides ACID durability
- Sequential writes to `ib_logfile*`

### Distributed Systems

**Apache Kafka**:
- Entire design based on append-only log
- Topics are partitioned logs
- Achieves high throughput with sequential I/O

**Apache Cassandra**:
- CommitLog is a WAL
- Every write goes to CommitLog first
- Memtables flushed to SSTables periodically

**Etcd, Raft, Paxos**:
- All use WAL for consensus
- Log replication across replicas
- Ensures all nodes agree on order

---

## Connection to Distribution

From the previous sections, we've learned:

1. **Section 1**: Why distribute? **Performance** (avoid high utilization)
2. **Section 2**: Why distribute? **Availability** (failures are constant)
3. **Section 3**: How to distribute? **WAL for durability**

**The Pattern**:
```
Single server with WAL:
  ├── Survives crashes (durability)
  └── But still a single point of failure

Multiple servers with WAL + Replication:
  ├── Each server logs to its own WAL
  ├── Changes replicated to other servers
  ├── Each replica independently recovers from its WAL
  └── System survives both crashes AND server failures!
```

**Next**: Section 4 will show how to replicate the WAL across multiple servers for fault tolerance!

---

## Exercises

### Exercise 1: Understand WAL Recovery

1. Run `DurableKVStoreTest`
2. Add print statements in `DurableKVStore.applyLog()` to see recovery
3. Observe how state is rebuilt from the log

### Exercise 2: Measure Performance

1. Run `DiskWritePerformanceTest`
2. Compare throughput with Section 1 iostat measurements
3. Calculate: How many log entries per second?

### Exercise 3: Experiment with Failures

Modify `DurableKVStoreTest`:
1. Write 1000 entries
2. Close the store (crash simulation)
3. Delete random log segments (simulate disk corruption)
4. Try to recover
5. What happens? How much data is lost?

### Exercise 4: Log Compaction

1. Generate a large log (10,000 entries)
2. Take a snapshot at entry 5,000
3. Use `LogIndexBasedLogCleaner` to compact
4. Verify old segments are removed
5. Verify recovery still works

---

## Key Takeaways

### Why WAL is Fundamental:

1. **Durability**: Writes survive crashes
2. **Performance**: Sequential writes are fast
3. **Recovery**: Replay log to rebuild state
4. **Foundation**: Building block for replication

### The Trade-offs:

| Benefit | Cost |
|---------|------|
| Durability (data survives crashes) | Extra disk I/O (write to log + apply to memory) |
| Fast sequential writes | Log grows forever (needs compaction) |
| Simple recovery (just replay) | Recovery time grows with log size (needs snapshots) |

### Design Decisions:

- **Log segment size**: Larger = fewer files, smaller = easier compaction
- **Sync frequency**: fsync every write = slow but durable, batch = fast but risky
- **Snapshot frequency**: More often = faster recovery, less often = less I/O overhead

---

## Further Reading

**Papers**:
- [The Design and Implementation of a Log-Structured File System (1991)](https://people.eecs.berkeley.edu/~brewer/cs262/LFS.pdf)
- [ARIES: A Transaction Recovery Method (1992)](https://cs.stanford.edu/people/chrismre/cs345/rl/aries.pdf)

**Books**:
- Designing Data-Intensive Applications (Chapter 3: Storage and Retrieval)
- Database Internals (Chapter 6: Write-Ahead Log)

**Code Examples**:
- [PostgreSQL WAL internals](https://www.postgresql.org/docs/current/wal-internals.html)
- [Apache Kafka log implementation](https://github.com/apache/kafka/tree/trunk/core/src/main/scala/kafka/log)
- [LevelDB/RocksDB WAL](https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-%28WAL%29)

---

**Remember**: Almost every distributed system uses some form of Write-Ahead Log. Understanding WAL is key to understanding how systems achieve durability at scale!
