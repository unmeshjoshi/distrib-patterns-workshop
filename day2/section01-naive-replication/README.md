# Day 2, Section 01: Single Server & Naive Replication

## Overview

This section demonstrates why naive approaches to replication fail and motivates the need for consensus protocols.

## Scenarios

### 1A. Single Server Lacks Fault Tolerance

**Actors:** Client, SingleServer

**Sequence:**
1. Client → SingleServer: `INCREMENT counter +5`
2. SingleServer executes locally
3. SingleServer → Client: `SUCCESS, newValue=5`

**Issues:**
- ✗ No availability under crash
- ✗ No durability guarantees  
- ✗ Single point of failure
- ✗ No way to recover state after crash

**Key Insight:** A single server cannot provide fault tolerance. We need replication.

---

### 1B. "Immediate Execution + Tell Others" (Naive Replication)

**Actors:** Client, ServerA (leader), ServerB, ServerC (followers)

**Sequence (naive approach):**
1. Client → ServerA: `INCREMENT counter +5`
2. ServerA executes **immediately** (local state update)
3. ServerA → (ServerB, ServerC): `REPLICATE operation` (best effort, no wait for ACK)
4. ServerA → Client: `SUCCESS, newValue=5`

**Failure Modes:**

#### Message Loss/Reorder → Divergent States
- Messages can be dropped by network
- No acknowledgment mechanism
- No way to detect inconsistency
- **Result:** Different servers have different values

#### Crash After Execute, Before Replication
- Leader executes operation locally
- Leader responds to client with SUCCESS
- Leader crashes before sending replication messages
- **Result:** Client thinks operation succeeded, but data is lost on followers

#### Stale Reads from Followers  
- Leader executes operation
- Follower is slow to receive replication message
- Client reads from follower
- **Result:** Client sees old data (read-your-writes violation)

#### No Safety Under Partitions
- Leader gets partitioned from followers
- Leader continues to accept writes
- Followers don't receive updates
- **Result:** Split-brain scenario, divergent states

**Key Insight:** "Execute immediately + tell others" provides:
- ✓ Low latency (responds before replication)
- ✗ No durability guarantees
- ✗ No consistency guarantees
- ✗ No safety under failures

## Running the Tests

```bash
# Run all tests
./gradlew :day2:section01-naive-replication:test

# Run single server tests
./gradlew :day2:section01-naive-replication:test --tests SingleServerTest

# Run naive replication tests  
./gradlew :day2:section01-naive-replication:test --tests NaiveReplicationTest
```

## Test Scenarios

### SingleServerTest

1. **testSingleServerBasicOperation** - Shows basic operations work when server is up
2. **testSingleServerCrashLosesAvailability** - Demonstrates no availability under crash
3. **testSingleServerMultipleOperations** - Shows multiple operations work correctly

### NaiveReplicationTest

1. **testNaiveReplicationHappyPath** - Shows naive replication works in ideal conditions
2. **testMessageLossCausesDivergence** - Demonstrates message loss causes divergent states
3. **testLeaderCrashAfterExecuteBeforeReplication** - Shows data loss when leader crashes
4. **testStaleReadsFromFollowers** - Demonstrates stale reads from followers

## What's Next?

These scenarios motivate the need for:
- **Consensus protocols** (Raft, Paxos) for safe replication
- **Write-Ahead Log** for durability
- **Quorum-based replication** for availability
- **Leader election** for fault tolerance
- **State machine replication** for consistency

See Day 2, Section 02 for introduction to Two-Phase Commit and consensus fundamentals.

## Key Takeaways

1. **Single server = single point of failure** - No fault tolerance at all
2. **Naive replication is unsafe** - "Execute + tell others" breaks under common failures
3. **Low latency ≠ correctness** - Fast responses don't guarantee durability
4. **Network is unreliable** - Must handle message loss, delays, reordering
5. **Need consensus** - Safe replication requires coordination protocols

## References

- "Patterns of Distributed Systems" by Unmesh Joshi - Write-Ahead Log, Replicated Log
- "Designing Data-Intensive Applications" Chapter 9 - Consistency and Consensus
- Original Raft paper - "In Search of an Understandable Consensus Algorithm"

