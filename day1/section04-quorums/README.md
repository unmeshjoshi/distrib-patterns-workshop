# Section 04: Quorum-based Key-Value Store

This section implements a distributed key-value store using quorum consensus for read and write operations.

## Overview

The implementation demonstrates:
- **Majority quorum consensus** for both reads and writes
- **Versioned values** with timestamps for conflict resolution
- **Network partition handling** and split-brain prevention
- **Clock skew scenarios** showing last-write-wins conflicts
- **Consistency property testing** (linearizability and sequential consistency)

## Implementation

### Core Classes

#### Main Implementation
- **`QuorumKVReplica`** - Main replica server that handles client requests
  - Receives GET/SET requests from clients
  - Broadcasts internal requests to all replicas
  - Uses `AsyncQuorumCallback` to wait for majority responses
  - Returns latest value based on timestamp for GET operations

- **`QuorumKVClient`** - Client for sending requests to the cluster
  - Sends requests to the primary replica
  - Receives responses asynchronously via `ListenableFuture`

- **`QuorumKVReplicaProcessFactory`** - Factory for creating replica instances

#### Message Types
- **Client Messages**: `GetRequest`, `GetResponse`, `SetRequest`, `SetResponse`
- **Internal Messages**: `InternalGetRequest`, `InternalGetResponse`, `InternalSetRequest`, `InternalSetResponse`
- **`QuorumKVMessageTypes`** - Message type constants for routing

## Tests

### NetworkPartitionTest

Comprehensive test suite demonstrating various distributed systems scenarios:

#### 1. Split-brain Prevention
Tests that during a network partition (2 vs 3 nodes), only the majority partition can successfully complete writes. After healing, the majority value persists.

**Key Concepts:**
- Minority writes fail (no quorum)
- Majority writes succeed
- System prevents split-brain condition

#### 2. Local Stale Read After Quorum Write
Demonstrates that reading from a node that wasn't part of the write quorum can return stale data.

**Consistency Results:**
- ❌ Not linearizable (stale read observed)
- ✅ Sequentially consistent (different clients can see different orders)

#### 3. Clock Skew Scenario
Shows how clock skew can cause a failed minority write (with higher timestamp) to "win" over a successful majority write (with lower timestamp) after partition healing.

**Key Concepts:**
- Last-write-wins based on timestamp
- Clock skew can violate real-time ordering
- ❌ Not linearizable
- ❌ Not sequentially consistent

#### 4. Network Delays
Verifies that the system handles variable network latencies correctly and operations complete successfully despite delays.

### DDIA_Linearizability_And_Quorum_ScenarioTest

Implements the famous scenario from "Designing Data-Intensive Applications" (Chapter 10.6) by Martin Kleppmann:

**Scenario:**
1. Writer updates a value via one node
2. Message propagation is delayed to some nodes
3. Alice reads via a node with fresh data → sees new value
4. Bob reads via a node with stale data → sees old value (even though Alice already saw new)

**Results:**
- ❌ Not linearizable (Bob sees old after Alice saw new)
- ✅ Sequentially consistent (can reorder to satisfy different clients)

## Running Tests

```bash
# From workspace root
./gradlew :day1:section04-quorums-java:test

# Run specific test
./gradlew :day1:section04-quorums-java:test --tests NetworkPartitionTest

# Run with verbose output
./gradlew :day1:section04-quorums-java:test --console=plain
```

## Key Learnings

1. **Quorums Don't Guarantee Linearizability**
   - R + W > N provides consistency across reads
   - But doesn't prevent stale reads or ensure real-time ordering

2. **Clock Skew is Dangerous**
   - Last-write-wins with wall-clock timestamps is problematic
   - Higher timestamp doesn't mean "more recent" in real-time
   - Can lead to lost writes and violated causality

3. **Partition Handling**
   - Majority quorum prevents split-brain
   - Minority partition becomes unavailable (by design)
   - Ensures at most one partition can make progress

4. **Consistency Models**
   - **Linearizability**: Strict real-time ordering (hard to achieve)
   - **Sequential Consistency**: Total order, ignoring real-time (easier)
   - Quorum systems typically provide neither by default

## Architecture

```
Client
  ↓ (CLIENT_GET_REQUEST)
QuorumKVReplica (Primary)
  ↓ (INTERNAL_GET_REQUEST) → All Replicas
  ← (INTERNAL_GET_RESPONSE) ← Majority Replicas
  ↓ (CLIENT_GET_RESPONSE)
Client
```

## Dependencies

- **Tickloom** (0.1.0-alpha.11): Distributed systems testing framework
- **Tickloom-Testkit** (0.1.0-alpha.11): Testing utilities including:
  - Simulated network with partition support
  - Cluster management
  - Consistency checking (via Jepsen/Knossos)
  - History recording and EDN export

## References

- Martin Kleppmann, "Designing Data-Intensive Applications", Chapter 9 (Consistency and Consensus)
- [Jepsen](https://jepsen.io/) - Distributed systems testing
- [Knossos](https://github.com/jepsen-io/knossos) - Linearizability checker

