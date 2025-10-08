# Day 2, Section 02: Generation Voting

## Overview

Generation Voting is a distributed algorithm for generating monotonically increasing numbers using quorum-based voting. It's essentially a simplified Paxos Phase 1 (prepare/promise) used for leader election and generation numbering.

## Algorithm

```
Client → Server: NextGenerationRequest (triggers leader election)

Server (Coordinator):
  1. Propose: generation + 1
  2. Broadcast: PrepareRequest(proposedGeneration) to all replicas
  3. Collect votes

Replicas:
  - If proposedGeneration > currentGeneration: ACCEPT (update + promise)
  - Otherwise: REJECT

Coordinator:
  - If QUORUM accepts: Election WON, return generation
  - Otherwise: Election LOST, retry with generation + 2
```

## Key Properties

- **Monotonic**: Numbers always increase
- **Unique**: Quorum ensures no duplicates
- **Fault-tolerant**: Works with minority node failures
- **Retry mechanism**: Automatic conflict resolution

## Running the Tests

```bash
# Run all tests
./gradlew :day2:section02-generation-voting:test

# Run specific tests
./gradlew :day2:section02-generation-voting:test --tests GenerationVotingTest.testGenerateMonotonicNumbers
./gradlew :day2:section02-generation-voting:test --tests GenerationVotingTest.testMultipleCoordinators
./gradlew :day2:section02-generation-voting:test --tests GenerationVotingTest.testConcurrentRequests
```

## Tests

### GenerationVotingTest
- **testGenerateMonotonicNumbers** - Sequential requests from single coordinator generate 1, 2, 3
- **testMultipleCoordinators** - Different nodes can coordinate elections, maintaining monotonicity
- **testConcurrentRequests** - Multiple simultaneous requests produce unique, monotonic numbers

## Implementation Details

### Strict Greater-Than (>)

Uses `>` instead of `>=` to ensure uniqueness without requiring server IDs in the ballot number:

```java
if (proposedGeneration > generation) {  // Strict >
    generation = proposedGeneration;
    promised = true;
}
```

### Retry Logic

If election fails (no quorum), automatically retries with higher generation:

```java
quorumCallback.onFailure(error -> {
    // Retry with generation + 1
    runElection(proposedGeneration + 1, attempt + 1, requestId);
});
```

### Quorum Voting

Uses `AsyncQuorumCallback` to collect votes and determine election outcome:

```java
var quorumCallback = new AsyncQuorumCallback<PrepareResponse>(
    getAllNodes().size(),
    response -> response != null && response.promised()
);
```

## Use Cases

- **Leader Election**: Elect a leader with unique epoch/term number
- **Distributed Locking**: Generate unique lock tokens
- **Versioning**: Create monotonic version numbers
- **Paxos/Raft**: Foundation for consensus protocols

## References

- "Paxos Made Simple" by Leslie Lamport - Prepare/Promise phase
- "Patterns of Distributed Systems" by Unmesh Joshi - Generation Clock, Leader Election
- "Designing Data-Intensive Applications" by Martin Kleppmann - Chapter 9: Consistency and Consensus

