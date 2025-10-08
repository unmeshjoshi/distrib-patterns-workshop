# Day 2, Section 01: Single Server & Naive Replication

## Overview

This section demonstrates the progression from single-server to consensus-based replication, showing why naive approaches fail and motivating the need for protocols like Two-Phase and Three-Phase Commit.

## Running the Tests

```bash
# Run all tests
./gradlew :day2:section01-naive-replication:test

# Run specific test classes
./gradlew :day2:section01-naive-replication:test --tests NaiveReplicationServerTest
./gradlew :day2:section01-naive-replication:test --tests TwoPhaseServerTest
./gradlew :day2:section01-naive-replication:test --tests ThreePhaseServerTest
./gradlew :day2:section01-naive-replication:test --tests ThreePhaseServerRecoveryTest
```

## Tests

### NaiveReplicationServerTest
- **testNaiveReplication** - Naive "execute immediately + tell others" approach works in happy path but unsafe

### TwoPhaseServerTest
- **testQuorumWrite** - Two-Phase Commit: ACCEPT (quorum) → COMMIT → execute, all nodes consistent

### ThreePhaseServerTest
- **testThreePhaseHappyPath** - Three-Phase Commit: QUERY → ACCEPT → COMMIT, normal operation
- **testThreePhaseMultipleOperations** - Sequential operations maintain consistency across all nodes
- **testThreePhaseCoordinatorFailureRecovery** - Coordinator crash after ACCEPT, new coordinator recovers (timing-based)

### ThreePhaseServerRecoveryTest
- **testDeterministicCoordinatorFailureRecovery** - Deterministic recovery test using `delayForMessageType()` to drop COMMIT messages, demonstrating non-blocking recovery

## Key Differences

| Approach | Safety | Blocking | Recovery |
|----------|--------|----------|----------|
| Naive Replication | ✗ Unsafe | - | - |
| Two-Phase Commit | ✓ Safe | ✓ Blocks on coordinator failure | ✗ No recovery |
| Three-Phase Commit | ✓ Safe | ✗ Non-blocking | ✓ Query phase enables recovery |

## References

- "Patterns of Distributed Systems" by Unmesh Joshi - Write-Ahead Log, Replicated Log, Two-Phase Commit
- "Designing Data-Intensive Applications" by Martin Kleppmann - Chapter 9: Consistency and Consensus
- Original Raft paper - "In Search of an Understandable Consensus Algorithm"

