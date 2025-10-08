# Day 2 - Section 05: Multi-Paxos

Optimized Paxos with stable leader that skips Prepare phase after election, reducing consensus from 3 phases to 2 phases for steady-state operations.

**Key Optimization**: Leader election establishes a global promise for ALL future log indices, eliminating per-operation Prepare phase.

## Running Tests

```bash
# Run all tests
./gradlew :day2:section05-multipaxos:test

# Run specific test
./gradlew :day2:section05-multipaxos:test --tests MultiPaxosTest.testLeaderElection
```

## Tests

### MultiPaxosTest
- **testLeaderElection** - Single node becomes leader via FullLogPrepare
- **testLeaderCanReplicateValues** - Leader replicates operations (Propose + Commit only)
- **testMultipleOperations** - Sequential operations maintain consistency
- **testNonLeaderRejectsRequests** - Followers reject client requests
- **testGetValue** - Read operation using no-op operation

