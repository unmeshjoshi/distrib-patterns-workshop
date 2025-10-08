# Day 2 - Section 03: Single Value Paxos

Single Value Paxos implementation demonstrating distributed consensus with automatic conflict resolution.

## Running Tests

```bash
# Run all tests
./gradlew :day2:section03-paxos:test

# Run specific test
./gradlew :day2:section03-paxos:test --tests PaxosTest.testBasicConsensus
```

## Tests

### PaxosTest
- **testBasicConsensus** - Single proposer reaches consensus (Prepare → Propose → Commit)
- **testConcurrentProposals** - Multiple proposers with automatic conflict resolution
- **testSetValueOperation** - Key-value store operation through Paxos consensus
