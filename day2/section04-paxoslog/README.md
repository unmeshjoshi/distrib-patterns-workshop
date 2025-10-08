# Day 2 - Section 04: PaxosLog

Replicated log where each entry is decided via Paxos consensus, enabling distributed state machine replication.

**Key Pattern**: High-Water Mark - entries must be committed in sequential order (no gaps). The high-water mark tracks the highest consecutive committed index, ensuring safe execution of operations.

## Running Tests

```bash
# Run all tests
./gradlew :day2:section04-paxoslog:test

# Run specific test
./gradlew :day2:section04-paxoslog:test --tests PaxosLogTest.testBasicSetValue
```

## Tests

### PaxosLogTest
- **testBasicSetValue** - Single SetValue operation execution and verification
- **testMultipleCommands** - Sequential log building with multiple operations
- **testCompareAndSwap** - Atomic conditional update (CAS) operations
- **testGetValue** - Read operation using no-op operation

