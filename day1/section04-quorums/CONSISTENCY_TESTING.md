# Consistency Testing with History Tracking

This document explains how to use Jepsen-style history tracking and consistency checking in the quorum tests.

## Overview

The `ReadRepairConsistencyTest` demonstrates how to:
1. Track operation history using `History<String>`
2. Export history to EDN (Extensible Data Notation) format
3. Check linearizability and sequential consistency using Jepsen's Knossos checker


###  ReadRepairConsistencyTest (1 test - THIS DOCUMENT)
**This file demonstrates history tracking and consistency checking:**
- `testSyncReadRepairConsistency()` - Shows complete pattern for using History API and ConsistencyChecker

## Using History Tracking

### Complete Pattern Example

```java
// 1. Create history tracker
History<String> history = new History<>();

// 2. Before each operation - record invoke
history.invoke(ALICE, Op.WRITE, "Microservices");

// 3. Perform the operation
var write = client.set(key.getBytes(), "Microservices".getBytes());
assertEventually(cluster, write::isCompleted);

// 4. After success - record ok
history.ok(ALICE, Op.WRITE, "Microservices");

// For reads, record the value that was read
history.invoke(ALICE, Op.READ, null);
var read = client.get(key.getBytes());
assertEventually(cluster, read::isCompleted);
history.ok(ALICE, Op.READ, new String(read.getResult().value()));

// 5. Export to EDN format
String edn = history.toEdn();
System.out.println(edn);

// 6. Check consistency properties
boolean linearizable = ConsistencyChecker.check(
    edn, 
    ConsistencyProperty.LINEARIZABILITY, 
    DataModel.REGISTER
);

boolean sequential = ConsistencyChecker.check(
    edn,
    ConsistencyProperty.SEQUENTIAL_CONSISTENCY,
    DataModel.REGISTER
);

// 7. Assert on results
assertTrue(linearizable, "Should be linearizable");
assertTrue(sequential, "Should be sequentially consistent");
```

### Example History Output (EDN Format)

```clojure
[{:process 0, :process-name "alice", :type :invoke, :f :write, :value "Microservices"}
 {:process 0, :process-name "alice", :type :ok, :f :write, :value "Microservices"}
 {:process 0, :process-name "alice", :type :invoke, :f :write, :value "Distributed Systems"}
 {:process 0, :process-name "alice", :type :ok, :f :write, :value "Distributed Systems"}
 {:process 0, :process-name "alice", :type :invoke, :f :read, :value nil}
 {:process 0, :process-name "alice", :type :ok, :f :read, :value "Distributed Systems"}
 {:process 0, :process-name "alice", :type :invoke, :f :read, :value nil}
 {:process 0, :process-name "alice", :type :ok, :f :read, :value "Distributed Systems"}]
```

### Operation Types in History

- **`:type :invoke`**: Operation started
- **`:type :ok`**: Operation completed successfully
- **`:type :fail`**: Operation failed (e.g., timeout, quorum not reached)
- **`:type :info`**: Operation outcome unknown (client crashed, etc.)

### Operation Functions

- **`:f :write`**: Write/set operation
- **`:f :read`**: Read/get operation

## Consistency Properties

### Linearizability
- **Strongest consistency model**
- Operations must appear to occur atomically at some point between invocation and completion
- Respects real-time ordering
- **In the demo**: Sync read repair with single client achieves linearizability ✅

### Sequential Consistency
- **Weaker than linearizability**
- All operations must appear in some total order
- Does NOT require respecting real-time ordering
- Allows different clients to see different operation orders

## Demo Test: ReadRepairConsistencyTest

The single test in `ReadRepairConsistencyTest` demonstrates the complete workflow:

### Scenario
1. **Write v0** ("Microservices") to all replicas
2. **Partition** CYRENE away from ATHENS and BYZANTIUM
3. **Write v1** ("Distributed Systems") to majority (ATHENS, BYZANTIUM)
4. **Heal** partition
5. **Read** - triggers sync read repair, returns v1
6. **Wait** for repair to complete
7. **Read again** - still returns v1
8. **Export history** to EDN
9. **Check consistency** - both linearizable and sequential ✅

### Expected Results

```
Linearizable: true ✅
Sequential: true ✅
```

**Why it's linearizable:**
- Single client (Alice)
- All operations complete successfully
- Sync read repair ensures all replicas converge
- Sequential ordering is maintained

## Key Insights from Testing

### With Sync Read Repair (Single Client)
```
✅ Linearizable: true
✅ Sequential: true
```
- Synchronous read repair ensures immediate convergence
- Single client simplifies ordering guarantees

### Without Read Repair (Multiple Clients)
```
❌ Linearizable: false (typically)
✅ Sequential: true (may pass)
```
- Stale replicas can serve old data
- Different clients may see different orders
- Violates real-time ordering (non-linearizable)
- But can still be sequentially consistent

### Value of Read Repair
Read repair significantly improves consistency guarantees:
- **Without**: Stale data persists, causing non-linearizable reads
- **With Sync**: Immediate convergence, achieves linearizability
- **With Async**: Eventual consistency, better than nothing

## Actor Names (Matching Scenarios)

All tests use actor names from the workshop scenarios:
- **Replicas**: `athens`, `byzantium`, `cyrene`
- **Clients**: `alice`, `bob`, `nathan`
- **Keys/Values**: `"title"`, `"Microservices"`, `"Distributed Systems"`

## Running the Test

```bash
# Run the consistency test
./gradlew :day1:section04-quorums:test --tests ReadRepairConsistencyTest

# See history output (with --console=plain for verbose output)
./gradlew :day1:section04-quorums:test --tests ReadRepairConsistencyTest --console=plain

# Run all quorum tests
./gradlew :day1:section04-quorums:test
```

## Understanding the Output

When you run the test, you'll see:

```
=== History EDN ===
[{:process 0, :process-name "alice", :type :invoke, :f :write, :value "Microservices"}
 {:process 0, :process-name "alice", :type :ok, :f :write, :value "Microservices"}
 ... more operations ...]
==================
Linearizable: true
Sequential: true
```

This confirms that the history of operations satisfies both consistency properties.

## Extending the Tests

You can create additional tests by:

1. **Adding multiple clients** to test concurrent operations
2. **Adding failures** using `history.fail()` for operations that timeout
3. **Testing different scenarios** (partitions, clock skew, etc.)
4. **Comparing with/without read repair** to see the difference

Example for failed operation:
```java
history.invoke(ALICE, Op.WRITE, "value");
var write = client.set(key.getBytes(), "value".getBytes());
assertEventually(cluster, write::isFailed);
history.fail(ALICE, Op.WRITE, "value");  // Record failure
```

## References

### Books
- **Patterns of Distributed Systems** by Unmesh Joshi (Addison-Wesley, 2023)
  - Quorum pattern
  - Paxos pattern
  - Replicated Log pattern
- **Designing Data-Intensive Applications (DDIA)** by Martin Kleppmann (O'Reilly, 2017)
  - Chapter 9: Consistency and Consensus
  - Chapter 5: Replication

### Tools & Frameworks
- **Tickloom**: https://github.com/unmeshjoshi/tickloom - Framework for building distributed algorithms. It is integrated with Jepsen
- **EDN Format**: https://github.com/edn-format/edn - Extensible Data Notation
- **Jepsen**: https://jepsen.io/ - Distributed systems testing framework
- **Knossos**: https://github.com/jepsen-io/knossos - Linearizability checker

## Further Reading

- Martin Kleppmann's blog on linearizability testing
- Jepsen analysis reports for real distributed systems
- Kyle Kingsbury's talks on distributed systems testing
