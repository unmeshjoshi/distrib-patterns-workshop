# Section 07: Raft Consensus Algorithm

## Overview

Raft implementation with the three key safety rules that differentiate it from Multi-Paxos:

1. **Election Restriction** (§5.4.1): Voters only grant votes to candidates with up-to-date logs
2. **Current-Term Commit Rule** (§5.4.2): Only commit entries from current term
3. **Log Consistency Check** (§5.3): AppendEntries includes prevLogIndex/prevLogTerm

## Running Tests

```bash
./gradlew :day2:section07-raft:test
```

## Tests

- **Basic Replication**: Leader replicates to followers



