# Section 07: Raft Consensus Algorithm

## Overview

Raft implementation of the replicated-log consensus pattern, positioned in the workshop as the practical counterpart to Multi-Paxos.

This section includes the three key safety rules that differentiate Raft from Multi-Paxos:

1. **Election Restriction** (§5.4.1): Voters only grant votes to candidates with up-to-date logs
2. **Current-Term Commit Rule** (§5.4.2): Only commit entries from current term
3. **Log Consistency Check** (§5.3): AppendEntries includes prevLogIndex/prevLogTerm

It also includes:

- Leader election driven by election timeouts
- Direct log replication through `AppendEntries`
- State-machine application for replicated key/value operations
- Client command execution through the elected leader

## Running Tests

```bash
./gradlew :day2:section07-raft:test
```

## Tests

- **Election Safety** - At most one leader per term
- **Basic Replication** - Leader replicates committed entries to followers

## Key Files

- `RaftServer.java` - Election, replication, commit, and state-machine logic
- `RequestVoteRequest.java` / `RequestVoteResponse.java` - Leader election RPCs
- `AppendEntriesRequest.java` / `AppendEntriesResponse.java` - Log replication and heartbeat RPCs
- `RaftTest.java` - Safety and replication tests

