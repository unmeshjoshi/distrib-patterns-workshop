# Scenario: A Stuck Leader

A leader loses one round of PROPOSE replies. It stays leader and keeps sending
heartbeats, but its log now has a hole that nobody fills. Execution stops at the
hole, so every later client request hangs. No committed entry is lost or changed:
this is a **liveness** problem, not a safety one.

The randomised safety test (`MultiPaxosWithHeartbeatsSafetyTest`) first found it
at seed 20, where the leader was cut off for 10 ticks while its PROPOSE replies
for index 158 were in flight.

## Timeline

Three nodes. Athens is the leader at generation 1.

| Step | What happens | Athens's log |
|------|--------------|--------------|
| 1 | A client asks athens to set `title`. Athens appends it at index 0 and sends PROPOSE to all three nodes. | 0: proposed |
| 2 | All three accept index 0 and write the accept to their WAL. | 0: accepted |
| 3 | Athens is cut off from the network for 10 ticks, so all three PROPOSE replies, including the one it sends itself, are dropped. | 0: accepted |
| 4 | Athens reconnects. 10 ticks is well under the election timeout (100+ ticks), so no follower has started an election. Athens is still leader. | 0: accepted |
| 5 | A second client request sets `author`. Athens appends it at index 1; PROPOSE and COMMIT both reach a quorum. | 0: accepted, 1: committed |
| 6 | Athens tries to execute index 1, but entries execute strictly in order and index 0 is not committed. `highWaterMark` stays at -1. Neither client gets a reply. | 0: accepted, 1: committed |
| 7 | Athens keeps sending heartbeats on every tick, so followers never time out and no election happens. The cluster stays stuck. | unchanged |

## Why the code gets stuck

- `startProposePhase` sends one quorum request. When it fails (the request times
  out), `failClientCommand` tells the client, but **nothing proposes index 0 again**.
- `tryExecuteLogEntries` stops at the first index that is not durably committed,
  so index 0 blocks index 1 and everything after it.
- `onTick` sends heartbeats whenever the role is `Leader`. A leader that cannot
  make progress still looks healthy to its followers.

## Why safety still holds

Index 0 was accepted by a quorum, so it may already be chosen. Any future leader
collects the logs of a quorum in FULL_LOG_PREPARE, finds the accepted value and
completes it in `completeLogAfterElection` with the *same* value. Nothing that
might have been chosen is ever replaced.

## What repairs it

Only a new election does. `completeLogAfterElection` proposes every uncommitted
entry above the high-water mark again, and fills gaps with no-ops, so the hole at
index 0 gets committed and execution resumes. Because heartbeats keep the stuck
leader in place, that election has to be triggered from outside, for example by
calling `startElection()` or by crashing the leader. The randomised safety test
does exactly that before its final write.

## Reproducing it

```java
cluster.tickUntil(cluster::areAllNodesInitialized);
cluster.tickUntil(athens::isLeader);
var client = cluster.newClientConnectedTo(ALICE, ATHENS, MultiPaxosWithHeartbeatsClient::new);

var first = client.execute(ATHENS, new SetValueOperation("title", "Microservices"));
// Cut athens off once the followers have accepted, before their replies arrive.
cluster.tickUntil(() -> byzantium.getPaxosLog().containsKey(0) && cyrene.getPaxosLog().containsKey(0));
cluster.isolateProcess(ATHENS);
for (int i = 0; i < 10; i++) cluster.tick();
cluster.reconnectProcess(ATHENS);

var second = client.execute(ATHENS, new SetValueOperation("author", "Martin"));
for (int i = 0; i < 1000; i++) cluster.tick();

// athens.isLeader() is true, index 0 is accepted but not committed, index 1 is committed,
// athens.getHighWaterMark() is -1, and neither `first` nor `second` has completed.
```

## Discussion questions

1. The heartbeat says "I am alive". What should it say instead, so that followers
   notice a leader that is alive but not making progress?
2. Should the leader retry the PROPOSE for index 0 until it succeeds, or step
   down and let a new election fill the hole? What does each choice cost?
3. A follower that misses a COMMIT is stuck in the same way, at its own gap. How
   is that case different from this one, and who can repair it?

## How other protocols handle it

- **Raft**: the leader keeps retrying `AppendEntries` for every follower until
  the follower's log matches its own. A lost reply is just retried on the next
  heartbeat, because in Raft the heartbeat *is* an `AppendEntries`.
- **Multi-Paxos in practice** (for example, Paxos Made Live): the leader keeps
  re-sending proposals for unfinished instances, and lagging replicas catch up
  through a separate catch-up mechanism.
