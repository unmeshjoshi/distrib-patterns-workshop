# Section 06: Multi-Paxos with Heartbeats

## Overview
This section extends the Multi-Paxos implementation with heartbeat-based leader liveness checks and step-down behavior.

It covers:
- Heartbeat requests/responses
- Leader step-down on heartbeat rejection
- Random election timeout for preventing split votes
- Recovery and catch-up scenarios exercised by tests

## Key Implementation Files
- `MultiPaxosWithHeartbeatsServer.java` - Enhanced server with heartbeat handlers
- `HeartbeatRequest.java` / `HeartbeatResponse.java` - Heartbeat messages
- Public methods: `sendHeartbeats()`, `checkLeader()`, role transitions

## Running Tests

```bash
./gradlew :day2:section06-multipaxos-heartbeats:test
```

## Tests

- **LeaderElectionTest** - Leader election and timeout behavior
- **LogConsistencyAndSafetyTest** - Log agreement and safety invariants
- **RecoveryAndCatchUpScenariosTest** - Node recovery and follower catch-up
- **MultiPaxosWithHeartbeatsTest** - End-to-end replicated log behavior

## Heartbeat Protocol (Conceptual)
```
Leader → Followers: HeartbeatRequest(generation)
Followers → Leader: HeartbeatResponse(success, currentGeneration)

If follower has higher generation:
  - Follower sends HeartbeatResponse(false, higherGeneration)
  - Leader receives rejection → steps down → becomes follower

If follower timeout expires:
  - Follower triggers election
  - Becomes candidate
```

## Notes

This module demonstrates heartbeat-driven failure detection inside the simplified workshop framework. Raft in the next section builds on the same liveness idea, but adds stricter election and commit rules.

## Comparison: Section 05 vs Section 06
| Feature | Section 05 | Section 06 |
|---------|-----------|------------|
| Heartbeat Messages | ❌ | ✅ (HeartbeatRequest/Response) |
| Leader Step-Down | ❌ | ✅ (On rejection) |
| Random Election Timeout | ❌ | ✅ (Prevents split votes) |
| Timer-Based Triggers | Manual | ⚠️ (Demonstrated, not automated) |
