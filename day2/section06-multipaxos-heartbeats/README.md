# Section 06: Multi-Paxos with Heartbeats (Demonstration)

## Overview
This section demonstrates the heartbeat protocol concepts for Multi-Paxos.

**Status**: Code-only demonstration (no automated tests). This shows the message flow for:
- Heartbeat requests/responses
- Leader step-down on heartbeat rejection
- Random election timeout for preventing split votes

## Key Implementation Files
- `MultiPaxosWithHeartbeatsServer.java` - Enhanced server with heartbeat handlers
- `HeartbeatRequest.java` / `HeartbeatResponse.java` - Heartbeat messages
- Public methods: `sendHeartbeats()`, `checkLeader()`, role transitions

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

## Production Implementation Note
A full production implementation requires:
1. **Timer Integration**: Scheduled heartbeat sending and timeout checking
2. **Automatic Election Trigger**: Follower auto-starts election on timeout
3. **Heartbeat Scheduler/Checker**: Background tasks for periodic heartbeat operations

These features are present in Raft implementations and the reference workshop code but require
deeper integration with a timer/scheduler system than what's available in the simplified tickloom framework.

## Comparison: Section 05 vs Section 06
| Feature | Section 05 | Section 06 |
|---------|-----------|------------|
| Heartbeat Messages | ❌ | ✅ (HeartbeatRequest/Response) |
| Leader Step-Down | ❌ | ✅ (On rejection) |
| Random Election Timeout | ❌ | ✅ (Prevents split votes) |
| Timer-Based Triggers | Manual | ⚠️ (Demonstrated, not automated) |

