package com.distribpatterns.multipaxosheartbeats.messages;

/**
 * Response to heartbeat from follower to leader.
 * 
 * If success=false, the follower has a higher generation and the leader should step down.
 */
public record HeartbeatResponse(
    boolean success,
    int currentGeneration
) {
}

