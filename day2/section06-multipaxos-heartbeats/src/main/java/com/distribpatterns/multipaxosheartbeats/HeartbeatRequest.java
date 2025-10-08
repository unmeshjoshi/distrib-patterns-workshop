package com.distribpatterns.multipaxosheartbeats;

/**
 * Heartbeat message sent from leader to followers.
 * 
 * Contains the leader's current generation so followers can:
 * - Verify the leader is legitimate
 * - Step down if they see a higher generation
 * - Reject if they have a higher generation
 */
public record HeartbeatRequest(
    int generation
) {
}

