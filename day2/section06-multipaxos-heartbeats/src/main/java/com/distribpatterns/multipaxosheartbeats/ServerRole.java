package com.distribpatterns.multipaxosheartbeats;

/**
 * Server role in Multi-Paxos cluster.
 * 
 * - Leader: Can propose new log entries (skip Prepare phase)
 * - Follower: Accepts proposals from current leader
 * - LookingForLeader: Transitioning state during election
 */
public enum ServerRole {
    Leader,
    Follower,
    LookingForLeader
}

