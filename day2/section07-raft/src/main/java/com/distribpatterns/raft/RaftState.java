package com.distribpatterns.raft;

/**
 * Server states in Raft.
 * 
 * State transitions:
 * - FOLLOWER -> CANDIDATE (election timeout)
 * - CANDIDATE -> LEADER (receives majority votes)
 * - CANDIDATE -> FOLLOWER (discovers current leader or new term)
 * - LEADER -> FOLLOWER (discovers server with higher term)
 */
public enum RaftState {
    FOLLOWER,
    CANDIDATE,
    LEADER
}


