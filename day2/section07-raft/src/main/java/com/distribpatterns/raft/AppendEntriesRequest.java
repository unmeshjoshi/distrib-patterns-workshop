package com.distribpatterns.raft;

import com.tickloom.ProcessId;

import java.util.List;

/**
 * AppendEntries RPC - Raft §5.3, Figure 2
 * 
 * Invoked by leader to:
 * 1. Replicate log entries (normal operation)
 * 2. Send heartbeats (empty entries list)
 * 
 * KEY RAFT RULE: Includes prevLogIndex and prevLogTerm for log consistency check.
 * Follower rejects if it doesn't have an entry at prevLogIndex with matching term.
 */
public record AppendEntriesRequest(
    int term,              // Leader's term
    ProcessId leaderId,    // So follower can redirect clients
    int prevLogIndex,      // Index of log entry immediately preceding new ones
    int prevLogTerm,       // Term of prevLogIndex entry
    List<LogEntry> entries,  // Log entries to store (empty for heartbeat)
    int leaderCommit       // Leader's commitIndex
) {
}

