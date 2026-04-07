package com.distribpatterns.raft.messages;

/**
 * AppendEntries RPC response - Raft §5.3, Figure 2
 */
public record AppendEntriesResponse(
    int term,          // Current term, for leader to update itself
    boolean success,   // true if follower contained entry matching prevLogIndex and prevLogTerm
    int matchIndex     // Highest log index replicated (for updating leader's matchIndex)
) {
}

