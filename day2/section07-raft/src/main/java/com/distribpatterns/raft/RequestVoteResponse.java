package com.distribpatterns.raft;

/**
 * RequestVote RPC response - Raft §5.2, Figure 2
 */
public record RequestVoteResponse(
    int term,         // Current term, for candidate to update itself
    boolean voteGranted  // true means candidate received vote
) {
}


