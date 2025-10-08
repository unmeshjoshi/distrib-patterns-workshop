package com.distribpatterns.raft;

import com.tickloom.ProcessId;

/**
 * RequestVote RPC - Raft §5.2, Figure 2
 * 
 * Invoked by candidates to gather votes during election.
 * 
 * KEY RAFT RULE: Includes lastLogIndex and lastLogTerm so voters can
 * implement the "election restriction" - only vote for candidates
 * whose log is at least as up-to-date as their own.
 */
public record RequestVoteRequest(
    int term,              // Candidate's term
    ProcessId candidateId, // Candidate requesting vote
    int lastLogIndex,      // Index of candidate's last log entry
    int lastLogTerm        // Term of candidate's last log entry
) {
}

