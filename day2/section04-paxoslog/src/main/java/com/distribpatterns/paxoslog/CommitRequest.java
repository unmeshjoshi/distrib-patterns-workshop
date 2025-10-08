package com.distribpatterns.paxoslog;

/**
 * Phase 3: Coordinator tells acceptors that consensus has been reached
 * and the value should be committed at this log index.
 */
public record CommitRequest(int logIndex, int generation, Operation value) {
}

