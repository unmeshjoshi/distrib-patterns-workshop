package com.distribpatterns.multipaxos;

/**
 * Phase 2: Leader tells followers to commit value at log index.
 */
public record CommitRequest(int logIndex, int generation, Operation value) {
}

