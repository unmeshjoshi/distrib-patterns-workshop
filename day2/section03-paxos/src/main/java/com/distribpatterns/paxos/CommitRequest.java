package com.distribpatterns.paxos;

/**
 * Phase 3 (Learn) message: Coordinator tells acceptors that consensus
 * has been reached and the value should be committed and executed.
 */
public record CommitRequest(int generation, Operation value) {
}

