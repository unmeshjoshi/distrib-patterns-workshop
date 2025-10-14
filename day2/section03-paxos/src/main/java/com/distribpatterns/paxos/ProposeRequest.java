package com.distribpatterns.paxos;

/**
 * Phase 2a message: Proposer asks acceptors to accept this specific value
 * for the generation.
 */
public record ProposeRequest(long generation, Operation value) {
}

