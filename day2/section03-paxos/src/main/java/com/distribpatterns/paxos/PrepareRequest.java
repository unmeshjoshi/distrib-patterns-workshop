package com.distribpatterns.paxos;

/**
 * Phase 1a message: Proposer asks acceptors to promise not to accept
 * any proposal with a generation lower than this one.
 */
public record PrepareRequest(int generation) {
}

