package com.distribpatterns.paxoslog;

/**
 * Phase 1a: Proposer asks acceptors to promise not to accept
 * any proposal with generation lower than this one FOR THIS LOG INDEX.
 */
public record PrepareRequest(int logIndex, int generation) {
}

