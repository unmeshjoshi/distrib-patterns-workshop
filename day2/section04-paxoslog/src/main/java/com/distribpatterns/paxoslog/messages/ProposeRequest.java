package com.distribpatterns.paxoslog.messages;

import com.distribpatterns.paxoslog.Operation;

/**
 * Phase 2a: Proposer asks acceptors to accept this value
 * at this log index with this generation.
 */
public record ProposeRequest(int logIndex, int generation, Operation value) {
}

