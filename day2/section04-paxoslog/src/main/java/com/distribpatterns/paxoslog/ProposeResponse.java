package com.distribpatterns.paxoslog;

/**
 * Phase 2b: Acceptor's response to ProposeRequest.
 * Must include logIndex so coordinator knows which entry this response is for.
 */
public record ProposeResponse(int logIndex, boolean accepted) {
}

