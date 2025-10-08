package com.distribpatterns.multipaxos;

/**
 * Response to ProposeRequest.
 * Must include logIndex so coordinator knows which entry this response is for.
 */
public record ProposeResponse(int logIndex, boolean accepted) {
}

