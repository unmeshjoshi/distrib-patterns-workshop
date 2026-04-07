package com.distribpatterns.multipaxos.messages;

/**
 * Response to ProposeRequest.
 * Must include logIndex so coordinator knows which entry this response is for.
 */
public record ProposeResponse(int logIndex, boolean accepted) {
}

