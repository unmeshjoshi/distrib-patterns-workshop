package com.distribpatterns.paxoslog;

/**
 * Phase 1b: Acceptor's response to PrepareRequest.
 * Includes any previously accepted value for this log index.
 */
public record PrepareResponse(
    boolean promised,
    Integer acceptedGeneration,  // null if none
    Operation acceptedValue      // null if none
) {
}

