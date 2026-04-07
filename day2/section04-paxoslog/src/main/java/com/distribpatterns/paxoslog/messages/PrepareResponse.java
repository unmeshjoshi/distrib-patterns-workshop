package com.distribpatterns.paxoslog.messages;

import com.distribpatterns.paxoslog.Operation;

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

