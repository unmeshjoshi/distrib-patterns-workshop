package com.distribpatterns.paxos;

/**
 * Phase 1b message: Acceptor's response to PrepareRequest.
 * 
 * If promised is true, the acceptor promises not to accept proposals with lower generations.
 * If the acceptor has previously accepted a value, it includes that value and generation.
 * This allows the proposer to discover any previously accepted values.
 * 
 * @param promised Whether the acceptor promises this generation
 * @param acceptedGeneration The generation of any previously accepted value (null if none)
 * @param acceptedValue The previously accepted value (null if none)
 */
public record PrepareResponse(
    boolean promised,
    Integer acceptedGeneration,
    Operation acceptedValue
) {
}

