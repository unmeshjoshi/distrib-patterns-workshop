package com.distribpatterns.generation;

/**
 * Prepare/Promise phase request in Generation Voting.
 * 
 * Coordinator proposes a generation number to all replicas.
 * Each replica votes by comparing with its current generation:
 * - If proposedGeneration > currentGeneration: ACCEPT (promise)
 * - Otherwise: REJECT
 * 
 * Note: Uses strict > (not >=) to ensure uniqueness without server IDs.
 */
public record PrepareRequest(int proposedGeneration) {
}

