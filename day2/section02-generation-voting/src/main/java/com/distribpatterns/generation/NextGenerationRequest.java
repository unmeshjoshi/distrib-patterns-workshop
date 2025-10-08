package com.distribpatterns.generation;

/**
 * Client request that triggers leader election at the server.
 * 
 * When a server receives this request, it will:
 * 1. Propose generation + 1 to all replicas
 * 2. Run quorum voting (prepare/promise phase)
 * 3. Return the elected generation number if quorum is reached
 * 4. Retry with higher generation if election fails
 */
public record NextGenerationRequest() {
}

