package com.distribpatterns.generation;

/**
 * Response containing the elected generation number.
 * 
 * This generation number is guaranteed to be:
 * - Monotonically increasing (always > previous)
 * - Unique (quorum ensures no duplicates)
 * - Agreed upon by majority of replicas
 */
public record NextGenerationResponse(long generation) {
}

