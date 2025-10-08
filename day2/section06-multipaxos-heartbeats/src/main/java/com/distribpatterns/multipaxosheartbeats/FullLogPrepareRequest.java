package com.distribpatterns.multipaxosheartbeats;

/**
 * Leader election prepare request.
 * Requests promise for ALL future log indices (not just one).
 */
public record FullLogPrepareRequest(int generation) {
}

