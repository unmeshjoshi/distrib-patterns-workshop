package com.distribpatterns.twophase;

/**
 * Phase 1 (Prepare/Accept) response.
 * Participant responds indicating whether it has prepared the operation.
 */
public record AcceptResponse(String transactionId, boolean accepted) {
}

