package com.distribpatterns.twophase;

/**
 * Phase 1 (Prepare/Accept) request.
 * Coordinator asks participants to prepare the operation without executing it.
 */
public record AcceptRequest(String transactionId, Operation operation) {
}

