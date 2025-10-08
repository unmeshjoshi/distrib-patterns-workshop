package com.distribpatterns.twophase;

/**
 * Phase 2 (Commit) request.
 * Coordinator instructs participants to commit (execute) the prepared operation.
 */
public record CommitRequest(String requestId) {
}

