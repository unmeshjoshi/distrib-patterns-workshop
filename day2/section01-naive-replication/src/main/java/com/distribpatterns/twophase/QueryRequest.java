package com.distribpatterns.twophase;

/**
 * Phase 0 (Query/CanCommit) request.
 * New coordinator queries all nodes to check for pending accepted requests.
 * This is the key difference between 2PC and 3PC - enables coordinator failure recovery.
 */
public record QueryRequest(String queryId) {
}

