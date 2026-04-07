package com.distribpatterns.paxoslog.messages;

/**
 * Phase 3 response: Acceptor confirms commit.
 */
public record CommitResponse(boolean success) {
}

