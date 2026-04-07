package com.distribpatterns.multipaxos.messages;

/**
 * Response to CommitRequest.
 */
public record CommitResponse(boolean success) {
}

