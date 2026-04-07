package com.distribpatterns.multipaxos.messages;

/**
 * No-op operation used for reads (to ensure we see committed state).
 */
public record NoOpOperation() implements Operation {
}
