package com.distribpatterns.raft;

/**
 * No-op operation used for log consistency checks and leader election.
 */
public record NoOpOperation() implements Operation {
}


