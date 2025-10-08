package com.distribpatterns.multipaxos;

/**
 * Client request to execute an operation in the replicated log.
 */
public record ExecuteCommandRequest(Operation operation) {
}
