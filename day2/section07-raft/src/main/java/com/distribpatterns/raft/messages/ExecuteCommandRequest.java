package com.distribpatterns.raft.messages;

/**
 * Client request to execute an operation.
 */
public record ExecuteCommandRequest(Operation operation) {
}


