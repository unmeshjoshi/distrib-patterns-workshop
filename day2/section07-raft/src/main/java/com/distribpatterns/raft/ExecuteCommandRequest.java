package com.distribpatterns.raft;

/**
 * Client request to execute an operation.
 */
public record ExecuteCommandRequest(Operation operation) {
}


