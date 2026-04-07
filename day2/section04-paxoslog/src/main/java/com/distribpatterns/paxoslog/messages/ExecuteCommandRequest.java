package com.distribpatterns.paxoslog.messages;

/**
 * Client request to execute an operation in the replicated log.
 */
public record ExecuteCommandRequest(Operation operation) {
}

