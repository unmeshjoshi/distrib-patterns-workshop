package com.distribpatterns.paxoslog.messages;

import com.distribpatterns.paxoslog.Operation;

/**
 * Client request to execute an operation in the replicated log.
 */
public record ExecuteCommandRequest(Operation operation) {
}

