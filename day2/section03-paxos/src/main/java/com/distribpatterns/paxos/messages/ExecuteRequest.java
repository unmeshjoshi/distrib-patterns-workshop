package com.distribpatterns.paxos.messages;

import com.distribpatterns.paxos.Operation;

/**
 * Client request to execute an operation using Paxos consensus.
 */
public record ExecuteRequest(Operation operation) {
}

