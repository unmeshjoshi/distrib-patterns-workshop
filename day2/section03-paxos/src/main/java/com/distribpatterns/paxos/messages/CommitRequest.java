package com.distribpatterns.paxos.messages;

import com.distribpatterns.paxos.Operation;

/**
 * Phase 3 (Learn) message: Coordinator tells acceptors that consensus
 * has been reached and the value should be committed and executed.
 */
public record CommitRequest(long generation, ClientMessage clientMessage, Operation committedOperation) {
    public String clientKey() {
        return clientMessage.clientKey();
    }
}
