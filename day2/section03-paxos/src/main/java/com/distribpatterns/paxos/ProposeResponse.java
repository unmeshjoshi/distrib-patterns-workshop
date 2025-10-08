package com.distribpatterns.paxos;

/**
 * Phase 2b message: Acceptor's response to ProposeRequest.
 * 
 * @param accepted Whether the acceptor accepted the proposed value
 */
public record ProposeResponse(boolean accepted) {
}

