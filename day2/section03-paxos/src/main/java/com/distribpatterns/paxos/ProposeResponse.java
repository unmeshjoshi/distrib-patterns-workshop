package com.distribpatterns.paxos;

/**
 * Phase 2b message: Acceptor's response to ProposeRequest.
 * 
 * @param accepted Whether the acceptor accepted the proposed value
 * @param generation The generation this response applies to
 */
public record ProposeResponse(boolean accepted, long generation) {
}
