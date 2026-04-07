package com.distribpatterns.paxos.messages;

import com.distribpatterns.paxos.Operation;

/**
 * Internal Paxos message object carrying the original client request details
 * through the prepare, propose, and commit phases.
 */
public record ClientMessage(String clientKey, Operation operation) {
}
