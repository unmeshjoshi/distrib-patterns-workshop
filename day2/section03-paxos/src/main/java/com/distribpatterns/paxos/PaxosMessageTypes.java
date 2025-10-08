package com.distribpatterns.paxos;

import com.tickloom.messaging.MessageType;

/**
 * Message type constants for Paxos protocol.
 */
public class PaxosMessageTypes {
    // Client messages
    public static final MessageType CLIENT_EXECUTE_REQUEST = new MessageType("CLIENT_EXECUTE_REQUEST");
    public static final MessageType CLIENT_EXECUTE_RESPONSE = new MessageType("CLIENT_EXECUTE_RESPONSE");
    
    // Phase 1: Prepare/Promise
    public static final MessageType PREPARE_REQUEST = new MessageType("PREPARE_REQUEST");
    public static final MessageType PREPARE_RESPONSE = new MessageType("PREPARE_RESPONSE");
    
    // Phase 2: Propose/Accept
    public static final MessageType PROPOSE_REQUEST = new MessageType("PROPOSE_REQUEST");
    public static final MessageType PROPOSE_RESPONSE = new MessageType("PROPOSE_RESPONSE");
    
    // Phase 3: Commit/Learn
    public static final MessageType COMMIT_REQUEST = new MessageType("COMMIT_REQUEST");
    public static final MessageType COMMIT_RESPONSE = new MessageType("COMMIT_RESPONSE");
}

