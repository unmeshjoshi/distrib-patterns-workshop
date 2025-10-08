package com.distribpatterns.paxoslog;

import com.tickloom.messaging.MessageType;

/**
 * Message types for PaxosLog protocol.
 */
public class PaxosLogMessageTypes {
    // Client messages
    public static final MessageType CLIENT_EXECUTE_REQUEST = new MessageType("CLIENT_EXECUTE_REQUEST");
    public static final MessageType CLIENT_EXECUTE_RESPONSE = new MessageType("CLIENT_EXECUTE_RESPONSE");
    public static final MessageType CLIENT_GET_REQUEST = new MessageType("CLIENT_GET_REQUEST");
    public static final MessageType CLIENT_GET_RESPONSE = new MessageType("CLIENT_GET_RESPONSE");
    
    // Paxos Phase 1: Prepare/Promise
    public static final MessageType PREPARE_REQUEST = new MessageType("PREPARE_REQUEST");
    public static final MessageType PREPARE_RESPONSE = new MessageType("PREPARE_RESPONSE");
    
    // Paxos Phase 2: Propose/Accept
    public static final MessageType PROPOSE_REQUEST = new MessageType("PROPOSE_REQUEST");
    public static final MessageType PROPOSE_RESPONSE = new MessageType("PROPOSE_RESPONSE");
    
    // Paxos Phase 3: Commit/Learn
    public static final MessageType COMMIT_REQUEST = new MessageType("COMMIT_REQUEST");
    public static final MessageType COMMIT_RESPONSE = new MessageType("COMMIT_RESPONSE");
}

