package com.distribpatterns.multipaxosheartbeats;

import com.tickloom.messaging.MessageType;

/**
 * Message types for Multi-Paxos protocol.
 */
public class MultiPaxosMessageTypes {
    // Client messages
    public static final MessageType CLIENT_EXECUTE_REQUEST = new MessageType("CLIENT_EXECUTE_REQUEST");
    public static final MessageType CLIENT_EXECUTE_RESPONSE = new MessageType("CLIENT_EXECUTE_RESPONSE");
    public static final MessageType CLIENT_GET_REQUEST = new MessageType("CLIENT_GET_REQUEST");
    public static final MessageType CLIENT_GET_RESPONSE = new MessageType("CLIENT_GET_RESPONSE");
    
    // Multi-Paxos Phase 0: Leader Election (Full Log Prepare)
    public static final MessageType FULL_LOG_PREPARE_REQUEST = new MessageType("FULL_LOG_PREPARE_REQUEST");
    public static final MessageType FULL_LOG_PREPARE_RESPONSE = new MessageType("FULL_LOG_PREPARE_RESPONSE");
    
    // Multi-Paxos Phase 1: Propose/Accept (NO Prepare phase after election!)
    public static final MessageType PROPOSE_REQUEST = new MessageType("PROPOSE_REQUEST");
    public static final MessageType PROPOSE_RESPONSE = new MessageType("PROPOSE_RESPONSE");
    
    // Multi-Paxos Phase 2: Commit/Learn
    public static final MessageType COMMIT_REQUEST = new MessageType("COMMIT_REQUEST");
    public static final MessageType COMMIT_RESPONSE = new MessageType("COMMIT_RESPONSE");
    
    // Heartbeat messages for leader failure detection
    public static final MessageType HEARTBEAT_REQUEST = new MessageType("HEARTBEAT_REQUEST");
    public static final MessageType HEARTBEAT_RESPONSE = new MessageType("HEARTBEAT_RESPONSE");
}

