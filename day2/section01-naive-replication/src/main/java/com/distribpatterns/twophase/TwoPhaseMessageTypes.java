package com.distribpatterns.twophase;

import com.tickloom.messaging.MessageType;

/**
 * Message types for Two-Phase Commit protocol
 */
public class TwoPhaseMessageTypes {
    // Client-facing operations
    public static final MessageType CLIENT_EXECUTE_REQUEST = new MessageType("CLIENT_EXECUTE_REQUEST");
    public static final MessageType CLIENT_EXECUTE_RESPONSE = new MessageType("CLIENT_EXECUTE_RESPONSE");
    
    // Phase 1: Prepare/Accept
    public static final MessageType ACCEPT_REQUEST = new MessageType("ACCEPT_REQUEST");
    public static final MessageType ACCEPT_RESPONSE = new MessageType("ACCEPT_RESPONSE");
    
    // Phase 2: Commit
    public static final MessageType COMMIT_REQUEST = new MessageType("COMMIT_REQUEST");
    
    private TwoPhaseMessageTypes() {
        // Utility class - prevent instantiation
    }
}

