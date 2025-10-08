package com.distribpatterns.generation;

import com.tickloom.messaging.MessageType;

/**
 * Message types for Generation Voting protocol.
 * 
 * Generation Voting is a distributed algorithm for generating monotonically 
 * increasing numbers using quorum-based voting (simplified Paxos Phase 1).
 */
public class GenerationMessageTypes {
    // Client-facing messages
    public static final MessageType NEXT_GENERATION_REQUEST = new MessageType("NEXT_GENERATION_REQUEST");
    public static final MessageType NEXT_GENERATION_RESPONSE = new MessageType("NEXT_GENERATION_RESPONSE");
    
    // Internal replica voting messages
    public static final MessageType PREPARE_REQUEST = new MessageType("PREPARE_REQUEST");
    public static final MessageType PREPARE_RESPONSE = new MessageType("PREPARE_RESPONSE");
    
    private GenerationMessageTypes() {
        // Utility class - prevent instantiation
    }
}

