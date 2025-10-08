package com.distribpatterns.naive;

import com.tickloom.messaging.MessageType;

/**
 * Message types for single server and naive replication protocols
 */
public class MessageTypes {
    // Client-facing operations
    public static final MessageType INCREMENT_REQUEST = new MessageType("INCREMENT_REQUEST");
    public static final MessageType INCREMENT_RESPONSE = new MessageType("INCREMENT_RESPONSE");
    public static final MessageType GET_REQUEST = new MessageType("GET_REQUEST");
    public static final MessageType GET_RESPONSE = new MessageType("GET_RESPONSE");
    
    // Internal replication (naive approach)
    public static final MessageType REPLICATE_OP = new MessageType("REPLICATE_OP");
    public static final MessageType REPLICATE_ACK = new MessageType("REPLICATE_ACK");
}
