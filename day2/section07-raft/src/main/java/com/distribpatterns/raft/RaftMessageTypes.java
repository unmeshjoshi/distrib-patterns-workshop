package com.distribpatterns.raft;

import com.tickloom.messaging.MessageType;

/**
 * Message types for Raft protocol.
 */
public class RaftMessageTypes {
    // Client requests
    public static final MessageType CLIENT_EXECUTE_REQUEST = new MessageType("CLIENT_EXECUTE_REQUEST");
    public static final MessageType CLIENT_EXECUTE_RESPONSE = new MessageType("CLIENT_EXECUTE_RESPONSE");
    public static final MessageType CLIENT_GET_REQUEST = new MessageType("CLIENT_GET_REQUEST");
    public static final MessageType CLIENT_GET_RESPONSE = new MessageType("CLIENT_GET_RESPONSE");
    
    // Raft RPCs
    public static final MessageType REQUEST_VOTE_REQUEST = new MessageType("REQUEST_VOTE_REQUEST");
    public static final MessageType REQUEST_VOTE_RESPONSE = new MessageType("REQUEST_VOTE_RESPONSE");
    public static final MessageType APPEND_ENTRIES_REQUEST = new MessageType("APPEND_ENTRIES_REQUEST");
    public static final MessageType APPEND_ENTRIES_RESPONSE = new MessageType("APPEND_ENTRIES_RESPONSE");
}


