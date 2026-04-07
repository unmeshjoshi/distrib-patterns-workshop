package com.distribpatterns.raft.messages;

/**
 * Response to execute command request.
 */
public record ExecuteCommandResponse(boolean success, String value) {
}


