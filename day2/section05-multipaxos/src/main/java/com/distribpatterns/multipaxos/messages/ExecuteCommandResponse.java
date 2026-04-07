package com.distribpatterns.multipaxos.messages;

/**
 * Response to ExecuteCommandRequest.
 */
public record ExecuteCommandResponse(boolean success, String result) {
}
