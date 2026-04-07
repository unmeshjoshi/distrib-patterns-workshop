package com.distribpatterns.multipaxosheartbeats.messages;

/**
 * Response to ExecuteCommandRequest.
 */
public record ExecuteCommandResponse(boolean success, String result) {
}
