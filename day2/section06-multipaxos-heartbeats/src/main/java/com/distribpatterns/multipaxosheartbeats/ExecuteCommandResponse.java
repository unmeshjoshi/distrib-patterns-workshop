package com.distribpatterns.multipaxosheartbeats;

/**
 * Response to ExecuteCommandRequest.
 */
public record ExecuteCommandResponse(boolean success, String result) {
}
