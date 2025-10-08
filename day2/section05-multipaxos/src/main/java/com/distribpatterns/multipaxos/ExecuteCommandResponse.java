package com.distribpatterns.multipaxos;

/**
 * Response to ExecuteCommandRequest.
 */
public record ExecuteCommandResponse(boolean success, String result) {
}
