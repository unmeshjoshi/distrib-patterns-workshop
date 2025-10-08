package com.distribpatterns.raft;

/**
 * Response to execute command request.
 */
public record ExecuteCommandResponse(boolean success, String value) {
}


