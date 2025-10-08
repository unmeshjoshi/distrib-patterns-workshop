package com.distribpatterns.paxoslog;

/**
 * Response to ExecuteCommandRequest.
 * 
 * @param success Whether the command was successfully committed
 * @param result The result of executing the command (e.g., previous value for CAS)
 */
public record ExecuteCommandResponse(boolean success, String result) {
}

