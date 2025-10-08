package com.distribpatterns.paxos;

/**
 * Response to client ExecuteRequest.
 * 
 * @param success Whether the operation was successfully committed
 * @param result The result of executing the operation (e.g., new counter value)
 */
public record ExecuteResponse(boolean success, String result) {
}

