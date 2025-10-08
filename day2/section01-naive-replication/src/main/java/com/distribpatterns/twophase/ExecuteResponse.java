package com.distribpatterns.twophase;

/**
 * Response to execute request.
 * Sent after the operation has been committed and executed.
 */
public record ExecuteResponse(boolean success, int newValue) {
}

