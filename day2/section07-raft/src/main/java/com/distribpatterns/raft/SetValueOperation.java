package com.distribpatterns.raft;

/**
 * Operation to set a key-value pair in the state machine.
 */
public record SetValueOperation(String key, String value) implements Operation {
}


