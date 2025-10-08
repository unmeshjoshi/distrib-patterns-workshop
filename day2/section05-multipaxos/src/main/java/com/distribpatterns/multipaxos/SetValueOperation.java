package com.distribpatterns.multipaxos;

/**
 * Operation to set a key-value pair in the store.
 */
public record SetValueOperation(String key, String value) implements Operation {
}
