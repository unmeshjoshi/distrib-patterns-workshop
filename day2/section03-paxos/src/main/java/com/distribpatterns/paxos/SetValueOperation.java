package com.distribpatterns.paxos;

/**
 * Operation to set a key-value pair.
 * Used to demonstrate Paxos with string-based key-value store.
 */
public record SetValueOperation(String key, String value) implements Operation {
}

