package com.distribpatterns.paxos;

/**
 * Operation to increment a counter.
 * Used to demonstrate Paxos with simple integer state.
 */
public record IncrementCounterOperation(String key) implements Operation {
}

