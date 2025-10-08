package com.distribpatterns.paxoslog;

/**
 * Atomic compare-and-swap operation.
 * Sets newValue only if current value equals expectedValue.
 */
public record CompareAndSwapOperation(
    String key,
    String expectedValue,  // null means "expect key to not exist"
    String newValue
) implements Operation {
}
