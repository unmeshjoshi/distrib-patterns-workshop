package com.distribpatterns.twophase;

/**
 * Operation to increment a counter by a delta value.
 * Serialization handled by tickloom's MessageCodec.
 */
public record IncrementCounterOperation(String key, int delta) implements Operation {
}

