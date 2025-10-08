package com.distribpatterns.naive;

/**
 * Request to increment a counter
 * Serialization is handled by tickloom's Message infrastructure
 */
public record IncrementCounterRequest(String key, int delta) {
}

