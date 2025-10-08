package com.distribpatterns.naive;

/**
 * Response to increment counter request
 * Serialization is handled by tickloom's Message infrastructure
 */
public record IncrementCounterResponse(boolean success, int newValue) {
}

