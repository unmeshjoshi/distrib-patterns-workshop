package com.distribpatterns.naive;

/**
 * Request to get a counter value
 * Serialization is handled by tickloom's Message infrastructure
 */
public record GetRequest(String key) {
}
