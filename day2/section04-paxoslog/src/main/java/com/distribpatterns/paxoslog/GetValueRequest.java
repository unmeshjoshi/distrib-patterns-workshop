package com.distribpatterns.paxoslog;

/**
 * Client request to get a value from the key-value store.
 * Uses a no-op command to ensure we read committed state.
 */
public record GetValueRequest(String key) {
}

