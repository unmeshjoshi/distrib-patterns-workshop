package com.distribpatterns.multipaxos;

/**
 * Client request to get a value from the key-value store.
 */
public record GetValueRequest(String key) {
}
