package com.distribpatterns.multipaxos.messages;

/**
 * Client request to get a value from the key-value store.
 */
public record GetValueRequest(String key) {
}
