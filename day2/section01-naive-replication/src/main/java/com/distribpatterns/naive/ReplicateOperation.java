package com.distribpatterns.naive;

/**
 * Internal message for naive replication
 * Used to replicate operations to followers
 */
public record ReplicateOperation(String key, int delta) {
}

