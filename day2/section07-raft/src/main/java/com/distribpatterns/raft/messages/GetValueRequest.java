package com.distribpatterns.raft.messages;

/**
 * Client request to read a value from the state machine.
 */
public record GetValueRequest(String key) {
}


