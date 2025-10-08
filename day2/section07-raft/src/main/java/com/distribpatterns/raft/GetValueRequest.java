package com.distribpatterns.raft;

/**
 * Client request to read a value from the state machine.
 */
public record GetValueRequest(String key) {
}


