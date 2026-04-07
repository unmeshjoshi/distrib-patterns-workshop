package com.distribpatterns.multipaxos.messages;

/**
 * Response to GetValueRequest.
 */
public record GetValueResponse(String value) {
}
