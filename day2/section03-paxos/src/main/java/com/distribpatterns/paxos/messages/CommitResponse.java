package com.distribpatterns.paxos.messages;

/**
 * Response to CommitRequest.
 * 
 * @param committed Whether the value was committed and executed
 */
public record CommitResponse(boolean committed) {
}

