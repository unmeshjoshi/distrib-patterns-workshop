package com.distribpatterns.paxos;

/**
 * Response to CommitRequest.
 * 
 * @param committed Whether the value was committed and executed
 */
public record CommitResponse(boolean committed) {
}

