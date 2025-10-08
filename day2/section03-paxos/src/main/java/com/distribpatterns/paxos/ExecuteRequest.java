package com.distribpatterns.paxos;

/**
 * Client request to execute an operation using Paxos consensus.
 */
public record ExecuteRequest(Operation operation) {
}

