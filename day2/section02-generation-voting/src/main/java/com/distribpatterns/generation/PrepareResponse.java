package com.distribpatterns.generation;

/**
 * Prepare/Promise phase response in Generation Voting.
 * 
 * Replica's vote on a proposed generation:
 * - promised = true: Accept this generation (it's higher than current)
 * - promised = false: Reject this generation (already seen higher)
 * 
 * Coordinator needs quorum of promises to win the election.
 */
public record PrepareResponse(boolean promised) {
}

