package com.distribpatterns.multipaxosheartbeats;

import java.util.Optional;

/**
 * Immutable Paxos state for a single log index in Multi-Paxos.
 * 
 * Simplified from PaxosLog: No per-index promisedGeneration 
 * (tracked globally at server level).
 */
public record PaxosState(
    Optional<Integer> acceptedGeneration,
    Optional<Operation> acceptedValue,
    Optional<Operation> committedValue,
    Optional<Integer> committedGeneration
) {
    
    /**
     * Creates initial empty state.
     */
    public PaxosState() {
        this(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }
    
    /**
     * Accept a proposed value with this generation.
     */
    public PaxosState accept(int generation, Operation value) {
        return new PaxosState(Optional.of(generation), Optional.of(value), committedValue, committedGeneration);
    }
    
    /**
     * Commit the value with this generation.
     */
    public PaxosState commit(int generation, Operation value) {
        return new PaxosState(Optional.of(generation), Optional.of(value), Optional.of(value), Optional.of(generation));
    }
    
    @Override
    public String toString() {
        return "PaxosState{" +
                "accepted=" + acceptedGeneration +
                ", acceptedVal=" + acceptedValue +
                ", committed=" + committedValue +
                ", committedGen=" + committedGeneration +
                '}';
    }
}

