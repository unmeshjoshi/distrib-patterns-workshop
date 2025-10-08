package com.distribpatterns.paxoslog;

import java.util.Optional;

/**
 * Immutable Paxos state for a single log index.
 * 
 * Each log index maintains its own independent Paxos state.
 */
public record PaxosState(
    int promisedGeneration,
    Optional<Integer> acceptedGeneration,
    Optional<Operation> acceptedValue,
    Optional<Operation> committedValue
) {
    
    /**
     * Creates initial empty state.
     */
    public PaxosState() {
        this(0, Optional.empty(), Optional.empty(), Optional.empty());
    }
    
    /**
     * Can we promise this generation?
     */
    public boolean canPromise(int generation) {
        return generation > this.promisedGeneration;
    }
    
    /**
     * Promise to not accept any generation lower than this one.
     */
    public PaxosState promise(int generation) {
        return new PaxosState(generation, acceptedGeneration, acceptedValue, committedValue);
    }
    
    /**
     * Can we accept a proposal with this generation?
     */
    public boolean canAccept(int generation) {
        return generation == promisedGeneration || generation > promisedGeneration;
    }
    
    /**
     * Accept a proposed value with this generation.
     */
    public PaxosState accept(int generation, Operation value) {
        return new PaxosState(generation, Optional.of(generation), Optional.of(value), committedValue);
    }
    
    /**
     * Commit the value with this generation.
     */
    public PaxosState commit(int generation, Operation value) {
        return new PaxosState(generation, Optional.of(generation), Optional.of(value), Optional.of(value));
    }
    
    @Override
    public String toString() {
        return "PaxosState{" +
                "promised=" + promisedGeneration +
                ", accepted=" + acceptedGeneration +
                ", acceptedVal=" + acceptedValue +
                ", committed=" + committedValue +
                '}';
    }
}

