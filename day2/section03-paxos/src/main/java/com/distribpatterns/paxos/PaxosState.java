package com.distribpatterns.paxos;

import java.util.Optional;

/**
 * Immutable Paxos state for a single instance.
 * 
 * State transitions:
 * 1. PREPARE phase: promise(generation) -> updates promisedGeneration
 * 2. PROPOSE phase: accept(generation, value) -> updates acceptedGeneration and acceptedValue
 * 3. COMMIT phase: commit(generation, value) -> updates committedValue
 * 
 * Key invariants:
 * - promisedGeneration >= acceptedGeneration >= committedGeneration
 * - Once a generation is promised, lower generations are rejected
 * - Once a value is committed, it cannot change
 */
public record PaxosState(
    long promisedGeneration,               // Highest generation we've promised not to accept lower generations
    Optional<Long> acceptedGeneration, // Generation of the value we've accepted (but not committed)
    Optional<Operation> acceptedValue,    // The value we've accepted (but not committed)
    Optional<Operation> committedValue    // The value we've committed and executed
) {
    
    /**
     * Creates initial empty state.
     */
    public PaxosState() {
        this(0, Optional.empty(), Optional.empty(), Optional.empty());
    }
    
    /**
     * Can we promise this generation?
     * True if the generation is strictly higher than our current promise.
     */
    public boolean canPromise(int generation) {
        return generation > this.promisedGeneration;
    }
    
    /**
     * Promise to not accept any generation lower than this one.
     * Returns new state with updated promisedGeneration.
     */
    public PaxosState promise(int generation) {
        return new PaxosState(generation, acceptedGeneration, acceptedValue, committedValue);
    }
    
    /**
     * Can we accept a proposal with this generation?
     * True if the generation equals or is higher than our promise.
     */
    public boolean canAccept(long generation) {
        return generation == promisedGeneration || generation > promisedGeneration;
    }
    
    /**
     * Accept a proposed value with this generation.
     * Returns new state with updated acceptedGeneration and acceptedValue.
     */
    public PaxosState accept(long generation, Operation value) {
        return new PaxosState(generation, Optional.of(generation), Optional.of(value), committedValue);
    }
    
    /**
     * Commit the value with this generation.
     * Returns new state with updated committedValue.
     */
    public PaxosState commit(long generation, Operation value) {
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

