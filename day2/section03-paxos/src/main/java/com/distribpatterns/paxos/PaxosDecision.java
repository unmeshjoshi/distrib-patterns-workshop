package com.distribpatterns.paxos;

/**
 * One decision an acceptor makes, as it is written to the write-ahead log. The log holds every decision in the
 * order it was made, and {@link PaxosState} is what you get by applying them in that order.
 * <p>
 * A promise carries no value. An accept and a commit carry the operation.
 */
public record PaxosDecision(Kind kind, long generation, Operation value) {

    public enum Kind {PROMISE, ACCEPT, COMMIT}

    public static PaxosDecision promise(long generation) {
        return new PaxosDecision(Kind.PROMISE, generation, null);
    }

    public static PaxosDecision accept(long generation, Operation value) {
        return new PaxosDecision(Kind.ACCEPT, generation, value);
    }

    public static PaxosDecision commit(long generation, Operation value) {
        return new PaxosDecision(Kind.COMMIT, generation, value);
    }

    /** The state after this decision. */
    public PaxosState applyTo(PaxosState state) {
        return switch (kind) {
            case PROMISE -> state.promise((int) generation);
            case ACCEPT -> state.accept(generation, value);
            case COMMIT -> state.commit(generation, value);
        };
    }
}
