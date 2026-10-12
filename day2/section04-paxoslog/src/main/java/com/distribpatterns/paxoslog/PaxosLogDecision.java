package com.distribpatterns.paxoslog;

import com.distribpatterns.paxoslog.messages.Operation;

import java.util.Map;

/**
 * One decision an acceptor makes at one log index, as it is written to the write-ahead log. A single log holds the
 * decisions for every index in the order they were made, and the {@link PaxosState} of each index is what you get by
 * applying that index's decisions in that order.
 * <p>
 * A promise carries no value. An accept and a commit carry the operation.
 */
public record PaxosLogDecision(Kind kind, int logIndex, int generation, Operation value) {

    public enum Kind {PROMISE, ACCEPT, COMMIT}

    public static PaxosLogDecision promise(int logIndex, int generation) {
        return new PaxosLogDecision(Kind.PROMISE, logIndex, generation, null);
    }

    public static PaxosLogDecision accept(int logIndex, int generation, Operation value) {
        return new PaxosLogDecision(Kind.ACCEPT, logIndex, generation, value);
    }

    public static PaxosLogDecision commit(int logIndex, int generation, Operation value) {
        return new PaxosLogDecision(Kind.COMMIT, logIndex, generation, value);
    }

    /** Applies this decision to the state of its log index. */
    public void applyTo(Map<Integer, PaxosState> log) {
        PaxosState state = log.getOrDefault(logIndex, new PaxosState());
        log.put(logIndex, switch (kind) {
            case PROMISE -> state.promise(generation);
            case ACCEPT -> state.accept(generation, value);
            case COMMIT -> state.commit(generation, value);
        });
    }
}
