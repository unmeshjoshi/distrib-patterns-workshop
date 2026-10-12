package com.distribpatterns.multipaxosheartbeats;

import com.distribpatterns.multipaxosheartbeats.messages.Operation;

/**
 * One decision an acceptor makes, as it is written to the write-ahead log. A single log holds them all, in the order
 * they were made.
 * <p>
 * A promise covers the whole log, so it has no index and no value. An accept also raises the promise to its generation,
 * because the acceptor takes a proposal only at a generation at or above the one it promised. A commit changes no promise.
 */
public record MultiPaxosDecision(Kind kind, int logIndex, int generation, Operation value) {

    public enum Kind {PROMISE, ACCEPT, COMMIT}

    public static MultiPaxosDecision promise(int generation) {
        return new MultiPaxosDecision(Kind.PROMISE, -1, generation, null);
    }

    public static MultiPaxosDecision accept(int logIndex, int generation, Operation value) {
        return new MultiPaxosDecision(Kind.ACCEPT, logIndex, generation, value);
    }

    public static MultiPaxosDecision commit(int logIndex, int generation, Operation value) {
        return new MultiPaxosDecision(Kind.COMMIT, logIndex, generation, value);
    }
}
