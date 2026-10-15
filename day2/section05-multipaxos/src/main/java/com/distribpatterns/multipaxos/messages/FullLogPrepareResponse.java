package com.distribpatterns.multipaxos.messages;

import com.distribpatterns.multipaxos.PaxosState;

import java.util.Map;

/**
 * Response to FullLogPrepareRequest.
 * Contains the acceptor's entire log, committed entries included, for log merging.
 */
public record FullLogPrepareResponse(
    boolean promised,
    Map<Integer, PaxosState> log
) {

    public static FullLogPrepareResponse rejected() {
        return new FullLogPrepareResponse(false, Map.of());
    }

    public static FullLogPrepareResponse accepted(Map<Integer, PaxosState> log) {
        return new FullLogPrepareResponse(true, log);
    }
}
