package com.distribpatterns.multipaxosheartbeats;

import java.util.Map;

/**
 * Response to FullLogPrepareRequest.
 * Contains all uncommitted log entries for log merging.
 */
public record FullLogPrepareResponse(
    boolean promised,
    Map<Integer, PaxosState> uncommittedEntries
) {
    
    public static FullLogPrepareResponse rejected() {
        return new FullLogPrepareResponse(false, Map.of());
    }
    
    public static FullLogPrepareResponse accepted(Map<Integer, PaxosState> uncommittedEntries) {
        return new FullLogPrepareResponse(true, uncommittedEntries);
    }
}

