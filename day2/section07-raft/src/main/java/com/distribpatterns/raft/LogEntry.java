package com.distribpatterns.raft;

/**
 * Raft log entry.
 * 
 * Each entry contains:
 * - index: Position in the log (1-based)
 * - term: Term when entry was received by leader
 * - operation: State machine command
 * - clientKey: For responding to client after commit (optional)
 */
public record LogEntry(
    int index,
    int term,
    Operation operation,
    String clientKey  // null for non-client operations (e.g., no-ops)
) {
    public LogEntry(int index, int term, Operation operation) {
        this(index, term, operation, null);
    }
}


