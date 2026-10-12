package com.distribpatterns.raft;

import com.tickloom.ProcessId;

/**
 * One record of the write-ahead log. Raft's persistent state (Figure 2) is the current term, the vote in that term, and
 * the log, so the log file holds two kinds of record, in the order they were written:
 * <ul>
 *   <li>{@code HARD_STATE}: the term and the vote, written whenever either changes. The last one wins.</li>
 *   <li>{@code ENTRY}: a log entry written at its index. Writing index i discards any entry at i or after, which is
 *       how a follower's conflicting suffix is removed, so no separate truncate record is needed.</li>
 * </ul>
 * The commit index is not logged. A restarted server learns it again from the leader.
 */
public record RaftRecord(Kind kind, int term, ProcessId votedFor, LogEntry entry) {

    public enum Kind {HARD_STATE, ENTRY}

    public static RaftRecord hardState(int term, ProcessId votedFor) {
        return new RaftRecord(Kind.HARD_STATE, term, votedFor, null);
    }

    public static RaftRecord entry(LogEntry entry) {
        return new RaftRecord(Kind.ENTRY, entry.term(), null, entry);
    }
}
