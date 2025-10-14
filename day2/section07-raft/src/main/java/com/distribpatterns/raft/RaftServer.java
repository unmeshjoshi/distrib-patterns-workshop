package com.distribpatterns.raft;

import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.Replica;
import com.tickloom.messaging.*;
import com.tickloom.network.MessageCodec;
import com.tickloom.storage.Storage;
import com.tickloom.util.Clock;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Raft consensus server implementation.
 *
 * Based on "In Search of an Understandable Consensus Algorithm (Extended Version)"
 * by Diego Ongaro and John Ousterhout.
 *
 * KEY DIFFERENCES FROM MULTI-PAXOS:
 * 1. Election Restriction (§5.4.1): Voters only grant votes to candidates with up-to-date logs
 * 2. Current-Term Commit Rule (§5.4.2): Only commit entries from current term
 * 3. Log Consistency Check (§5.3): AppendEntries includes prevLogIndex/prevLogTerm
 */
public class RaftServer extends Replica {

    // ========== PERSISTENT STATE (Figure 2) ==========
    // Should survive crashes - stored in storage
    private int currentTerm = 0;                    // Latest term server has seen
    private ProcessId votedFor = null;             // CandidateId that received vote in current term
    private List<LogEntry> log = new ArrayList<>(); // Log entries (1-indexed, 0 is unused)

    // ========== VOLATILE STATE (all servers) ==========
    private RaftState state = RaftState.FOLLOWER;
    private int commitIndex = 0;                    // Highest log entry known to be committed
    private int lastApplied = 0;                    // Highest log entry applied to state machine

    // ========== VOLATILE STATE (leaders only) ==========
    private Map<ProcessId, Integer> nextIndex = new HashMap<>();   // Next log index to send to each server
    private Map<ProcessId, Integer> matchIndex = new HashMap<>();  // Highest log index replicated on each server

    // ========== STATE MACHINE ==========
    private Map<String, String> kvStore = new HashMap<>();

    // ========== ELECTION TIMEOUT ==========
    private int electionTimeoutTicks;
    private int currentTickCount = 0;
    private int lastHeartbeatTick = 0;
    private static final int HEARTBEAT_INTERVAL_TICKS = 10;

    // ========== LEADER TRACKING ==========
    private ProcessId currentLeader = null;

    public RaftServer(List<ProcessId> allNodes, ProcessParams processParams, int electionTimeoutTicks) {
        super(allNodes, processParams);

        // Initialize log with dummy entry at index 0
        log.add(new LogEntry(0, 0, new NoOpOperation()));

        this.electionTimeoutTicks = electionTimeoutTicks;
        resetElectionTimeout();

        System.out.println(id + ": Raft server started as FOLLOWER (electionTimeout=" +
                         electionTimeoutTicks + " ticks)");
    }

    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        Map<MessageType, Handler> handlers = new HashMap<>();

        // Client requests
        handlers.put(RaftMessageTypes.CLIENT_EXECUTE_REQUEST, this::handleClientExecuteRequest);
        handlers.put(RaftMessageTypes.CLIENT_GET_REQUEST, this::handleClientGetRequest);

        // Raft RPCs
        handlers.put(RaftMessageTypes.REQUEST_VOTE_REQUEST, this::handleRequestVoteRequest);
        handlers.put(RaftMessageTypes.REQUEST_VOTE_RESPONSE, this::handleRequestVoteResponse);
        handlers.put(RaftMessageTypes.APPEND_ENTRIES_REQUEST, this::handleAppendEntriesRequest);
        handlers.put(RaftMessageTypes.APPEND_ENTRIES_RESPONSE, this::handleAppendEntriesResponse);

        return handlers;
    }

    @Override
    public void onTick() {
        super.onTick();
        currentTickCount++;

        if (state == RaftState.LEADER) {
            sendHeartbeats();
        } else {
            checkElectionTimeout();
        }
    }

    // ========== CLIENT REQUEST HANDLING ==========

    private void handleClientExecuteRequest(Message message) {
        if (state != RaftState.LEADER) {
            System.out.println(id + ": Not leader, rejecting client request");
            send(createMessage(message.source(), message.correlationId(),
                new ExecuteCommandResponse(false, null), RaftMessageTypes.CLIENT_EXECUTE_RESPONSE));
            return;
        }

        ExecuteCommandRequest req = deserializePayload(message.payload(), ExecuteCommandRequest.class);
        System.out.println(id + ": Leader received client request: " + req.operation());

        // Register client callback that will be invoked when entry is committed
        String clientKey = message.correlationId();


        // Append to local log
        int newIndex = log.size();
        LogEntry entry = new LogEntry(newIndex, currentTerm, req.operation(), clientKey);
        log.add(entry);
        waitingList.add(String.valueOf(newIndex), new RequestCallback<Object>() {
            @Override
            public void onResponse(Object response, ProcessId fromNode) {
                var responseMsg = createMessage(message.source(), message.correlationId(),
                        response, RaftMessageTypes.CLIENT_EXECUTE_RESPONSE);
                send(responseMsg);
                System.out.println(id + ": Sent response to client for " + clientKey);
            }

            @Override
            public void onError(Exception error) {
                send(createMessage(message.source(), message.correlationId(),
                        new ExecuteCommandResponse(false, null), RaftMessageTypes.CLIENT_EXECUTE_RESPONSE));
            }
        });
        System.out.println(id + ": Appended entry at index " + newIndex + " (term " + currentTerm + ")");

        // Replicate to followers
        replicateToFollowers();
    }

    private void handleClientGetRequest(Message message) {
        GetValueRequest req = deserializePayload(message.payload(), GetValueRequest.class);
        String value = kvStore.get(req.key());
        send(createMessage(message.source(), message.correlationId(),
            new GetValueResponse(value), RaftMessageTypes.CLIENT_GET_RESPONSE));
    }

    // ========== RAFT RPC: REQUEST VOTE ==========

    private void handleRequestVoteRequest(Message message) {
        RequestVoteRequest req = deserializePayload(message.payload(), RequestVoteRequest.class);

        System.out.println(id + ": Received RequestVote from " + req.candidateId() +
                         " for term " + req.term());

        // Update term if we see a higher one
        if (req.term() > currentTerm) {
            becomeFollower(req.term());
        }

        // ✅ RAFT ELECTION RESTRICTION (§5.4.1)
        // Only grant vote if:
        // 1. Term is >= our current term
        // 2. We haven't already voted for someone else this term
        // 3. Candidate's log is at least as up-to-date as ours

        boolean termOk = req.term() >= currentTerm;
        boolean notVoted = (votedFor == null || votedFor.equals(req.candidateId()));
        boolean logUpToDate = isLogUpToDate(req.lastLogIndex(), req.lastLogTerm());

        boolean voteGranted = termOk && notVoted && logUpToDate;

        if (voteGranted) {
            votedFor = req.candidateId();
            resetElectionTimeout();
            System.out.println(id + ": Granted vote to " + req.candidateId() +
                             " (log up-to-date: lastIndex=" + req.lastLogIndex() +
                             ", lastTerm=" + req.lastLogTerm() + ")");
        } else {
            System.out.println(id + ": Denied vote to " + req.candidateId() +
                             " (termOk=" + termOk + ", notVoted=" + notVoted +
                             ", logUpToDate=" + logUpToDate + ")");
        }

        send(createMessage(message.source(), message.correlationId(),
            new RequestVoteResponse(currentTerm, voteGranted), RaftMessageTypes.REQUEST_VOTE_RESPONSE));
    }

    /**
     * ✅ RAFT ELECTION RESTRICTION (§5.4.1)
     *
     * Candidate's log is "up-to-date" if:
     * 1. Last log term is HIGHER than ours, OR
     * 2. Last log term is SAME and log length is >= ours
     *
     * This ensures only candidates with complete logs can become leader.
     */
    private boolean isLogUpToDate(int candidateLastIndex, int candidateLastTerm) {
        int myLastIndex = getLastLogIndex();
        int myLastTerm = getLastLogTerm();

        if (candidateLastTerm > myLastTerm) return true;
        if (candidateLastTerm == myLastTerm && candidateLastIndex >= myLastIndex) return true;
        return false;
    }

    private void handleRequestVoteResponse(Message message) {
        RequestVoteResponse resp = deserializePayload(message.payload(), RequestVoteResponse.class);
        waitingList.handleResponse(message.correlationId(), resp, message.source());
    }

    // ========== RAFT RPC: APPEND ENTRIES ==========

    private void handleAppendEntriesRequest(Message message) {
        AppendEntriesRequest req = deserializePayload(message.payload(), AppendEntriesRequest.class);

        // Update term if we see a higher one
        if (req.term() > currentTerm) {
            becomeFollower(req.term());
        }

        // Reject if term is lower
        if (req.term() < currentTerm) {
            send(createMessage(message.source(), message.correlationId(),
                new AppendEntriesResponse(currentTerm, false, 0), RaftMessageTypes.APPEND_ENTRIES_RESPONSE));
            return;
        }

        // Valid leader - reset election timeout and update leader
        resetElectionTimeout();
        currentLeader = req.leaderId();

        if (state != RaftState.FOLLOWER) {
            becomeFollower(req.term());
        }

        // ✅ RAFT LOG CONSISTENCY CHECK (§5.3)
        // Reject if we don't have an entry at prevLogIndex with matching term
        if (req.prevLogIndex() > 0) {
            if (req.prevLogIndex() >= log.size()) {
                System.out.println(id + ": Rejecting AppendEntries - prevLogIndex " +
                                 req.prevLogIndex() + " beyond log end");
                send(createMessage(message.source(), message.correlationId(),
                    new AppendEntriesResponse(currentTerm, false, 0), RaftMessageTypes.APPEND_ENTRIES_RESPONSE));
                return;
            }

            LogEntry prevEntry = log.get(req.prevLogIndex());
            if (prevEntry.term() != req.prevLogTerm()) {
                System.out.println(id + ": Rejecting AppendEntries - prevLogTerm mismatch at index " +
                                 req.prevLogIndex() + " (expected " + req.prevLogTerm() +
                                 ", got " + prevEntry.term() + ")");
                send(createMessage(message.source(), message.correlationId(),
                    new AppendEntriesResponse(currentTerm, false, 0), RaftMessageTypes.APPEND_ENTRIES_RESPONSE));
                return;
            }
        }

        // ✅ APPEND ENTRIES (§5.3)
        // Delete conflicting entries and append new ones
        if (!req.entries().isEmpty()) {
            int insertIndex = req.prevLogIndex() + 1;

            for (int i = 0; i < req.entries().size(); i++) {
                LogEntry newEntry = req.entries().get(i);
                int entryIndex = insertIndex + i;

                if (entryIndex < log.size()) {
                    LogEntry existingEntry = log.get(entryIndex);
                    if (existingEntry.term() != newEntry.term()) {
                        // Conflict: delete this and all following entries
                        System.out.println(id + ": Conflict at index " + entryIndex +
                                         " - truncating log from here");
                        while (log.size() > entryIndex) {
                            log.remove(log.size() - 1);
                        }
                        log.add(newEntry);
                    }
                } else {
                    log.add(newEntry);
                }
            }

            System.out.println(id + ": Appended " + req.entries().size() +
                             " entries starting at index " + insertIndex);
        }

        // ✅ UPDATE COMMIT INDEX (§5.3)
        if (req.leaderCommit() > commitIndex) {
            commitIndex = Math.min(req.leaderCommit(), getLastLogIndex());
            System.out.println(id + ": Updated commitIndex to " + commitIndex);
            applyCommittedEntries();
        }

        // Send response with our current last log index (what we have replicated)
        send(createMessage(message.source(), message.correlationId(),
            new AppendEntriesResponse(currentTerm, true, getLastLogIndex()),
            RaftMessageTypes.APPEND_ENTRIES_RESPONSE));
    }

    private void handleAppendEntriesResponse(Message message) {
        if (state != RaftState.LEADER) return;

        AppendEntriesResponse resp = deserializePayload(message.payload(), AppendEntriesResponse.class);
        ProcessId follower = message.source();

        if (resp.term() > currentTerm) {
            becomeFollower(resp.term());
            return;
        }

        if (resp.success()) {
            // Update nextIndex and matchIndex based on what follower actually has
            matchIndex.put(follower, resp.matchIndex());
            nextIndex.put(follower, resp.matchIndex() + 1);

            System.out.println(id + ": " + follower + " replicated up to index " + resp.matchIndex());

            // ✅ RAFT COMMIT RULE (§5.4.2)
            updateCommitIndex();
        } else {
            // Log inconsistency - decrement nextIndex and retry
            int next = nextIndex.getOrDefault(follower, log.size());
            nextIndex.put(follower, Math.max(1, next - 1));
            System.out.println(id + ": " + follower + " log inconsistent, retrying with nextIndex=" +
                             nextIndex.get(follower));
            sendAppendEntries(follower);
        }
    }

    // ========== LEADER ELECTION ==========

    private void checkElectionTimeout() {
        int ticksSinceHeartbeat = currentTickCount - lastHeartbeatTick;

        if (ticksSinceHeartbeat >= electionTimeoutTicks) {
            System.out.println(id + ": Election timeout after " + ticksSinceHeartbeat +
                             " ticks! Starting election");
            startElection();
        }
    }

    private void startElection() {
        becomeCandidate();

        // Vote for self
        votedFor = id;
        int votesReceived = 1;

        System.out.println(id + ": Starting election for term " + currentTerm);

        // Create quorum callback
        var quorumCallback = new AsyncQuorumCallback<RequestVoteResponse>(
            getAllNodes().size(),
            resp -> resp != null && resp.voteGranted()
        );

        quorumCallback.onSuccess(responses -> {
            if (state == RaftState.CANDIDATE) {
                becomeLeader();
            }
        });

        quorumCallback.onFailure(error -> {
            System.out.println(id + ": Election failed: " + error.getMessage());
        });

        // Broadcast RequestVote
        int lastLogIndex = getLastLogIndex();
        int lastLogTerm = getLastLogTerm();

        RequestVoteRequest req = new RequestVoteRequest(currentTerm, id, lastLogIndex, lastLogTerm);
        broadcastToAllReplicas(quorumCallback, (node, correlationId) ->
            createMessage(node, correlationId, req, RaftMessageTypes.REQUEST_VOTE_REQUEST)
        );
    }

    // ========== STATE TRANSITIONS ==========

    private void becomeFollower(int newTerm) {
        System.out.println(id + ": Becoming FOLLOWER for term " + newTerm);
        state = RaftState.FOLLOWER;
        currentTerm = newTerm;
        votedFor = null;
        resetElectionTimeout();
    }

    private void becomeCandidate() {
        System.out.println(id + ": Becoming CANDIDATE");
        state = RaftState.CANDIDATE;
        currentTerm++;
        resetElectionTimeout();
    }

    private void becomeLeader() {
        System.out.println(id + ": Became LEADER for term " + currentTerm);
        state = RaftState.LEADER;
        currentLeader = id;

        // Initialize nextIndex and matchIndex
        for (ProcessId peer : getAllNodes()) {
            nextIndex.put(peer, log.size());
            matchIndex.put(peer, 0);
        }

        // Send initial heartbeat
        sendHeartbeats();
    }

    // ========== HEARTBEATS ==========

    private void sendHeartbeats() {
        if (state != RaftState.LEADER) return;

        for (ProcessId peer : getAllNodes()) {
            if (!peer.equals(id)) {
                sendAppendEntries(peer);
            }
        }
    }

    private void sendAppendEntries(ProcessId peer) {
        int next = nextIndex.getOrDefault(peer, log.size());
        int prevIndex = next - 1;
        int prevTerm = log.get(prevIndex).term();

        List<LogEntry> entries = new ArrayList<>();
        for (int i = next; i < log.size(); i++) {
            entries.add(log.get(i));
        }

        AppendEntriesRequest req = new AppendEntriesRequest(
            currentTerm, id, prevIndex, prevTerm, entries, commitIndex
        );

        String correlationId = UUID.randomUUID().toString();
        send(createMessage(peer, correlationId, req, RaftMessageTypes.APPEND_ENTRIES_REQUEST));
    }

    private void replicateToFollowers() {
        for (ProcessId peer : getAllNodes()) {
            if (!peer.equals(id)) {
                sendAppendEntries(peer);
            }
        }
    }

    // ========== COMMIT LOGIC ==========

    /**
     * ✅ RAFT COMMIT RULE (§5.4.2)
     *
     * Leader only commits entries from its CURRENT TERM once replicated to majority.
     * Old-term entries are committed INDIRECTLY when a current-term entry is committed.
     *
     * This prevents the "Figure 8" scenario where old-term entries get overwritten.
     */
    private void updateCommitIndex() {
        if (state != RaftState.LEADER) return;

        // Try to advance commit index
        for (int n = commitIndex + 1; n < log.size(); n++) {
            LogEntry entry = log.get(n);

            // ✅ CRITICAL: Only commit entries from CURRENT term
            if (entry.term() != currentTerm) {
                continue;  // Skip old-term entries
            }

            // Count how many servers have replicated this entry
            int replicationCount = 1; // Leader has it
            for (ProcessId peer : getAllNodes()) {
                if (!peer.equals(id) && matchIndex.getOrDefault(peer, 0) >= n) {
                    replicationCount++;
                }
            }

            // If majority have it, commit it (and all prior entries)
            if (replicationCount >= getMajority()) {
                commitIndex = n;
                System.out.println(id + ": Committed index " + n + " (term " + entry.term() +
                                 ") - replicated on " + replicationCount + " servers");
                applyCommittedEntries();
            }
        }
    }

    private void applyCommittedEntries() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry entry = log.get(lastApplied);

            System.out.println(id + ": Applying entry " + lastApplied + ": " + entry.operation());

            // Execute operation on state machine
            String result = executeOperation(entry.operation());

            // Respond to client if this was their request
            waitingList.handleResponse(String.valueOf(lastApplied),
                    new ExecuteCommandResponse(true, result), id);

        }
    }

    private String executeOperation(Operation operation) {
        if (operation instanceof SetValueOperation setOp) {
            kvStore.put(setOp.key(), setOp.value());
            return setOp.value();
        }
        return null;
    }

    // ========== HELPERS ==========

    private int getLastLogIndex() {
        return log.size() - 1;
    }

    private int getLastLogTerm() {
        return log.isEmpty() ? 0 : log.get(log.size() - 1).term();
    }

    private int getMajority() {
        return (getAllNodes().size() / 2) + 1;
    }

    private void resetElectionTimeout() {
        lastHeartbeatTick = currentTickCount;
    }

    // ========== PUBLIC API (for testing) ==========

    public boolean isLeader() {
        return state == RaftState.LEADER;
    }

    public RaftState getState() {
        return state;
    }

    public int getCurrentTerm() {
        return currentTerm;
    }

    public String getValue(String key) {
        return kvStore.get(key);
    }

    public int getCommitIndex() {
        return commitIndex;
    }

    public LogEntry getLogEntry(int index) {
        return index < log.size() ? log.get(index) : null;
    }
}

