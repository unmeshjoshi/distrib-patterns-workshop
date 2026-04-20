package com.distribpatterns.raft;

import com.distribpatterns.raft.messages.*;
import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.Replica;
import com.tickloom.messaging.AsyncQuorumCallback;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageType;
import com.tickloom.messaging.RequestCallback;
import com.tickloom.util.Timeout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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

    private int currentTerm = 0;
    private ProcessId votedFor = null;
    private final List<LogEntry> log = new ArrayList<>();

    // ========== VOLATILE STATE (all servers) ==========

    private RaftState state = RaftState.FOLLOWER;
    private int commitIndex = 0;
    private int lastApplied = 0;

    // ========== VOLATILE STATE (leaders only) ==========

    private final Map<ProcessId, Integer> nextIndex = new HashMap<>();
    private final Map<ProcessId, Integer> matchIndex = new HashMap<>();

    // ========== STATE MACHINE ==========

    private final Map<String, String> kvStore = new HashMap<>();

    // ========== TIMEOUTS ==========

    private static final int HEARTBEAT_INTERVAL_TICKS = 10;

    private final Timeout electionTimeout;
    private final Timeout heartbeatInterval;

    // ========== LEADER TRACKING ==========

    private ProcessId currentLeader = null;

    public RaftServer(List<ProcessId> allNodes, ProcessParams processParams, int electionTimeoutTicks) {
        super(allNodes, processParams);

        log.add(new LogEntry(0, 0, new NoOpOperation()));

        this.electionTimeout = new Timeout(id + "-election-timeout", electionTimeoutTicks);
        this.heartbeatInterval = new Timeout(id + "-heartbeat-interval", HEARTBEAT_INTERVAL_TICKS);

        becomeFollower(currentTerm);

        System.out.println(id + ": Raft server started as FOLLOWER (electionTimeout=" +
            electionTimeoutTicks + " ticks)");
    }

    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
            RaftMessageTypes.CLIENT_EXECUTE_REQUEST, this::handleClientExecuteRequest,
            RaftMessageTypes.CLIENT_GET_REQUEST, this::handleClientGetRequest,
            RaftMessageTypes.REQUEST_VOTE_REQUEST, this::handleRequestVoteRequest,
            RaftMessageTypes.REQUEST_VOTE_RESPONSE, this::handleRequestVoteResponse,
            RaftMessageTypes.APPEND_ENTRIES_REQUEST, this::handleAppendEntriesRequest,
            RaftMessageTypes.APPEND_ENTRIES_RESPONSE, this::handleAppendEntriesResponse
        );
    }

    @Override
    public void onTick() {
        super.onTick();
        electionTimeout.tick();
        heartbeatInterval.tick();

        if (state == RaftState.LEADER) {
            sendHeartbeatsWhenDue();
            return;
        }

        startElectionWhenTimedOut();
    }

    // ========== CLIENT REQUEST HANDLING ==========

    private void handleClientExecuteRequest(Message clientMessage) {
        if (rejectClientExecuteRequestIfNotLeader(clientMessage)) {
            return;
        }

        ExecuteCommandRequest request = deserializePayload(clientMessage.payload(), ExecuteCommandRequest.class);
        String clientKey = clientKeyFor(clientMessage);

        System.out.println(id + ": Leader received client request: " + request.operation());

        int newIndex = appendLogEntry(request.operation(), clientKey);
        registerClientExecuteCallback(clientMessage, clientKey, newIndex);
        replicateToFollowers();
    }

    private void handleClientGetRequest(Message clientMessage) {
        GetValueRequest request = deserializePayload(clientMessage.payload(), GetValueRequest.class);
        sendClientResponse(
            clientMessage,
            new GetValueResponse(kvStore.get(request.key())),
            RaftMessageTypes.CLIENT_GET_RESPONSE
        );
    }

    private boolean rejectClientExecuteRequestIfNotLeader(Message clientMessage) {
        if (state == RaftState.LEADER) {
            return false;
        }

        System.out.println(id + ": Not leader, rejecting client request");
        sendClientResponse(
            clientMessage,
            new ExecuteCommandResponse(false, null),
            RaftMessageTypes.CLIENT_EXECUTE_RESPONSE
        );
        return true;
    }

    private int appendLogEntry(Operation operation, String clientKey) {
        int newIndex = log.size();
        log.add(new LogEntry(newIndex, currentTerm, operation, clientKey));
        System.out.println(id + ": Appended entry at index " + newIndex + " (term " + currentTerm + ")");
        return newIndex;
    }

    private String clientKeyFor(Message clientMessage) {
        return clientMessage.correlationId();
    }

    private void registerClientExecuteCallback(Message clientMessage, String clientKey, int logIndex) {
        waitingList.add(String.valueOf(logIndex), new RequestCallback<>() {
            @Override
            public void onResponse(Object response, ProcessId fromNode) {
                sendClientResponse(clientMessage, response, RaftMessageTypes.CLIENT_EXECUTE_RESPONSE);
                System.out.println(id + ": Sent response to client for " + clientKey);
            }

            @Override
            public void onError(Exception error) {
                sendClientResponse(
                    clientMessage,
                    new ExecuteCommandResponse(false, null),
                    RaftMessageTypes.CLIENT_EXECUTE_RESPONSE
                );
            }
        });
    }

    private void sendClientResponse(Message clientMessage, Object response, MessageType responseType) {
        send(createMessage(clientMessage.source(), clientMessage.correlationId(), response, responseType));
    }

    // ========== REQUEST VOTE RPC ==========

    private void handleRequestVoteRequest(Message message) {
        RequestVoteRequest request = deserializePayload(message.payload(), RequestVoteRequest.class);

        System.out.println(id + ": Received RequestVote from " + request.candidateId() +
            " for term " + request.term());

        updateTermIfNeeded(request.term());

        boolean voteGranted = canGrantVoteTo(request);
        if (voteGranted) {
            grantVoteTo(request.candidateId(), request.lastLogIndex(), request.lastLogTerm());
        } else {
            denyVoteTo(request.candidateId(), request.term(), isLogUpToDate(request.lastLogIndex(), request.lastLogTerm()));
        }

        sendRequestVoteResponse(message, voteGranted);
    }

    private void updateTermIfNeeded(int term) {
        if (term > currentTerm) {
            becomeFollower(term);
        }
    }

    private boolean canGrantVoteTo(RequestVoteRequest request) {
        boolean termOk = request.term() >= currentTerm;
        boolean notVoted = votedFor == null || votedFor.equals(request.candidateId());
        boolean logUpToDate = isLogUpToDate(request.lastLogIndex(), request.lastLogTerm());
        return termOk && notVoted && logUpToDate;
    }

    private void grantVoteTo(ProcessId candidateId, int lastLogIndex, int lastLogTerm) {
        votedFor = candidateId;
        resetElectionTimeout();
        System.out.println(id + ": Granted vote to " + candidateId +
            " (log up-to-date: lastIndex=" + lastLogIndex + ", lastTerm=" + lastLogTerm + ")");
    }

    private void denyVoteTo(ProcessId candidateId, int candidateTerm, boolean logUpToDate) {
        boolean termOk = candidateTerm >= currentTerm;
        boolean notVoted = votedFor == null || votedFor.equals(candidateId);
        System.out.println(id + ": Denied vote to " + candidateId +
            " (termOk=" + termOk + ", notVoted=" + notVoted + ", logUpToDate=" + logUpToDate + ")");
    }

    private void sendRequestVoteResponse(Message requestMessage, boolean voteGranted) {
        send(createMessage(
            requestMessage.source(),
            requestMessage.correlationId(),
            new RequestVoteResponse(currentTerm, voteGranted),
            RaftMessageTypes.REQUEST_VOTE_RESPONSE
        ));
    }

    /**
     * ✅ RAFT ELECTION RESTRICTION (§5.4.1)
     *
     * Candidate's log is "up-to-date" if:
     * 1. Last log term is HIGHER than ours, OR
     * 2. Last log term is SAME and log length is >= ours
     */
    private boolean isLogUpToDate(int candidateLastIndex, int candidateLastTerm) {
        int myLastIndex = getLastLogIndex();
        int myLastTerm = getLastLogTerm();
        return candidateLastTerm > myLastTerm ||
            (candidateLastTerm == myLastTerm && candidateLastIndex >= myLastIndex);
    }

    private void handleRequestVoteResponse(Message message) {
        RequestVoteResponse response = deserializePayload(message.payload(), RequestVoteResponse.class);
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }

    // ========== APPEND ENTRIES RPC ==========

    private void handleAppendEntriesRequest(Message message) {
        AppendEntriesRequest request = deserializePayload(message.payload(), AppendEntriesRequest.class);

        updateTermIfNeeded(request.term());

        if (rejectAppendEntriesIfStale(message, request.term())) {
            return;
        }

        followLeader(request.leaderId(), request.term());

        if (rejectAppendEntriesIfLogInconsistent(message, request.prevLogIndex(), request.prevLogTerm())) {
            return;
        }

        appendEntries(request.prevLogIndex(), request.entries());
        advanceCommitIndex(request.leaderCommit());
        sendAppendEntriesResponse(message, true, getLastLogIndex());
    }

    private boolean rejectAppendEntriesIfStale(Message message, int requestTerm) {
        if (requestTerm >= currentTerm) {
            return false;
        }

        sendAppendEntriesResponse(message, false, 0);
        return true;
    }

    private void followLeader(ProcessId leaderId, int leaderTerm) {
        resetElectionTimeout();
        currentLeader = leaderId;

        if (state != RaftState.FOLLOWER) {
            becomeFollower(leaderTerm);
            currentLeader = leaderId;
        }
    }

    private boolean rejectAppendEntriesIfLogInconsistent(Message message, int prevLogIndex, int prevLogTerm) {
        if (prevLogIndex == 0) {
            return false;
        }

        if (prevLogIndex >= log.size()) {
            System.out.println(id + ": Rejecting AppendEntries - prevLogIndex " + prevLogIndex + " beyond log end");
            sendAppendEntriesResponse(message, false, 0);
            return true;
        }

        LogEntry prevEntry = log.get(prevLogIndex);
        if (prevEntry.term() == prevLogTerm) {
            return false;
        }

        System.out.println(id + ": Rejecting AppendEntries - prevLogTerm mismatch at index " +
            prevLogIndex + " (expected " + prevLogTerm + ", got " + prevEntry.term() + ")");
        sendAppendEntriesResponse(message, false, 0);
        return true;
    }

    private void appendEntries(int prevLogIndex, List<LogEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }

        int insertIndex = prevLogIndex + 1;
        for (int i = 0; i < entries.size(); i++) {
            LogEntry newEntry = entries.get(i);
            int entryIndex = insertIndex + i;

            if (entryIndex < log.size()) {
                LogEntry existingEntry = log.get(entryIndex);
                if (existingEntry.term() != newEntry.term()) {
                    truncateLogFrom(entryIndex);
                    log.add(newEntry);
                }
            } else {
                log.add(newEntry);
            }
        }

        System.out.println(id + ": Appended " + entries.size() + " entries starting at index " + insertIndex);
    }

    private void truncateLogFrom(int entryIndex) {
        System.out.println(id + ": Conflict at index " + entryIndex + " - truncating log from here");
        while (log.size() > entryIndex) {
            log.remove(log.size() - 1);
        }
    }

    private void advanceCommitIndex(int leaderCommit) {
        if (leaderCommit <= commitIndex) {
            return;
        }

        commitIndex = Math.min(leaderCommit, getLastLogIndex());
        System.out.println(id + ": Updated commitIndex to " + commitIndex);
        applyCommittedEntries();
    }

    private void sendAppendEntriesResponse(Message requestMessage, boolean success, int matchIndex) {
        send(createMessage(
            requestMessage.source(),
            requestMessage.correlationId(),
            new AppendEntriesResponse(currentTerm, success, matchIndex),
            RaftMessageTypes.APPEND_ENTRIES_RESPONSE
        ));
    }

    private void handleAppendEntriesResponse(Message message) {
        if (state != RaftState.LEADER) {
            return;
        }

        AppendEntriesResponse response = deserializePayload(message.payload(), AppendEntriesResponse.class);
        ProcessId follower = message.source();

        if (response.term() > currentTerm) {
            becomeFollower(response.term());
            return;
        }

        if (response.success()) {
            recordSuccessfulReplication(follower, response.matchIndex());
            updateCommitIndex();
            return;
        }

        retryAppendEntriesWithEarlierIndex(follower);
    }

    private void recordSuccessfulReplication(ProcessId follower, int matchIndex) {
        this.matchIndex.put(follower, matchIndex);
        nextIndex.put(follower, matchIndex + 1);
        System.out.println(id + ": " + follower + " replicated up to index " + matchIndex);
    }

    private void retryAppendEntriesWithEarlierIndex(ProcessId follower) {
        int next = nextIndex.getOrDefault(follower, log.size());
        nextIndex.put(follower, Math.max(1, next - 1));
        System.out.println(id + ": " + follower + " log inconsistent, retrying with nextIndex=" +
            nextIndex.get(follower));
        sendAppendEntries(follower);
    }

    // ========== LEADER ELECTION ==========

    private void startElectionWhenTimedOut() {
        if (!electionTimeout.fired()) {
            return;
        }

        System.out.println(id + ": Election timeout after " +
            (electionTimeout.getDurationTicks() - electionTimeout.getRemainingTicks()) + " ticks! Starting election");
        startElection();
    }

    private void startElection() {
        becomeCandidate();
        votedFor = id;

        System.out.println(id + ": Starting election for term " + currentTerm);

        var quorumCallback = requestVoteQuorumCallback();
        broadcastRequestVote(quorumCallback);
    }

    private AsyncQuorumCallback<RequestVoteResponse> requestVoteQuorumCallback() {
        var quorumCallback = new AsyncQuorumCallback<RequestVoteResponse>(
            getAllNodes().size(),
            response -> response != null && response.voteGranted()
        );

        quorumCallback.onSuccess(responses -> {
            if (state == RaftState.CANDIDATE) {
                becomeLeader();
            }
        });

        quorumCallback.onFailure(error ->
            System.out.println(id + ": Election failed: " + error.getMessage())
        );

        return quorumCallback;
    }

    private void broadcastRequestVote(AsyncQuorumCallback<RequestVoteResponse> quorumCallback) {
        RequestVoteRequest request = new RequestVoteRequest(
            currentTerm, //gneration number
            id,
            getLastLogIndex(),
            getLastLogTerm()
        );

        broadcastToAllReplicas(quorumCallback, (node, correlationId) ->
            createMessage(node, correlationId, request, RaftMessageTypes.REQUEST_VOTE_REQUEST)
        );
    }

    // ========== STATE TRANSITIONS ==========

    private void becomeFollower(int newTerm) {
        System.out.println(id + ": Becoming FOLLOWER for term " + newTerm);
        state = RaftState.FOLLOWER;
        currentTerm = newTerm;
        votedFor = null;
        currentLeader = null;
        heartbeatInterval.stop();
        resetElectionTimeout();
    }

    private void becomeCandidate() {
        System.out.println(id + ": Becoming CANDIDATE");
        state = RaftState.CANDIDATE;
        currentTerm++;
        currentLeader = null;
        heartbeatInterval.stop();
        resetElectionTimeout();
    }

    private void becomeLeader() {
        System.out.println(id + ": Became LEADER for term " + currentTerm);
        state = RaftState.LEADER;
        currentLeader = id;

        for (ProcessId peer : getAllNodes()) {
            nextIndex.put(peer, log.size());
            matchIndex.put(peer, 0);
        }

        electionTimeout.stop();
        startHeartbeatInterval();
        sendHeartbeats();
    }

    // ========== HEARTBEATS AND REPLICATION ==========

    private void sendHeartbeatsWhenDue() {
        if (!heartbeatInterval.fired()) {
            return;
        }

        sendHeartbeats();
    }

    private void sendHeartbeats() {
        if (state != RaftState.LEADER) {
            return;
        }

        replicateToFollowers();
        heartbeatInterval.reset();
    }

    private void sendAppendEntries(ProcessId peer) {
        int next = nextIndex.getOrDefault(peer, log.size());
        int prevIndex = next - 1;
        int prevTerm = log.get(prevIndex).term();

        List<LogEntry> entries = new ArrayList<>();
        for (int i = next; i < log.size(); i++) {
            entries.add(log.get(i));
        }

        AppendEntriesRequest request = new AppendEntriesRequest(
            currentTerm,
            id,
            prevIndex,
            prevTerm,
            entries,
            commitIndex
        );

        send(createMessage(peer, UUID.randomUUID().toString(), request, RaftMessageTypes.APPEND_ENTRIES_REQUEST));
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
     */
    private void updateCommitIndex() {
        if (state != RaftState.LEADER) {
            return;
        }

        for (int index = commitIndex + 1; index < log.size(); index++) {
            LogEntry entry = log.get(index);

            if (entry.term() != currentTerm) {
                continue;
            }

            if (replicationCountFor(index) >= getMajority()) {
                commitIndex = index;
                System.out.println(id + ": Committed index " + index + " (term " + entry.term() +
                    ") - replicated on " + replicationCountFor(index) + " servers");
                applyCommittedEntries();
            }
        }
    }

    private int replicationCountFor(int index) {
        int replicationCount = 1;
        for (ProcessId peer : getAllNodes()) {
            if (!peer.equals(id) && matchIndex.getOrDefault(peer, 0) >= index) {
                replicationCount++;
            }
        }
        return replicationCount;
    }

    private void applyCommittedEntries() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry entry = log.get(lastApplied);

            System.out.println(id + ": Applying entry " + lastApplied + ": " + entry.operation());

            String result = executeOperation(entry.operation());
            waitingList.handleResponse(String.valueOf(lastApplied),
                new ExecuteCommandResponse(true, result), id);
        }
    }

    private String executeOperation(Operation operation) {
        if (operation instanceof SetValueOperation setOperation) {
            kvStore.put(setOperation.key(), setOperation.value());
            return setOperation.value();
        }
        return null;
    }

    // ========== HELPERS ==========

    private int getLastLogIndex() {
        return log.size() - 1;
    }

    private int getLastLogTerm() {
        return log.get(log.size() - 1).term();
    }

    private int getMajority() {
        return (getAllNodes().size() / 2) + 1;
    }

    private void resetElectionTimeout() {
        electionTimeout.start();
    }

    private void startHeartbeatInterval() {
        heartbeatInterval.start();
        heartbeatInterval.reset();
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
