package com.distribpatterns.multipaxosheartbeats;

import com.distribpatterns.multipaxosheartbeats.messages.*;
import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.Replica;
import com.tickloom.messaging.AsyncQuorumCallback;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageType;
import com.tickloom.messaging.RequestCallback;
import com.tickloom.util.Timeout;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Multi-Paxos with Heartbeats: adds automatic leader election and failure detection.
 *
 * New features vs MultiPaxosServer:
 * 1. Followers start elections automatically after a heartbeat timeout.
 * 2. Leaders send heartbeats on every tick to maintain leadership.
 * 3. Followers step down when they see a higher generation.
 * 4. A newly elected leader completes any uncommitted log entries.
 */
public class MultiPaxosWithHeartbeatsServer extends Replica {
    private static final NoOpOperation NO_OP = new NoOpOperation();
    private static final long BASE_ELECTION_TIMEOUT_TICKS = 100;
    private static final long ELECTION_TIMEOUT_STEP_TICKS = 25;

    private ServerRole role = ServerRole.LookingForLeader;
    private int promisedGeneration = 0;

    private final Map<Integer, PaxosState> paxosLog = new HashMap<>();
    private final Map<String, String> kvStore = new HashMap<>();

    private int highWaterMark = -1;

    private final AtomicInteger generationCounter = new AtomicInteger(0);
    private final AtomicInteger logIndex = new AtomicInteger(0);

    private Timeout leaderHeartbeatTimeout;

    public MultiPaxosWithHeartbeatsServer(List<ProcessId> peerIds, ProcessParams processParams) {
        super(peerIds, processParams);
        configureElectionTimeout();
        becomeFollower(promisedGeneration);
    }

    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
            MultiPaxosMessageTypes.CLIENT_EXECUTE_REQUEST, this::handleClientExecuteRequest,
            MultiPaxosMessageTypes.CLIENT_GET_REQUEST, this::handleClientGetRequest,
            MultiPaxosMessageTypes.FULL_LOG_PREPARE_REQUEST, this::handleFullLogPrepareRequest,
            MultiPaxosMessageTypes.FULL_LOG_PREPARE_RESPONSE, this::handleFullLogPrepareResponse,
            MultiPaxosMessageTypes.PROPOSE_REQUEST, this::handleProposeRequest,
            MultiPaxosMessageTypes.PROPOSE_RESPONSE, this::handleProposeResponse,
            MultiPaxosMessageTypes.COMMIT_REQUEST, this::handleCommitRequest,
            MultiPaxosMessageTypes.COMMIT_RESPONSE, this::handleCommitResponse,
            MultiPaxosMessageTypes.HEARTBEAT_REQUEST, this::handleHeartbeatRequest,
            MultiPaxosMessageTypes.HEARTBEAT_RESPONSE, this::handleHeartbeatResponse
        );
    }

    // ========== CLIENT REQUEST HANDLING ==========

    private void handleClientExecuteRequest(Message clientMessage) {
        ExecuteCommandRequest request = deserializePayload(clientMessage.payload(), ExecuteCommandRequest.class);

        if (rejectExecuteRequestIfNotLeader(clientMessage)) {
            return;
        }

        String clientKey = clientKeyFor(clientMessage);

        System.out.println(id + ": Leader received execute request for operation: " + request.operation());
        registerExecuteResponseCallback(clientMessage, clientKey);
        appendToLog(request.operation(), clientKey);
    }

    private void handleClientGetRequest(Message clientMessage) {
        GetValueRequest request = deserializePayload(clientMessage.payload(), GetValueRequest.class);

        if (rejectGetRequestIfNotLeader(clientMessage)) {
            return;
        }

        String clientKey = clientKeyFor(clientMessage);

        System.out.println(id + ": Leader received get request for key: " + request.key());
        registerGetResponseCallback(clientMessage, request.key(), clientKey);
        appendToLog(NO_OP, clientKey);
    }

    private boolean rejectExecuteRequestIfNotLeader(Message clientMessage) {
        if (role == ServerRole.Leader) {
            return false;
        }

        System.out.println(id + ": Rejecting client request - not leader (role=" + role + ")");
        sendClientResponse(
            clientMessage,
            new ExecuteCommandResponse(false, "Not leader"),
            MultiPaxosMessageTypes.CLIENT_EXECUTE_RESPONSE
        );
        return true;
    }

    private boolean rejectGetRequestIfNotLeader(Message clientMessage) {
        if (role == ServerRole.Leader) {
            return false;
        }

        System.out.println(id + ": Rejecting client get request - not leader (role=" + role + ")");
        sendClientResponse(
            clientMessage,
            new GetValueResponse(null),
            MultiPaxosMessageTypes.CLIENT_GET_RESPONSE
        );
        return true;
    }

    private String clientKeyFor(Message clientMessage) {
        return "client_" + clientMessage.correlationId();
    }

    private void registerExecuteResponseCallback(Message clientMessage, String clientKey) {
        waitingList.add(clientKey, new RequestCallback<>() {
            @Override
            public void onResponse(Object response, ProcessId fromNode) {
                sendClientResponse(clientMessage, response, MultiPaxosMessageTypes.CLIENT_EXECUTE_RESPONSE);
            }

            @Override
            public void onError(Exception error) {
                sendClientResponse(
                    clientMessage,
                    new ExecuteCommandResponse(false, null),
                    MultiPaxosMessageTypes.CLIENT_EXECUTE_RESPONSE
                );
            }
        });
    }

    private void registerGetResponseCallback(Message clientMessage, String key, String clientKey) {
        waitingList.add(clientKey, new RequestCallback<>() {
            @Override
            public void onResponse(Object response, ProcessId fromNode) {
                sendClientResponse(
                    clientMessage,
                    new GetValueResponse(kvStore.get(key)),
                    MultiPaxosMessageTypes.CLIENT_GET_RESPONSE
                );
            }

            @Override
            public void onError(Exception error) {
                sendClientResponse(
                    clientMessage,
                    new GetValueResponse(null),
                    MultiPaxosMessageTypes.CLIENT_GET_RESPONSE
                );
            }
        });
    }

    private void sendClientResponse(Message clientMessage, Object response, MessageType responseType) {
        send(createMessage(clientMessage.source(), clientMessage.correlationId(), response, responseType));
    }

    // ========== PROPOSER ROLE ==========
    // ========== LOG APPEND ==========

    private void appendToLog(Operation operation, String clientKey) {
        int index = logIndex.getAndIncrement();
        System.out.println(id + ": Leader appending " + operation + " at index " + index);
        startProposePhase(index, promisedGeneration, operation, clientKey);
    }

    // ========== PROPOSE / ACCEPT ==========

    private void startProposePhase(int logIndex, int generation, Operation operation, String clientKey) {
        System.out.println(id + ": Phase 1 - PROPOSE " + operation + " for index " + logIndex);

        var quorumCallback = proposeQuorumCallbackFor(logIndex, generation, operation, clientKey);
        broadcastProposeRequest(logIndex, generation, operation, quorumCallback);
    }

    private AsyncQuorumCallback<ProposeResponse> proposeQuorumCallbackFor(int logIndex,
                                                                          int generation,
                                                                          Operation operation,
                                                                          String clientKey) {
        var quorumCallback = new AsyncQuorumCallback<ProposeResponse>(
            getAllNodes().size(),
            response -> response != null && response.accepted()
        );

        quorumCallback
            .onSuccess(responses -> {
                System.out.println(id + ": PROPOSE quorum reached for index " + logIndex);
                startCommitPhase(logIndex, generation, operation, clientKey);
            })
            .onFailure(error -> failClientCommand(clientKey, "PROPOSE quorum failed for index " + logIndex));

        return quorumCallback;
    }

    private void failClientCommand(String clientKey, String reason) {
        System.out.println(id + ": " + reason);
        waitingList.handleResponse(clientKey, new ExecuteCommandResponse(false, null), id);
    }

    // ========== COMMIT / LEARN ==========

    private void startCommitPhase(int logIndex, int generation, Operation operation, String clientKey) {
        System.out.println(id + ": Phase 2 - COMMIT " + operation + " for index " + logIndex);

        registerClientForCommittedIndex(logIndex, clientKey);

        var quorumCallback = commitQuorumCallbackFor(logIndex);
        broadcastCommitRequest(logIndex, generation, operation, quorumCallback);
    }

    private AsyncQuorumCallback<CommitResponse> commitQuorumCallbackFor(int logIndex) {
        var quorumCallback = new AsyncQuorumCallback<CommitResponse>(
            getAllNodes().size(),
            response -> response != null && response.success()
        );

        quorumCallback
            .onSuccess(responses -> System.out.println(id + ": COMMIT quorum reached for index " + logIndex))
            .onFailure(error -> System.out.println(id + ": COMMIT quorum failed for index " + logIndex));

        return quorumCallback;
    }

    private void registerClientForCommittedIndex(int logIndex, String clientKey) {
        waitingList.add(indexClientKeyFor(logIndex), new RequestCallback<>() {
            @Override
            public void onResponse(Object response, ProcessId fromNode) {
                waitingList.handleResponse(clientKey, response, fromNode);
            }

            @Override
            public void onError(Exception error) {
                waitingList.handleResponse(clientKey, new ExecuteCommandResponse(false, null), id);
            }
        });
    }

    private String indexClientKeyFor(int logIndex) {
        return "index_" + logIndex + "_client";
    }

    private String recoveryClientKeyFor(int logIndex) {
        return "recovery_" + logIndex;
    }

    // ========== LEADER ELECTION ==========

    public void startElection() {
        System.out.println(id + ": Starting leader election");
        becomeCandidate();
        sendFullLogPrepare(nextGeneration());
    }

    private void becomeCandidate() {
        System.out.println(id + ": Becoming candidate");
        role = ServerRole.LookingForLeader;
        resetHeartbeatTimeout();
    }

    private void becomeLeader(int generation) {
        System.out.println(id + ": Becoming leader with generation " + generation);
        role = ServerRole.Leader;
        promisedGeneration = generation;
        generationCounter.updateAndGet(current -> Math.max(current, generation));
        leaderHeartbeatTimeout.stop();
    }

    private void becomeFollower(int generation) {
        System.out.println(id + ": Becoming follower for generation " + generation);
        role = ServerRole.Follower;
        promisedGeneration = generation;
        generationCounter.updateAndGet(current -> Math.max(current, generation));
        resetHeartbeatTimeout();
    }

    private int nextGeneration() {
        return generationCounter.updateAndGet(current -> Math.max(current, promisedGeneration) + 1);
    }

    private void sendFullLogPrepare(int generation) {
        System.out.println(id + ": Sending FULL_LOG_PREPARE with generation " + generation);

        var quorumCallback = fullLogPrepareQuorumCallbackFor(generation);
        broadcastFullLogPrepareRequest(generation, quorumCallback);
    }

    private AsyncQuorumCallback<FullLogPrepareResponse> fullLogPrepareQuorumCallbackFor(int generation) {
        var quorumCallback = new AsyncQuorumCallback<FullLogPrepareResponse>(
            getAllNodes().size(),
            response -> response != null && response.promised()
        );

        quorumCallback
            .onSuccess(responses -> becomeLeaderForGeneration(generation, responses))
            .onFailure(error -> failLeaderElection(error));

        return quorumCallback;
    }

    private void becomeLeaderForGeneration(int generation, Map<ProcessId, FullLogPrepareResponse> responses) {
        System.out.println(id + ": Election succeeded, became LEADER with generation " + generation);
        responses.values().forEach(this::mergeLog);
        becomeLeader(generation);
        completeUncommittedEntries(generation);
    }

    private void failLeaderElection(Throwable error) {
        System.out.println(id + ": Election failed: " + error.getMessage() + ", becoming follower");
        becomeFollower(promisedGeneration);
    }

    // ========== ACCEPTOR / LEARNER ROLE ==========
    // ========== PROPOSE / ACCEPT ==========

    private void handleProposeRequest(Message message) {
        ProposeRequest request = deserializePayload(message.payload(), ProposeRequest.class);
        int logIndex = request.logIndex();
        int generation = request.generation();
        Operation value = request.value();

        if (canAcceptProposal(generation)) {
            acceptProposal(message, logIndex, generation, value);
            return;
        }

        rejectProposal(message, logIndex, generation);
    }

    private boolean canAcceptProposal(int generation) {
        return generation >= promisedGeneration;
    }

    private void acceptProposal(Message message, int logIndex, int generation, Operation value) {
        promisedGeneration = generation;
        generationCounter.updateAndGet(current -> Math.max(current, generation));
        var paxosState = getOrCreatePaxosState(logIndex);
        var newState = paxosState.accept(generation, value);
        paxosLog.put(logIndex, newState);
        System.out.println(id + ": ACCEPTED proposal for index " + logIndex);
        sendProposeResponse(message, logIndex, true);
    }

    private void rejectProposal(Message message, int logIndex, int generation) {
        System.out.println(id + ": REJECTED proposal for index " + logIndex +
            " (gen=" + generation + " < promised=" + promisedGeneration + ")");
        sendProposeResponse(message, logIndex, false);
    }

    private void sendProposeResponse(Message requestMessage, int logIndex, boolean accepted) {
        send(createMessage(
            requestMessage.source(),
            requestMessage.correlationId(),
            new ProposeResponse(logIndex, accepted),
            MultiPaxosMessageTypes.PROPOSE_RESPONSE
        ));
    }

    private void handleProposeResponse(Message message) {
        ProposeResponse response = deserializePayload(message.payload(), ProposeResponse.class);
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }

    // ========== COMMIT / LEARN ==========

    private void handleCommitRequest(Message message) {
        CommitRequest request = deserializePayload(message.payload(), CommitRequest.class);
        int logIndex = request.logIndex();
        int generation = request.generation();
        Operation value = request.value();

        System.out.println(id + ": Received COMMIT for index " + logIndex);
        commitValue(message, logIndex, generation, value);
    }

    private void commitValue(Message message, int logIndex, int generation, Operation value) {
        var paxosState = getOrCreatePaxosState(logIndex);
        var newState = paxosState.commit(generation, value);
        paxosLog.put(logIndex, newState);

        System.out.println(id + ": COMMITTED index " + logIndex);
        tryExecuteLogEntries();
        sendCommitResponse(message, true);
    }

    private void sendCommitResponse(Message requestMessage, boolean success) {
        send(createMessage(
            requestMessage.source(),
            requestMessage.correlationId(),
            new CommitResponse(success),
            MultiPaxosMessageTypes.COMMIT_RESPONSE
        ));
    }

    private void handleCommitResponse(Message message) {
        CommitResponse response = deserializePayload(message.payload(), CommitResponse.class);
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }

    // ========== FULL LOG PREPARE ==========

    private void handleFullLogPrepareRequest(Message message) {
        FullLogPrepareRequest request = deserializePayload(message.payload(), FullLogPrepareRequest.class);
        int generation = request.generation();

        System.out.println(id + ": Received FULL_LOG_PREPARE with generation " + generation);

        if (generation > promisedGeneration) {
            promiseLeadershipGeneration(message, generation);
            return;
        }

        rejectLeadershipGeneration(message, generation);
    }

    private void promiseLeadershipGeneration(Message message, int generation) {
        Map<Integer, PaxosState> uncommitted = getUncommittedEntries();
        System.out.println(id + ": PROMISED generation " + generation + ", becoming FOLLOWER, sending " +
            uncommitted.size() + " uncommitted entries");

        becomeFollower(generation);
        sendFullLogPrepareResponse(message, FullLogPrepareResponse.accepted(uncommitted));
    }

    private void rejectLeadershipGeneration(Message message, int generation) {
        System.out.println(id + ": REJECTED generation " + generation + " (have " + promisedGeneration + ")");
        sendFullLogPrepareResponse(message, FullLogPrepareResponse.rejected());
    }

    private void sendFullLogPrepareResponse(Message requestMessage, FullLogPrepareResponse response) {
        send(createMessage(
            requestMessage.source(),
            requestMessage.correlationId(),
            response,
            MultiPaxosMessageTypes.FULL_LOG_PREPARE_RESPONSE
        ));
    }

    private void handleFullLogPrepareResponse(Message message) {
        FullLogPrepareResponse response = deserializePayload(message.payload(), FullLogPrepareResponse.class);
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }

    private void mergeLog(FullLogPrepareResponse response) {
        for (Map.Entry<Integer, PaxosState> entry : response.uncommittedEntries().entrySet()) {
            int index = entry.getKey();
            PaxosState peerState = entry.getValue();
            PaxosState selfState = paxosLog.get(index);

            if (selfState == null ||
                (peerState.acceptedGeneration().isPresent() &&
                    (selfState.acceptedGeneration().isEmpty() ||
                        peerState.acceptedGeneration().get() > selfState.acceptedGeneration().get()))) {
                paxosLog.put(index, peerState);
                System.out.println(id + ": Merged log entry " + index + " from peer");
            }
        }
    }

    private void completeUncommittedEntries(int generation) {
        Map<Integer, PaxosState> uncommitted = getUncommittedEntries();

        if (uncommitted.isEmpty()) {
            System.out.println(id + ": No uncommitted entries to complete");
            return;
        }

        System.out.println(id + ": Completing " + uncommitted.size() + " uncommitted entries");

        for (Map.Entry<Integer, PaxosState> entry : uncommitted.entrySet()) {
            int index = entry.getKey();
            PaxosState state = entry.getValue();

            if (state.acceptedValue().isPresent()) {
                Operation value = state.acceptedValue().get();
                System.out.println(id + ": Completing uncommitted entry at index " + index);
                startProposePhase(index, generation, value, recoveryClientKeyFor(index));
            }
        }
    }

    private Map<Integer, PaxosState> getUncommittedEntries() {
        return paxosLog.entrySet().stream()
            .filter(entry -> entry.getValue().committedValue().isEmpty())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    // ========== HEARTBEAT HANDLING ==========

    public void checkLeader() {
        if (!heartbeatTimedOut()) {
            return;
        }

        System.out.println(id + ": Heartbeat timeout after " + ticksSinceLastHeartbeat() + " ticks (role=" + role + ")");
        if (role == ServerRole.Follower || role == ServerRole.LookingForLeader) {
            System.out.println(id + ": Triggering election due to heartbeat timeout");
            startElection();
        }
    }

    @Override
    public void onTick() {
        leaderHeartbeatTimeout.tick();

        if (role == ServerRole.Leader) {
            sendHeartbeats();
            return;
        }

        checkLeader();
    }

    public void sendHeartbeats() {
        if (role != ServerRole.Leader) {
            return;
        }

        System.out.println(id + ": Sending heartbeats (generation=" + promisedGeneration + ")");
        broadcastHeartbeats();
    }

    private void handleHeartbeatRequest(Message message) {
        HeartbeatRequest request = deserializePayload(message.payload(), HeartbeatRequest.class);

        if (request.generation() < promisedGeneration) {
            rejectHeartbeat(message, request.generation());
            return;
        }

        followLeaderHeartbeat(request.generation());
        acknowledgeHeartbeat(message);
    }

    private void followLeaderHeartbeat(int generation) {
        if (generation > promisedGeneration || role != ServerRole.Leader) {
            becomeFollower(generation);
            return;
        }

        resetHeartbeatTimeout();
    }

    private void acknowledgeHeartbeat(Message message) {
        sendHeartbeatResponse(message, true);
    }

    private void rejectHeartbeat(Message message, int generation) {
        System.out.println(id + ": Heartbeat from stale generation " + generation +
            " (current=" + promisedGeneration + "), rejecting");
        sendHeartbeatResponse(message, false);
    }

    private void handleHeartbeatResponse(Message message) {
        HeartbeatResponse response = deserializePayload(message.payload(), HeartbeatResponse.class);

        if (!response.success()) {
            System.out.println(id + ": Heartbeat rejected by " + message.source() +
                " with higher generation " + response.currentGeneration() +
                " (current=" + promisedGeneration + "), stepping down");
            becomeFollower(response.currentGeneration());
        }
    }

    private boolean heartbeatTimedOut() {
        return leaderHeartbeatTimeout.fired();
    }

    private long ticksSinceLastHeartbeat() {
        return leaderHeartbeatTimeout.getDurationTicks() - leaderHeartbeatTimeout.getRemainingTicks();
    }

    private void resetHeartbeatTimeout() {
        leaderHeartbeatTimeout.start();
    }

    private void configureElectionTimeout() {
        // Use a deterministic per-node stagger instead of randomness so followers
        // do not all start elections on the same tick in the simulator.
        long electionTimeoutTicks = BASE_ELECTION_TIMEOUT_TICKS + (stableNodeOrder() * ELECTION_TIMEOUT_STEP_TICKS);
        leaderHeartbeatTimeout = new Timeout(id + "-heartbeat-timeout", electionTimeoutTicks);
        System.out.println(id + ": Election timeout = " + electionTimeoutTicks + " ticks");
    }

    private int stableNodeOrder() {
        return getAllNodes().stream()
            .sorted(java.util.Comparator.comparing(ProcessId::toString))
            .toList()
            .indexOf(id);
    }

    private void broadcastProposeRequest(int logIndex,
                                         int generation,
                                         Operation operation,
                                         AsyncQuorumCallback<ProposeResponse> quorumCallback) {
        ProposeRequest proposeReq = new ProposeRequest(logIndex, generation, operation);
        broadcastToAllReplicas(quorumCallback, (node, correlationId) ->
            createMessage(node, correlationId, proposeReq, MultiPaxosMessageTypes.PROPOSE_REQUEST)
        );
    }

    private void broadcastCommitRequest(int logIndex,
                                        int generation,
                                        Operation operation,
                                        AsyncQuorumCallback<CommitResponse> quorumCallback) {
        CommitRequest commitReq = new CommitRequest(logIndex, generation, operation);
        broadcastToAllReplicas(quorumCallback, (node, correlationId) ->
            createMessage(node, correlationId, commitReq, MultiPaxosMessageTypes.COMMIT_REQUEST)
        );
    }

    private void broadcastFullLogPrepareRequest(int generation,
                                                AsyncQuorumCallback<FullLogPrepareResponse> quorumCallback) {
        FullLogPrepareRequest prepareReq = new FullLogPrepareRequest(generation);
        broadcastToAllReplicas(quorumCallback, (node, correlationId) ->
            createMessage(node, correlationId, prepareReq, MultiPaxosMessageTypes.FULL_LOG_PREPARE_REQUEST)
        );
    }

    private void broadcastHeartbeats() {
        HeartbeatRequest request = new HeartbeatRequest(promisedGeneration);
        for (ProcessId peer : getAllNodes()) {
            if (peer.equals(id)) {
                continue;
            }

            send(createMessage(
                peer,
                java.util.UUID.randomUUID().toString(),
                request,
                MultiPaxosMessageTypes.HEARTBEAT_REQUEST
            ));
        }
    }

    private void sendHeartbeatResponse(Message requestMessage, boolean success) {
        send(createMessage(
            requestMessage.source(),
            requestMessage.correlationId(),
            new HeartbeatResponse(success, promisedGeneration),
            MultiPaxosMessageTypes.HEARTBEAT_RESPONSE
        ));
    }

    // ========== LOG EXECUTION ==========

    private void tryExecuteLogEntries() {
        for (int index = highWaterMark + 1; ; index++) {
            PaxosState state = paxosLog.get(index);
            if (state == null || state.committedValue().isEmpty()) {
                break;
            }

            executeLogEntry(index, state.committedValue().get());
            highWaterMark = index;
        }
    }

    private void executeLogEntry(int logIndex, Operation operation) {
        System.out.println(id + ": Executing log entry " + logIndex + ": " + operation);

        String result = null;
        boolean success = true;

        if (operation instanceof SetValueOperation set) {
            kvStore.put(set.key(), set.value());
            result = set.value();
            System.out.println(id + ": Executed SET " + set.key() + "=" + set.value());
        } else if (operation instanceof CompareAndSwapOperation cas) {
            String existingValue = kvStore.get(cas.key());
            boolean matches = Objects.equals(existingValue, cas.expectedValue());

            if (matches) {
                kvStore.put(cas.key(), cas.newValue());
                success = true;
            } else {
                success = false;
            }
            result = existingValue;

            System.out.println(id + ": Executed CAS " + cas.key() +
                " (expected=" + cas.expectedValue() + ", actual=" + existingValue +
                ", success=" + success + ")");
        } else if (operation instanceof NoOpOperation) {
            System.out.println(id + ": Executed NO-OP");
        }

        waitingList.handleResponse(indexClientKeyFor(logIndex), new ExecuteCommandResponse(success, result), id);
    }

    // ========== UTILITY METHODS ==========

    private PaxosState getOrCreatePaxosState(int logIndex) {
        return paxosLog.computeIfAbsent(logIndex, ignored -> new PaxosState());
    }

    // ========== PUBLIC ACCESSORS FOR TESTING ==========

    public ServerRole getRole() {
        return role;
    }

    public boolean isLeader() {
        return role == ServerRole.Leader;
    }

    public Map<Integer, PaxosState> getPaxosLog() {
        return paxosLog;
    }

    public String getValue(String key) {
        return kvStore.get(key);
    }

    public int getHighWaterMark() {
        return highWaterMark;
    }

    public boolean isFollower() {
        return role == ServerRole.Follower;
    }

    public int getPromisedGeneration() {
        return promisedGeneration;
    }
}
