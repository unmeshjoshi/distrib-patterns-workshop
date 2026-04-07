package com.distribpatterns.paxos;

import com.distribpatterns.paxos.messages.*;
import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.Replica;
import com.tickloom.future.ListenableFuture;
import com.tickloom.messaging.AsyncQuorumCallback;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageType;
import com.tickloom.messaging.RequestCallback;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Single Value Paxos implementation following the established structure
 * from TwoPhaseServer, ThreePhaseServer, and GenerationVotingServer.
 * <p>
 * Paxos Protocol:
 * 1. PREPARE phase: Proposer gets promises from quorum
 * 2. PROPOSE phase: Proposer proposes value (highest accepted or client's)
 * 3. COMMIT phase: Coordinator commits the agreed value
 */

public class PaxosServer extends Replica {
    private static final String PAXOS_STATE_KEY = "paxos_state";
    
    // State machine
    private final Map<String, Integer> counters = new HashMap<>();
    private final Map<String, String> kvStore = new HashMap<>();
    
    // Paxos state (immutable, recreated on each update)
    private PaxosState state = new PaxosState();
    
    // For generation number
    private final AtomicInteger generationCounter = new AtomicInteger(0);
    
    // Retry mechanism
    private static final int MAX_RETRIES = 5;

    public PaxosServer(List<ProcessId> allNodes, ProcessParams processParams) {
        super(allNodes, processParams);
        // Initialization is handled by Process.initialise()
    }

    @Override
    protected ListenableFuture<?> onInit() {
        // Load initial state using standardized method
        return load(PAXOS_STATE_KEY, PaxosState.class).handle((loadedState, error) -> {
            if (error != null) {
                System.err.println(id + ": Failed to load PaxosState: " + error.getMessage());
                return;
            }

            if (loadedState != null) {
                state = loadedState;
            } else {
                state = new PaxosState();
            }
        });
    }

    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
                PaxosMessageTypes.CLIENT_EXECUTE_REQUEST, this::handleClientExecuteRequest,
                PaxosMessageTypes.PREPARE_REQUEST, this::handlePrepareRequest,
                PaxosMessageTypes.PREPARE_RESPONSE, this::handlePrepareResponse,
                PaxosMessageTypes.PROPOSE_REQUEST, this::handleProposeRequest,
                PaxosMessageTypes.PROPOSE_RESPONSE, this::handleProposeResponse,
                PaxosMessageTypes.COMMIT_REQUEST, this::handleCommitRequest,
                PaxosMessageTypes.COMMIT_RESPONSE, this::handleCommitResponse
        );
    }

    // ========== PROPOSER ROLE ==========

    private void handleClientExecuteRequest(Message clientMessage) {
        ExecuteRequest request = deserializePayload(clientMessage.payload(), ExecuteRequest.class);
        String clientKey = clientKeyFor(clientMessage);

        System.out.println(id + ": Received client request for operation: " + request.operation());

        registerClientCompletionCallback(clientMessage, clientKey);

        //clientMessage.. we can carry the original client request operation through to the commit request,
        //so that we respond to client with success only if the client request is the one we executed and committed
        //Otherwise we send a failure response back with the final operation that is executed.
        // Start Paxos protocol with retry

        startPaxosWithRetry(new ClientMessage(clientKey, request.operation()), 0);
    }

    //Starts paxos protocol
    private void startPaxosWithRetry(ClientMessage clientMessage, int attempt) {
        if (attemptsExhausted(attempt)) {
            System.out.println(id + ": Max retries reached, waiting for another proposer to complete the value");
            return;
        }

        // Generate new generation number (higher than any seen)
        int generation = generationCounter.incrementAndGet();
        // Phase 1: Prepare

        System.out.println(id + ": Starting Paxos attempt " + (attempt + 1) + " with generation " + generation);
        startPreparePhase(generation, clientMessage, attempt);
    }
    private void startPreparePhase(int generation, ClientMessage clientMessage, int attempt) {
        System.out.println(id + ": Phase 1 - PREPARE with generation " + generation);

        var quorumCallback = this.<PrepareResponse>quorumCallbackBuilder()
                .succeedsWhen(response -> isPromiseFor(response, generation))
                .onSuccess(responses -> {
                    System.out.println(id + ": PREPARE quorum reached (" + responses.size() + " promises), moving to Phase 2");
                    startProposePhase(generation, clientMessage, responses, attempt);
                })
                .onFailure(error -> {
                    System.out.println(id + ": PREPARE quorum failed: " + error.getMessage() + ", retrying...");
                    startPaxosWithRetry(clientMessage, attempt + 1);
                })
                .build();

        broadcastPrepareFor(generation, quorumCallback);
    }
    private void startProposePhase(long generation, ClientMessage clientMessage,
                                   Map<ProcessId, PrepareResponse> promises, int attempt) {
        // Paxos key insight: Choose the value with the highest accepted generation,
        // or use client's value if none were accepted
        Operation valueToPropose = selectValueToPropose(clientMessage.operation(), promises);

        System.out.println(id + ": Phase 2 - PROPOSE value " + valueToPropose + " with generation " + generation);

        var quorumCallback = this.<ProposeResponse>quorumCallbackBuilder()
                .succeedsWhen(response -> isAcceptedFor(response, generation))
                .onSuccess(responses -> {
                    System.out.println(id + ": PROPOSE quorum reached (" + responses.size() + " accepts), moving to Phase 3");
                    startCommitPhase(generation, clientMessage, valueToPropose);
                })
                .onFailure(error -> {
                    System.out.println(id + ": PROPOSE quorum failed: " + error.getMessage() + ", retrying...");
                    // Retry immediately (could add delay if needed)
                    startPaxosWithRetry(clientMessage, attempt + 1);
                })
                .build();

        ProposeRequest proposeReq = new ProposeRequest(generation, valueToPropose);
        broadcastToAllReplicas(quorumCallback, (node, correlationId) ->
                createMessage(node, correlationId, proposeReq, PaxosMessageTypes.PROPOSE_REQUEST)
        );
        System.out.println(id + ": Broadcast PROPOSE to " + getAllNodes().size() + " nodes");
    }

    private void startCommitPhase(long generation, ClientMessage clientMessage, Operation committedOperation) {
        System.out.println(id + ": Phase 3 - COMMIT value " + committedOperation);

        var quorumCallback = commitQuorumCallback();
        broadcastCommitFor(generation, clientMessage, committedOperation, quorumCallback);
    }

    private String clientKeyFor(Message clientMessage) {
        return "client_" + clientMessage.correlationId();
    }

    private void registerClientCompletionCallback(Message clientMessage, String clientKey) {
        waitingList.add(clientKey, new RequestCallback<Object>() {
            @Override
            public void onResponse(Object response, ProcessId fromNode) {
                var responseMsg = createMessage(clientMessage.source(), clientMessage.correlationId(),
                        response, PaxosMessageTypes.CLIENT_EXECUTE_RESPONSE);
                System.out.println(id + ": Sending response to client: " + response);
                send(responseMsg);
            }

            @Override
            public void onError(Exception error) {
                System.out.println(id + ": Error processing client request: " + error.getMessage());
                send(createMessage(clientMessage.source(), clientMessage.correlationId(),
                        new ExecuteResponse(false, null), PaxosMessageTypes.CLIENT_EXECUTE_RESPONSE));
            }
        });
    }

    private static boolean attemptsExhausted(int attempt) {
        return attempt >= MAX_RETRIES;
    }

    private void broadcastPrepareFor(int generation, AsyncQuorumCallback<PrepareResponse> quorumCallback) {
        PrepareRequest prepareReq = new PrepareRequest(generation);
        broadcastToAllReplicas(quorumCallback, (node, correlationId) ->
                createMessage(node, correlationId, prepareReq, PaxosMessageTypes.PREPARE_REQUEST)
        );
        System.out.println(id + ": Broadcast PREPARE to " + getAllNodes().size() + " nodes");
    }

    private static boolean isPromiseFor(PrepareResponse response, long generation) {
        return response != null
                && response.promised()
                && response.promisedGeneration() == generation;
    }

    private void handlePrepareResponse(Message message) {
        PrepareResponse response = deserializePayload(message.payload(), PrepareResponse.class);
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }

    // ========== PHASE 2: PROPOSE/ACCEPT ==========
    private Operation selectValueToPropose(Operation clientOperation,
                                           Map<ProcessId, PrepareResponse> promises) {
        // Find the promise with the highest accepted generation
        Optional<PrepareResponse> highestAccepted = promises.values().stream()
                .filter(p -> p.acceptedGeneration() != null)
                .max(Comparator.comparingLong(PrepareResponse::acceptedGeneration));

        if (highestAccepted.isPresent() && highestAccepted.get().acceptedValue() != null) {
            Operation recoveredValue = highestAccepted.get().acceptedValue();
            System.out.println(id + ": Found previously accepted value, using it: " + recoveredValue);
            return recoveredValue;
        } else {
            System.out.println(id + ": No previously accepted value, using client's: " + clientOperation);
            return clientOperation;
        }
    }

    private static boolean isAcceptedFor(ProposeResponse response, long generation) {
        return response != null
                && response.accepted()
                && response.generation() == generation;
    }

    private void handleProposeResponse(Message message) {
        ProposeResponse response = deserializePayload(message.payload(), ProposeResponse.class);
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }

    // ========== PHASE 3: COMMIT/LEARN ==========

    private AsyncQuorumCallback<CommitResponse> commitQuorumCallback() {
        // What happens when commit requests fail? Any next client request handled will trigger prepare
        // phase which recovers the nodes missing the committed operations. This 'reconciling' lagging replicas
        // is a problem. In Paxos it's reconciled either by a new client request or the node itself can trigger
        // the prepare phase and full paxos execution to get the latest state.
        return this.<CommitResponse>quorumCallbackBuilder()
                .succeedsWhen(response -> response != null && response.committed())
                .onSuccess(acks -> System.out.println(id + ": COMMIT quorum reached (" + acks.size() + ")"))
                .onFailure(err -> System.out.println(id + ": COMMIT quorum failed: " + err.getMessage()))
                .build();
    }

    private void broadcastCommitFor(long generation,
                                    ClientMessage clientMessage,
                                    Operation committedOperation,
                                    AsyncQuorumCallback<CommitResponse> quorumCallback) {
        // We send commit requests to all replicas. When this replica,
        // which had been coordinating the client request, handles the commit request,
        // it will execute the committed operation and respond to the client.
        CommitRequest commitReq = new CommitRequest(generation, clientMessage, committedOperation);
        broadcastToAllReplicas(quorumCallback, (node, correlationId) ->
                createMessage(node, correlationId, commitReq, PaxosMessageTypes.COMMIT_REQUEST)
        );
        System.out.println(id + ": Broadcast COMMIT to " + getAllNodes().size() + " nodes");
    }

    private void handleCommitResponse(Message message) {
        CommitResponse response = deserializePayload(message.payload(), CommitResponse.class);
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }

    // ========== ACCEPTOR / LEARNER ROLE ==========

    private void handlePrepareRequest(Message message) {
        PrepareRequest request = deserializePayload(message.payload(), PrepareRequest.class);
        int generation = request.generation();

        System.out.println(id + ": Received PREPARE for generation " + generation + " (current promise: " + state.promisedGeneration() + ")");

        if (state.canPromise(generation)) {
            promiseGeneration(message, generation);
            return;
        }

        rejectPrepareRequest(message, generation);
    }

    private void promiseGeneration(Message message, int generation) {
        state = state.promise(generation);
        persist(PAXOS_STATE_KEY, state, () -> {
            PrepareResponse response = promiseResponse();
            System.out.println(id + ": PROMISED generation " + generation
                    + acceptedValueDescription());
            sendPrepareResponse(message, response);
        });
    }

    private void rejectPrepareRequest(Message message, int generation) {
        System.out.println(id + ": REJECTED generation " + generation
                + ", already promised " + state.promisedGeneration());
        sendPrepareResponse(message, rejectedPrepareResponse());
    }

    private PrepareResponse promiseResponse() {
        return new PrepareResponse(
                true,
                state.promisedGeneration(),
                state.acceptedGeneration().orElse(null),
                state.acceptedValue().orElse(null)
        );
    }

    private PrepareResponse rejectedPrepareResponse() {
        return new PrepareResponse(
                false,
                state.promisedGeneration(),
                state.acceptedGeneration().orElse(null),
                state.acceptedValue().orElse(null)
        );
    }

    private String acceptedValueDescription() {
        return state.acceptedValue().isPresent() ? " (have accepted value)" : " (no accepted value)";
    }

    private void sendPrepareResponse(Message message, PrepareResponse response) {
        send(createMessage(message.source(), message.correlationId(), response,
                PaxosMessageTypes.PREPARE_RESPONSE));
    }

    private void handleProposeRequest(Message message) {
        ProposeRequest request = deserializePayload(message.payload(), ProposeRequest.class);
        long generation = request.generation();
        Operation value = request.value();

        System.out.println(id + ": Received PROPOSE for generation " + generation + ", value " + value);

        if (state.canAccept(generation)) {
            state = state.accept(generation, value);
            persist(PAXOS_STATE_KEY, state, () -> {
                System.out.println(id + ": ACCEPTED value " + value + " for generation " + generation);
                send(createMessage(message.source(), message.correlationId(),
                        new ProposeResponse(true, generation), PaxosMessageTypes.PROPOSE_RESPONSE));
            });
            return;
        }

        System.out.println(id + ": REJECTED proposal for generation " + generation +
                " (promised " + state.promisedGeneration() + ")");
        send(createMessage(message.source(), message.correlationId(),
                new ProposeResponse(false, generation), PaxosMessageTypes.PROPOSE_RESPONSE));
    }

    private void handleCommitRequest(Message message) {
        CommitRequest request = deserializePayload(message.payload(), CommitRequest.class);
        Operation committedOperation = request.committedOperation();

        System.out.println(id + ": Received COMMIT for generation " + request.generation()
                + ", committedOperation " + committedOperation);

        commitAndExecute(request);
    }

    private void commitAndExecute(CommitRequest request) {
        Operation committedOperation = request.committedOperation();

        // Learning a chosen value must not lower the promise already made.
        // So commit records the chosen generation and value, while preserving the higher promise.
        state = state.commit(request.generation(), committedOperation);
        persist(PAXOS_STATE_KEY, state, () -> {
            String result = executeCommittedOperation(committedOperation);
            respondToClientWithCommittedResult(request, committedOperation, result);
        });
    }

    private String executeCommittedOperation(Operation committedOperation) {
        String result = executeOperation(committedOperation);
        System.out.println(id + ": COMMITTED and executed committedOperation "
                + committedOperation + ", result: " + result);
        return result;
    }

    private void respondToClientWithCommittedResult(CommitRequest request,
                                                    Operation committedOperation,
                                                    String result) {
        boolean clientRequestCommitted = committedClientOperation(request, committedOperation);
        if (!clientRequestCommitted) {
            logCommittedRecoveredOperation(request, committedOperation);
        }

        System.out.println(id + ": Coordinator responding to client with result: " + result);
        waitingList.handleResponse(request.clientKey(), new ExecuteResponse(clientRequestCommitted, result), id);
    }

    private static boolean committedClientOperation(CommitRequest request, Operation committedOperation) {
        return committedOperation.equals(request.clientMessage().operation());
    }

    private void logCommittedRecoveredOperation(CommitRequest request, Operation committedOperation) {
        System.out.println(id + ": Client operation was " + request.clientMessage().operation()
                + " but Paxos committed recovered operation " + committedOperation);
    }

    // ========== SHARED HELPERS ==========

    private <T> QuorumCallbackBuilder<T> quorumCallbackBuilder() {
        return QuorumCallbackBuilder.forTotalResponses(getAllNodes().size());
    }

    // ========== STATE MACHINE EXECUTION ==========

    private String executeOperation(Operation operation) {
        if (operation instanceof IncrementCounterOperation inc) {
            int newValue = counters.getOrDefault(inc.key(), 0) + 1;
            counters.put(inc.key(), newValue);
            System.out.println(id + ": Executed INCREMENT, counter=" + newValue);
            return String.valueOf(newValue);
        } else if (operation instanceof SetValueOperation set) {
            kvStore.put(set.key(), set.value());
            System.out.println(id + ": Executed SET, key=" + set.key() + ", value=" + set.value());
            return set.value();
        }
        throw new IllegalArgumentException("Unknown operation: " + operation);
    }

    // ========== PUBLIC ACCESSORS FOR TESTING ==========

    public PaxosState getState() {
        return state;
    }

    public Integer getCounter(String key) {
        return counters.get(key);
    }

    public String getValue(String key) {
        return kvStore.get(key);
    }
}
