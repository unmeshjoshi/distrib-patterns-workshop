package com.distribpatterns.paxos;

import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.Replica;
import com.tickloom.future.ListenableFuture;
import com.tickloom.messaging.*;
import com.tickloom.storage.Storage;
import com.tickloom.storage.VersionedValue;

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
    private static final byte[] PAXOS_STATE = "paxos_state".getBytes();
    // State machine
    private final Map<String, Integer> counters = new HashMap<>();
    private final Map<String, String> kvStore = new HashMap<>();
    private boolean isInitialised = false;


    // Paxos state (immutable, recreated on each update)
    private PaxosState state = new PaxosState();

    // For generation number
    private final AtomicInteger generationCounter = new AtomicInteger(0);

    // Retry mechanism
    private static final int MAX_RETRIES = 3;

    public PaxosServer(List<ProcessId> allNodes, Storage storage, ProcessParams processParams) {
        super(allNodes, storage, processParams);
        storage.get(PAXOS_STATE).handle((value, error)->{
            if (error != null) {
                System.out.println("Error reading paxos state = " + error.getMessage());
                return;
            }

            if (value != null) { //we have persisted some state,
                // so need to initialise by reading it.
                state = messageCodec.decode(value.value(), PaxosState.class);
            }
            isInitialised = true;
        });
    }


    public boolean isInitialised() {
        return isInitialised;
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

    static record ClientMessage(String clientKey, Operation operation) {}
    // ========== CLIENT REQUEST HANDLING ==========

    private void handleClientExecuteRequest(Message clientMessage) {
        ExecuteRequest request = deserializePayload(clientMessage.payload(), ExecuteRequest.class);
        String clientKey = "client_" + clientMessage.correlationId();

        System.out.println(id + ": Received client request for operation: " + request.operation());

        // Store client callback
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

        //clientMessage.. we can carry the original client request operation through to the commit request,
        //so that we respond to client with success only if the client request is the one we executed and committed
        //Otherwise we send a failure response back with the final operation that is executed.
        // Start Paxos protocol with retry

        startPaxosWithRetry(new ClientMessage(clientKey, request.operation()), 0);
    }

    private void startPaxosWithRetry(ClientMessage clientMessage, int attempt) {
        if (attemptsExhausted(attempt)) {
            System.out.println(id + ": Max retries reached, failing request");
            waitingList.handleResponse(clientMessage.clientKey(), new ExecuteResponse(false, "Max retries exceeded"), id);
            return;
        }

        // Generate new generation number (higher than any seen)
        int generation = generationCounter.incrementAndGet();

        System.out.println(id + ": Starting Paxos attempt " + (attempt + 1) + " with generation " + generation);

        // Phase 1: Prepare
        startPreparePhase(generation, clientMessage, attempt);
    }

    private static boolean attemptsExhausted(int attempt) {
        return attempt >= MAX_RETRIES;
    }

    // ========== PHASE 1: PREPARE/PROMISE ==========

    private void startPreparePhase(int generation, ClientMessage clientMessage, int attempt) {
        System.out.println(id + ": Phase 1 - PREPARE with generation " + generation);

        var quorumCallback = new AsyncQuorumCallback<PrepareResponse>(
                getAllNodes().size(),
                response -> isPromised(response)
        );

        quorumCallback
                .onSuccess(responses -> {
                    System.out.println(id + ": PREPARE quorum reached (" + responses.size() + " promises), moving to Phase 2");
                    startProposePhase(generation, clientMessage, responses, attempt);
                })
                .onFailure(error -> {
                    System.out.println(id + ": PREPARE quorum failed: " + error.getMessage() + ", retrying...");
                    // Retry immediately (could add delay if needed)
                    startPaxosWithRetry(clientMessage, attempt + 1);
                });

        PrepareRequest prepareReq = new PrepareRequest(generation);
        broadcastToAllReplicas(quorumCallback, (node, correlationId) ->
                createMessage(node, correlationId, prepareReq, PaxosMessageTypes.PREPARE_REQUEST)
        );
        System.out.println(id + ": Broadcast PREPARE to " + getAllNodes().size() + " nodes");
    }

    private static boolean isPromised(PrepareResponse response) {
        return response != null && response.promised();
    }
    /**
     * Generic method to handle the common pattern of:
     * 1. Update state
     * 2. Persist state
     * 3. Execute success callback on successful persistence
     * 4. Handle errors silently (client will timeout)
     */
    private void updateStateAndPersist(PaxosState newState, Runnable onSuccess) {
        state = newState;
        ListenableFuture<Boolean> storageFuture = persistPaxosState(state);
        storageFuture.handle((result, error) -> {
            if (error != null) {
                return; // Don't send any response if there is storage error.
                // The client will timeout, and may try with other server.
            }
            onSuccess.run();
        });
    }

    private void handlePrepareRequest(Message message) {
        PrepareRequest request = deserializePayload(message.payload(), PrepareRequest.class);
        int generation = request.generation();

        System.out.println(id + ": Received PREPARE for generation " + generation + " (current promise: " + state.promisedGeneration() + ")");

        if (state.canPromise(generation)) {
            // Promise this generation
            updateStateAndPersist(state.promise(generation), () -> {
                // Send back our previously accepted value (if any)
                PrepareResponse response = new PrepareResponse(
                        true,
                        state.acceptedGeneration().orElse(null),
                        state.acceptedValue().orElse(null)
                );

                System.out.println(id + ": PROMISED generation " + generation +
                        (state.acceptedValue().isPresent() ? " (have accepted value)" : " (no accepted value)"));
                send(createMessage(message.source(), message.correlationId(), response,
                        PaxosMessageTypes.PREPARE_RESPONSE));
            });

        } else {
            // Reject - we've promised a higher generation
            PrepareResponse response = new PrepareResponse(
                    false,
                    state.acceptedGeneration().orElse(null),
                    state.acceptedValue().orElse(null)
            );

            System.out.println(id + ": REJECTED generation " + generation + ", already promised " + state.promisedGeneration());
            send(createMessage(message.source(), message.correlationId(), response,
                    PaxosMessageTypes.PREPARE_RESPONSE));
        }
    }

    private void handleProposeRequest(Message message) {
        ProposeRequest request = deserializePayload(message.payload(), ProposeRequest.class);
        long generation = request.generation();
        Operation value = request.value();

        System.out.println(id + ": Received PROPOSE for generation " + generation + ", value " + value);

        if (state.canAccept(generation)) {
            // Accept this value
            updateStateAndPersist(state.accept(generation, value), () -> {
                System.out.println(id + ": ACCEPTED value " + value + " for generation " + generation);
                send(createMessage(message.source(), message.correlationId(),
                        new ProposeResponse(true), PaxosMessageTypes.PROPOSE_RESPONSE));
            });

        } else {
            System.out.println(id + ": REJECTED proposal for generation " + generation +
                    " (promised " + state.promisedGeneration() + ")");
            send(createMessage(message.source(), message.correlationId(),
                    new ProposeResponse(false), PaxosMessageTypes.PROPOSE_RESPONSE));
        }
    }

    private void handleCommitRequest(Message message) {
        CommitRequest request = deserializePayload(message.payload(), CommitRequest.class);
        long generation = request.generation();
        Operation committedOperation = request.committedOperation();

        System.out.println(id + ": Received COMMIT for generation " + generation + ", committedOperation " + committedOperation);

        // This check is actually redundant. If the request is committed, every future request is guaranteed
        // to be with
        if (state.canAccept(generation)) {
            // Commit and execute the committedOperation
            updateStateAndPersist(state.commit(generation, committedOperation), () -> {
                String result = executeOperation(committedOperation);
                System.out.println(id + ": COMMITTED and executed committedOperation " + committedOperation + ", result: " + result);
                boolean clientRequestCommitted = committedOperation.equals(request.clientMessage().operation());
                if (!clientRequestCommitted) {
                    System.out.println("Client operation was  = "
                            + request.clientMessage().operation()
                            + " but commited a pending operation "
                            + clientRequestCommitted);
                }
                // If we're the coordinator (have a pending client request), respond to client
                System.out.println(id + ": Coordinator responding to client with result: " + result);
                waitingList.handleResponse(request.clientKey(), new ExecuteResponse(clientRequestCommitted, result), id);
            });
        }
    }
    private ListenableFuture<Boolean> persistPaxosState(PaxosState state) {
        ListenableFuture<Boolean> setFuture = storage.set(PAXOS_STATE, new VersionedValue(messageCodec.encode(state), clock.now()));
        return setFuture;
    }

    private void handlePrepareResponse(Message message) {
        PrepareResponse response = deserializePayload(message.payload(), PrepareResponse.class);
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }

    // ========== PHASE 2: PROPOSE/ACCEPT ==========

    private void startProposePhase(long generation, ClientMessage clientMessage,
                                   Map<ProcessId, PrepareResponse> promises, int attempt) {
        // Paxos key insight: Choose the value with the highest accepted generation,
        // or use client's value if none were accepted
        Operation valueToPropose = selectValueToPropose(clientMessage.operation(), promises);

        System.out.println(id + ": Phase 2 - PROPOSE value " + valueToPropose + " with generation " + generation);

        var quorumCallback = new AsyncQuorumCallback<ProposeResponse>(
                getAllNodes().size(),
                response -> response != null && response.accepted()
        );

        quorumCallback
                .onSuccess(responses -> {
                    System.out.println(id + ": PROPOSE quorum reached (" + responses.size() + " accepts), moving to Phase 3");
                    startCommitPhase(generation, clientMessage, valueToPropose);
                })
                .onFailure(error -> {
                    System.out.println(id + ": PROPOSE quorum failed: " + error.getMessage() + ", retrying...");
                    // Retry immediately (could add delay if needed)
                    startPaxosWithRetry(clientMessage, attempt + 1);
                });

        ProposeRequest proposeReq = new ProposeRequest(generation, valueToPropose);
        broadcastToAllReplicas(quorumCallback, (node, correlationId) ->
                createMessage(node, correlationId, proposeReq, PaxosMessageTypes.PROPOSE_REQUEST)
        );
        System.out.println(id + ": Broadcast PROPOSE to " + getAllNodes().size() + " nodes");
    }

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


    private void handleProposeResponse(Message message) {
        ProposeResponse response = deserializePayload(message.payload(), ProposeResponse.class);
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }

    // ========== PHASE 3: COMMIT/LEARN ==========

    private void startCommitPhase(long generation, ClientMessage clientMessage, Operation committedOperation) {
        System.out.println(id + ": Phase 3 - COMMIT value " + committedOperation);

        var quorumCallback = new AsyncQuorumCallback<CommitResponse>(
                getAllNodes().size(),
                response -> response != null && response.committed()
        );

        //What happens when commit requests fail? Any next client request handled, will trigger prepare
        //phase which recovers the nodes missing the committed operations. This 'reconciling' lagging replicas
        //is a problem. In Paxos its reconciled either by a new client request or the node itself can trigger
        //the prepare phase and full paxos execution to get the latest state.

        quorumCallback
                .onSuccess(acks -> System.out.println(id + ": COMMIT quorum reached ("+acks.size()+")"))
                .onFailure(err -> System.out.println(id + ": COMMIT quorum failed: " + err.getMessage()));

        //We send commit requests to all replicas. When this replica,
        // which had been coordinating the client request, handles the commit request,
        //it will execute the committed operation and respond to the client.
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

