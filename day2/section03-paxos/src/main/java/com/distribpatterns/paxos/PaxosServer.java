package com.distribpatterns.paxos;

import com.tickloom.ProcessId;
import com.tickloom.Replica;
import com.tickloom.messaging.*;
import com.tickloom.network.MessageCodec;
import com.tickloom.network.PeerType;
import com.tickloom.storage.Storage;
import com.tickloom.util.Clock;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Single Value Paxos implementation following the established patterns
 * from TwoPhaseServer, ThreePhaseServer, and GenerationVotingServer.
 * 
 * Paxos Protocol:
 * 1. PREPARE phase: Proposer gets promises from quorum
 * 2. PROPOSE phase: Proposer proposes value (highest accepted or client's)
 * 3. COMMIT phase: Coordinator commits the agreed value
 */
public class PaxosServer extends Replica {
    // State machine
    private final Map<String, Integer> counters = new HashMap<>();
    private final Map<String, String> kvStore = new HashMap<>();
    
    // Paxos state (immutable, recreated on each update)
    private PaxosState state = new PaxosState();
    
    // For generation number
    private final AtomicInteger generationCounter = new AtomicInteger(0);
    
    // Track the current client request (for coordinator to respond after commit)
    private String currentClientKey;
    
    // Retry mechanism
    private static final int MAX_RETRIES = 3;
    
    public PaxosServer(ProcessId id, List<ProcessId> allNodes, MessageBus messageBus,
                       MessageCodec messageCodec, Storage storage, Clock clock, int requestTimeoutTicks) {
        super(id, allNodes, messageBus, messageCodec, storage, clock, requestTimeoutTicks);
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
        
        // Start Paxos protocol with retry
        startPaxosWithRetry(clientKey, request.operation(), 0);
    }
    
    private void startPaxosWithRetry(String clientKey, Operation operation, int attempt) {
        if (attempt >= MAX_RETRIES) {
            System.out.println(id + ": Max retries reached, failing request");
            waitingList.handleResponse(clientKey, new ExecuteResponse(false, "Max retries exceeded"), id);
            return;
        }
        
        // Generate new generation number (higher than any seen)
        int generation = generationCounter.incrementAndGet();
        
        System.out.println(id + ": Starting Paxos attempt " + (attempt + 1) + " with generation " + generation);
        
        // Phase 1: Prepare
        startPreparePhase(clientKey, generation, operation, attempt);
    }
    
    // ========== PHASE 1: PREPARE/PROMISE ==========
    
    private void startPreparePhase(String clientKey, int generation, Operation operation, int attempt) {
        // Store client key so we can respond after commit
        this.currentClientKey = clientKey;
        
        System.out.println(id + ": Phase 1 - PREPARE with generation " + generation);
        
        var quorumCallback = new AsyncQuorumCallback<PrepareResponse>(
            getAllNodes().size(),
            response -> response != null && response.promised()
        );
        
        quorumCallback
            .onSuccess(responses -> {
                System.out.println(id + ": PREPARE quorum reached (" + responses.size() + " promises), moving to Phase 2");
                startProposePhase(clientKey, generation, operation, responses, attempt);
            })
            .onFailure(error -> {
                System.out.println(id + ": PREPARE quorum failed: " + error.getMessage() + ", retrying...");
                // Retry immediately (could add delay if needed)
                startPaxosWithRetry(clientKey, operation, attempt + 1);
            });
        
        PrepareRequest prepareReq = new PrepareRequest(generation);
        broadcastToAllReplicas(quorumCallback, (node, correlationId) ->
            createMessage(node, correlationId, prepareReq, PaxosMessageTypes.PREPARE_REQUEST)
        );
        System.out.println(id + ": Broadcast PREPARE to " + getAllNodes().size() + " nodes");
    }
    
    private void handlePrepareRequest(Message message) {
        PrepareRequest request = deserializePayload(message.payload(), PrepareRequest.class);
        int generation = request.generation();
        
        System.out.println(id + ": Received PREPARE for generation " + generation + " (current promise: " + state.promisedGeneration() + ")");
        
        if (state.canPromise(generation)) {
            // Promise this generation
            state = state.promise(generation);
            
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
    
    private void handlePrepareResponse(Message message) {
        PrepareResponse response = deserializePayload(message.payload(), PrepareResponse.class);
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }
    
    // ========== PHASE 2: PROPOSE/ACCEPT ==========
    
    private void startProposePhase(String clientKey, int generation, Operation clientOperation,
                                   Map<ProcessId, PrepareResponse> promises, int attempt) {
        // Paxos key insight: Choose the value with the highest accepted generation,
        // or use client's value if none were accepted
        Operation valueToPropose = selectValueToPropose(clientOperation, promises);
        
        System.out.println(id + ": Phase 2 - PROPOSE value " + valueToPropose + " with generation " + generation);
        
        var quorumCallback = new AsyncQuorumCallback<ProposeResponse>(
            getAllNodes().size(),
            response -> response != null && response.accepted()
        );
        
        quorumCallback
            .onSuccess(responses -> {
                System.out.println(id + ": PROPOSE quorum reached (" + responses.size() + " accepts), moving to Phase 3");
                startCommitPhase(clientKey, generation, valueToPropose);
            })
            .onFailure(error -> {
                System.out.println(id + ": PROPOSE quorum failed: " + error.getMessage() + ", retrying...");
                // Retry immediately (could add delay if needed)
                startPaxosWithRetry(clientKey, clientOperation, attempt + 1);
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
            .max((p1, p2) -> Integer.compare(p1.acceptedGeneration(), p2.acceptedGeneration()));
        
        if (highestAccepted.isPresent() && highestAccepted.get().acceptedValue() != null) {
            Operation recoveredValue = highestAccepted.get().acceptedValue();
            System.out.println(id + ": Found previously accepted value, using it: " + recoveredValue);
            return recoveredValue;
        } else {
            System.out.println(id + ": No previously accepted value, using client's: " + clientOperation);
            return clientOperation;
        }
    }
    
    private void handleProposeRequest(Message message) {
        ProposeRequest request = deserializePayload(message.payload(), ProposeRequest.class);
        int generation = request.generation();
        Operation value = request.value();
        
        System.out.println(id + ": Received PROPOSE for generation " + generation + ", value " + value);
        
        if (state.canAccept(generation)) {
            // Accept this value
            state = state.accept(generation, value);
            
            System.out.println(id + ": ACCEPTED value " + value + " for generation " + generation);
            send(createMessage(message.source(), message.correlationId(), 
                new ProposeResponse(true), PaxosMessageTypes.PROPOSE_RESPONSE));
        } else {
            System.out.println(id + ": REJECTED proposal for generation " + generation + 
                " (promised " + state.promisedGeneration() + ")");
            send(createMessage(message.source(), message.correlationId(), 
                new ProposeResponse(false), PaxosMessageTypes.PROPOSE_RESPONSE));
        }
    }
    
    private void handleProposeResponse(Message message) {
        ProposeResponse response = deserializePayload(message.payload(), ProposeResponse.class);
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }
    
    // ========== PHASE 3: COMMIT/LEARN ==========
    
    private void startCommitPhase(String clientKey, int generation, Operation value) {
        System.out.println(id + ": Phase 3 - COMMIT value " + value);
        
        var quorumCallback = new AsyncQuorumCallback<CommitResponse>(
            getAllNodes().size(),
            response -> response != null && response.committed()
        );
        
        quorumCallback
            .onSuccess(responses -> {
                System.out.println(id + ": COMMIT quorum reached (" + responses.size() + " commits)");
                // Note: We don't execute here. The coordinator will execute when it receives
                // its own COMMIT message (like all other nodes), ensuring exactly-once execution.
                // For now, we'll respond to client after we execute our own commit
            })
            .onFailure(error -> {
                System.out.println(id + ": COMMIT quorum failed (unexpected): " + error.getMessage());
                waitingList.handleResponse(clientKey, new ExecuteResponse(false, null), id);
            });
        
        CommitRequest commitReq = new CommitRequest(generation, value);
        broadcastToAllReplicas(quorumCallback, (node, correlationId) ->
            createMessage(node, correlationId, commitReq, PaxosMessageTypes.COMMIT_REQUEST)
        );
        System.out.println(id + ": Broadcast COMMIT to " + getAllNodes().size() + " nodes");
    }
    
    private void handleCommitRequest(Message message) {
        CommitRequest request = deserializePayload(message.payload(), CommitRequest.class);
        int generation = request.generation();
        Operation value = request.value();
        
        System.out.println(id + ": Received COMMIT for generation " + generation + ", value " + value);
        
        if (state.canAccept(generation)) {
            // Commit and execute the value
            state = state.commit(generation, value);
            String result = executeOperation(value);
            
            System.out.println(id + ": COMMITTED and executed value " + value + ", result: " + result);
            
            // If we're the coordinator (have a pending client request), respond to client
            if (currentClientKey != null) {
                System.out.println(id + ": Coordinator responding to client with result: " + result);
                waitingList.handleResponse(currentClientKey, new ExecuteResponse(true, result), id);
                currentClientKey = null; // Clear for next request
            }
            
            send(createMessage(message.source(), message.correlationId(), 
                new CommitResponse(true), PaxosMessageTypes.COMMIT_RESPONSE));
        } else {
            System.out.println(id + ": REJECTED commit for generation " + generation);
            send(createMessage(message.source(), message.correlationId(), 
                new CommitResponse(false), PaxosMessageTypes.COMMIT_RESPONSE));
        }
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

