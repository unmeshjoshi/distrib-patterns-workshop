package com.distribpatterns.paxoslog;

import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.Replica;
import com.tickloom.messaging.*;
import com.tickloom.network.MessageCodec;
import com.tickloom.storage.Storage;
import com.tickloom.util.Clock;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * PaxosLog: Replicated log where each entry is decided via Paxos consensus.
 * 
 * Key features:
 * - Each log index runs independent Paxos instance
 * - Sequential execution: entries must be committed in order (no gaps)
 * - Automatic conflict resolution: retry at next index if value rejected
 * - State machine: applies commands (SetValue, CompareAndSwap) to KV store
 */
public class PaxosLogServer extends Replica {
    // Paxos log: one PaxosState per log index
    private final Map<Integer, PaxosState> paxosLog = new TreeMap<>();
    
    // State machine: key-value store
    private final Map<String, String> kvStore = new HashMap<>();
    
    // Track highest committed index (high water mark)
    private int highWaterMark = -1;
    
    // Generation counter for Paxos rounds
    private final AtomicInteger generationCounter = new AtomicInteger(0);
    
    // Server ID for breaking ties (if needed)
    private final int serverId;
    
    // No-op operation for reads
    private static final NoOpOperation NO_OP = new NoOpOperation();
    
    public PaxosLogServer(List<ProcessId> allNodes, Storage storage, ProcessParams processParams) {
        super(allNodes, storage, processParams);
        // Simple server ID: hash of process ID
        this.serverId = id.toString().hashCode();
    }
    
    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
            // Client handlers
            PaxosLogMessageTypes.CLIENT_EXECUTE_REQUEST, this::handleClientExecuteRequest,
            PaxosLogMessageTypes.CLIENT_GET_REQUEST, this::handleClientGetRequest,
            
            // Paxos Phase 1: Prepare/Promise
            PaxosLogMessageTypes.PREPARE_REQUEST, this::handlePrepareRequest,
            PaxosLogMessageTypes.PREPARE_RESPONSE, this::handlePrepareResponse,
            
            // Paxos Phase 2: Propose/Accept
            PaxosLogMessageTypes.PROPOSE_REQUEST, this::handleProposeRequest,
            PaxosLogMessageTypes.PROPOSE_RESPONSE, this::handleProposeResponse,
            
            // Paxos Phase 3: Commit/Learn
            PaxosLogMessageTypes.COMMIT_REQUEST, this::handleCommitRequest,
            PaxosLogMessageTypes.COMMIT_RESPONSE, this::handleCommitResponse
        );
    }
    
    // ========== CLIENT REQUEST HANDLING ==========
    
    private void handleClientExecuteRequest(Message clientMessage) {
        ExecuteCommandRequest request = deserializePayload(clientMessage.payload(), ExecuteCommandRequest.class);
        String clientKey = "client_" + clientMessage.correlationId();
        
        System.out.println(id + ": Received execute request for command: " + request.operation());
        
        // Store client callback
        waitingList.add(clientKey, new RequestCallback<Object>() {
            @Override
            public void onResponse(Object response, ProcessId fromNode) {
                var responseMsg = createMessage(clientMessage.source(), clientMessage.correlationId(),
                    response, PaxosLogMessageTypes.CLIENT_EXECUTE_RESPONSE);
                send(responseMsg);
            }
            
            @Override
            public void onError(Exception error) {
                send(createMessage(clientMessage.source(), clientMessage.correlationId(),
                    new ExecuteCommandResponse(false, null), PaxosLogMessageTypes.CLIENT_EXECUTE_RESPONSE));
            }
        });
        
        // Start appending to log
        appendToLog(0, request.operation(), clientKey);
    }
    
    private void handleClientGetRequest(Message clientMessage) {
        GetValueRequest request = deserializePayload(clientMessage.payload(), GetValueRequest.class);
        String clientKey = "client_" + clientMessage.correlationId();
        
        System.out.println(id + ": Received get request for key: " + request.key());
        
        // Store client callback
        waitingList.add(clientKey, new RequestCallback<Object>() {
            @Override
            public void onResponse(Object response, ProcessId fromNode) {
                // After no-op commits, read from KV store
                String value = kvStore.get(request.key());
                var responseMsg = createMessage(clientMessage.source(), clientMessage.correlationId(),
                    new GetValueResponse(value), PaxosLogMessageTypes.CLIENT_GET_RESPONSE);
                send(responseMsg);
            }
            
            @Override
            public void onError(Exception error) {
                send(createMessage(clientMessage.source(), clientMessage.correlationId(),
                    new GetValueResponse(null), PaxosLogMessageTypes.CLIENT_GET_RESPONSE));
            }
        });
        
        // Execute a no-op to ensure we see committed state
        appendToLog(0, NO_OP, clientKey);
    }
    
    // ========== LOG APPEND LOGIC ==========
    
    /**
     * Append operation to log at given index.
     * If consensus fails (value rejected), retry at next index.
     */
    private void appendToLog(int startIndex, Operation operation, String clientKey) {
        int generation = generationCounter.incrementAndGet();
        System.out.println(id + ": Attempting to append " + operation + " at index " + startIndex + 
                         " with generation " + generation);
        
        runPaxosForIndex(startIndex, generation, operation, clientKey, 0);
    }
    
    private void runPaxosForIndex(int logIndex, int generation, Operation operation, String clientKey, int attempt) {
        if (attempt >= 3) {
            System.out.println(id + ": Max attempts reached for " + operation);
            waitingList.handleResponse(clientKey, new ExecuteCommandResponse(false, null), id);
            return;
        }
        
        startPreparePhase(logIndex, generation, operation, clientKey, attempt);
    }
    
    // ========== PHASE 1: PREPARE/PROMISE ==========
    
    private void startPreparePhase(int logIndex, int generation, Operation operation, String clientKey, int attempt) {
        System.out.println(id + ": Phase 1 - PREPARE for index " + logIndex + ", generation " + generation);
        
        var quorumCallback = new AsyncQuorumCallback<PrepareResponse>(
            getAllNodes().size(),
            response -> response != null && response.promised()
        );
        
        quorumCallback
            .onSuccess(responses -> {
                System.out.println(id + ": PREPARE quorum reached for index " + logIndex);
                startProposePhase(logIndex, generation, operation, responses, clientKey, attempt);
            })
            .onFailure(error -> {
                System.out.println(id + ": PREPARE quorum failed for index " + logIndex + ": " + error.getMessage());
                // Retry with higher generation
                int newGeneration = generationCounter.incrementAndGet();
                runPaxosForIndex(logIndex, newGeneration, operation, clientKey, attempt + 1);
            });
        
        PrepareRequest prepareReq = new PrepareRequest(logIndex, generation);
        broadcastToAllReplicas(quorumCallback, (node, correlationId) ->
            createMessage(node, correlationId, prepareReq, PaxosLogMessageTypes.PREPARE_REQUEST)
        );
    }
    
    private void handlePrepareRequest(Message message) {
        PrepareRequest request = deserializePayload(message.payload(), PrepareRequest.class);
        int logIndex = request.logIndex();
        int generation = request.generation();
        
        var paxosState = getOrCreatePaxosState(logIndex);
        
        if (paxosState.canPromise(generation)) {
            // Promise this generation
            var newState = paxosState.promise(generation);
            paxosLog.put(logIndex, newState);
            
            PrepareResponse response = new PrepareResponse(
                true,
                newState.acceptedGeneration().orElse(null),
                newState.acceptedValue().orElse(null)
            );
            
            System.out.println(id + ": PROMISED generation " + generation + " for index " + logIndex);
            send(createMessage(message.source(), message.correlationId(), response,
                PaxosLogMessageTypes.PREPARE_RESPONSE));
        } else {
            // Reject
            PrepareResponse response = new PrepareResponse(
                false,
                paxosState.acceptedGeneration().orElse(null),
                paxosState.acceptedValue().orElse(null)
            );
            
            System.out.println(id + ": REJECTED generation " + generation + " for index " + logIndex);
            send(createMessage(message.source(), message.correlationId(), response,
                PaxosLogMessageTypes.PREPARE_RESPONSE));
        }
    }
    
    private void handlePrepareResponse(Message message) {
        PrepareResponse response = deserializePayload(message.payload(), PrepareResponse.class);
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }
    
    // ========== PHASE 2: PROPOSE/ACCEPT ==========
    
    private void startProposePhase(int logIndex, int generation, Operation initialOperation,
                                   Map<ProcessId, PrepareResponse> promises, String clientKey, int attempt) {
        // Select value to propose: highest accepted or initial command
        Operation valueToPropose = selectValueToPropose(initialOperation, promises);
        
        System.out.println(id + ": Phase 2 - PROPOSE " + valueToPropose + " for index " + logIndex);
        
        var quorumCallback = new AsyncQuorumCallback<ProposeResponse>(
            getAllNodes().size(),
            response -> response != null && response.accepted()
        );
        
        quorumCallback
            .onSuccess(responses -> {
                System.out.println(id + ": PROPOSE quorum reached for index " + logIndex);
                startCommitPhase(logIndex, generation, valueToPropose, initialOperation, clientKey);
            })
            .onFailure(error -> {
                System.out.println(id + ": PROPOSE quorum failed for index " + logIndex);
                // Retry with higher generation
                int newGeneration = generationCounter.incrementAndGet();
                runPaxosForIndex(logIndex, newGeneration, initialOperation, clientKey, attempt + 1);
            });
        
        ProposeRequest proposeReq = new ProposeRequest(logIndex, generation, valueToPropose);
        broadcastToAllReplicas(quorumCallback, (node, correlationId) ->
            createMessage(node, correlationId, proposeReq, PaxosLogMessageTypes.PROPOSE_REQUEST)
        );
    }
    
    private Operation selectValueToPropose(Operation initialOperation, Map<ProcessId, PrepareResponse> promises) {
        Optional<PrepareResponse> highestAccepted = promises.values().stream()
            .filter(p -> p.acceptedGeneration() != null)
            .max((p1, p2) -> Integer.compare(p1.acceptedGeneration(), p2.acceptedGeneration()));
        
        if (highestAccepted.isPresent() && highestAccepted.get().acceptedValue() != null) {
            Operation recoveredValue = highestAccepted.get().acceptedValue();
            System.out.println(id + ": Found previously accepted value: " + recoveredValue);
            return recoveredValue;
        } else {
            return initialOperation;
        }
    }
    
    private void handleProposeRequest(Message message) {
        ProposeRequest request = deserializePayload(message.payload(), ProposeRequest.class);
        int logIndex = request.logIndex();
        int generation = request.generation();
        Operation value = request.value();
        
        var paxosState = getOrCreatePaxosState(logIndex);
        
        if (paxosState.canAccept(generation)) {
            // Accept the proposal
            var newState = paxosState.accept(generation, value);
            paxosLog.put(logIndex, newState);
            
            System.out.println(id + ": ACCEPTED proposal for index " + logIndex);
            send(createMessage(message.source(), message.correlationId(), 
                new ProposeResponse(logIndex, true), PaxosLogMessageTypes.PROPOSE_RESPONSE));
        } else {
            System.out.println(id + ": REJECTED proposal for index " + logIndex);
            send(createMessage(message.source(), message.correlationId(), 
                new ProposeResponse(logIndex, false), PaxosLogMessageTypes.PROPOSE_RESPONSE));
        }
    }
    
    private void handleProposeResponse(Message message) {
        ProposeResponse response = deserializePayload(message.payload(), ProposeResponse.class);
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }
    
    // ========== PHASE 3: COMMIT/LEARN ==========
    
    private void startCommitPhase(int logIndex, int generation, Operation agreedValue, 
                                  Operation initialOperation, String clientKey) {
        System.out.println(id + ": Phase 3 - COMMIT " + agreedValue + " for index " + logIndex);
        
        var quorumCallback = new AsyncQuorumCallback<CommitResponse>(
            getAllNodes().size(),
            response -> response != null && response.success()
        );
        
        quorumCallback
            .onSuccess(responses -> {
                System.out.println(id + ": COMMIT quorum reached for index " + logIndex);
                
                // Check if the agreed value is different from what we proposed
                if (!agreedValue.equals(initialOperation)) {
                    System.out.println(id + ": Value mismatch at index " + logIndex + 
                                     ", retrying at next index");
                    // Retry at next index
                    appendToLog(logIndex + 1, initialOperation, clientKey);
                }
                // If values match, client will be notified when entry is executed
            })
            .onFailure(error -> {
                System.out.println(id + ": COMMIT quorum failed for index " + logIndex);
                waitingList.handleResponse(clientKey, new ExecuteCommandResponse(false, null), id);
            });
        
        CommitRequest commitReq = new CommitRequest(logIndex, generation, agreedValue);
        // Track which client request this is for
        String indexClientKey = "index_" + logIndex + "_client";
        waitingList.add(indexClientKey, new RequestCallback<Object>() {
            @Override
            public void onResponse(Object response, ProcessId fromNode) {
                // Will be called by executeLogEntry when this index is executed
                if (agreedValue.equals(initialOperation)) {
                    waitingList.handleResponse(clientKey, response, fromNode);
                }
            }
            
            @Override
            public void onError(Exception error) {
                if (agreedValue.equals(initialOperation)) {
                    waitingList.handleResponse(clientKey, new ExecuteCommandResponse(false, null), id);
                }
            }
        });
        
        broadcastToAllReplicas(quorumCallback, (node, correlationId) ->
            createMessage(node, correlationId, commitReq, PaxosLogMessageTypes.COMMIT_REQUEST)
        );
    }
    
    private void handleCommitRequest(Message message) {
        CommitRequest request = deserializePayload(message.payload(), CommitRequest.class);
        int logIndex = request.logIndex();
        int generation = request.generation();
        Operation value = request.value();
        
        System.out.println(id + ": Received COMMIT for index " + logIndex + ", value " + value);
        
        var paxosState = getOrCreatePaxosState(logIndex);
        
        if (paxosState.canAccept(generation)) {
            // Commit the value
            var newState = paxosState.commit(generation, value);
            paxosLog.put(logIndex, newState);
            
            System.out.println(id + ": COMMITTED index " + logIndex);
            
            // Try to execute this entry and any subsequent committed entries
            tryExecuteLogEntries();
            
            send(createMessage(message.source(), message.correlationId(),
                new CommitResponse(true), PaxosLogMessageTypes.COMMIT_RESPONSE));
        } else {
            System.out.println(id + ": REJECTED commit for index " + logIndex);
            send(createMessage(message.source(), message.correlationId(),
                new CommitResponse(false), PaxosLogMessageTypes.COMMIT_RESPONSE));
        }
    }
    
    private void handleCommitResponse(Message message) {
        CommitResponse response = deserializePayload(message.payload(), CommitResponse.class);
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }
    
    // ========== LOG EXECUTION (STATE MACHINE) ==========
    
    /**
     * Try to execute committed log entries in order (no gaps).
     */
    private void tryExecuteLogEntries() {
        // Execute all consecutive committed entries starting from highWaterMark + 1
        for (int index = highWaterMark + 1; ; index++) {
            PaxosState state = paxosLog.get(index);
            if (state == null || state.committedValue().isEmpty()) {
                // Gap found or no more entries
                break;
            }
            
            // Execute this entry
            Operation operation = state.committedValue().get();
            executeLogEntry(index, operation);
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
            // No-op: just ensure we see committed state
            System.out.println(id + ": Executed NO-OP");
        }
        
        // Notify client if they're waiting for this index
        String indexClientKey = "index_" + logIndex + "_client";
        waitingList.handleResponse(indexClientKey, new ExecuteCommandResponse(success, result), id);
    }
    
    // ========== UTILITY METHODS ==========
    
    private PaxosState getOrCreatePaxosState(int logIndex) {
        return paxosLog.computeIfAbsent(logIndex, k -> new PaxosState());
    }
    
    // ========== PUBLIC ACCESSORS FOR TESTING ==========
    
    public Map<Integer, PaxosState> getPaxosLog() {
        return paxosLog;
    }
    
    public String getValue(String key) {
        return kvStore.get(key);
    }
    
    public int getHighWaterMark() {
        return highWaterMark;
    }
}

