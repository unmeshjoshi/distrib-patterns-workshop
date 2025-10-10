package com.distribpatterns.multipaxos;

import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.Replica;
import com.tickloom.messaging.*;
import com.tickloom.network.MessageCodec;
import com.tickloom.storage.Storage;
import com.tickloom.util.Clock;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Multi-Paxos: Optimized Paxos with stable leader.
 * 
 * Key optimization: After leader election, skip Prepare phase.
 * - Election: Prepare once for ALL future indices
 * - Normal operation: Just Propose + Commit (2 phases instead of 3)
 * 
 * New leader completes uncommitted log entries during election.
 */
public class MultiPaxosServer extends Replica {
    // Server role
    private ServerRole role = ServerRole.LookingForLeader;
    
    // Global promised generation (not per-index!)
    private int promisedGeneration = 0;
    
    // Paxos log: one PaxosState per log index
    private final Map<Integer, PaxosState> paxosLog = new HashMap<>();
    
    // State machine: key-value store
    private final Map<String, String> kvStore = new HashMap<>();
    
    // Track highest committed index (high water mark)
    private int highWaterMark = -1;
    
    // Generation counter
    private final AtomicInteger generationCounter = new AtomicInteger(0);
    
    // Log index counter
    private final AtomicInteger logIndex = new AtomicInteger(0);
    
    // Server ID for breaking ties
    private final int serverId;
    
    // No-op operation for reads
    private static final NoOpOperation NO_OP = new NoOpOperation();
    
    public MultiPaxosServer(List<ProcessId> allNodes, Storage storage, ProcessParams processParams) {
        super(allNodes, storage, processParams);
        this.serverId = id.toString().hashCode();
    }
    
    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
            // Client handlers
            MultiPaxosMessageTypes.CLIENT_EXECUTE_REQUEST, this::handleClientExecuteRequest,
            MultiPaxosMessageTypes.CLIENT_GET_REQUEST, this::handleClientGetRequest,
            
            // Leader Election: Full Log Prepare
            MultiPaxosMessageTypes.FULL_LOG_PREPARE_REQUEST, this::handleFullLogPrepareRequest,
            MultiPaxosMessageTypes.FULL_LOG_PREPARE_RESPONSE, this::handleFullLogPrepareResponse,
            
            // Normal Operation: Propose/Accept
            MultiPaxosMessageTypes.PROPOSE_REQUEST, this::handleProposeRequest,
            MultiPaxosMessageTypes.PROPOSE_RESPONSE, this::handleProposeResponse,
            
            // Normal Operation: Commit/Learn
            MultiPaxosMessageTypes.COMMIT_REQUEST, this::handleCommitRequest,
            MultiPaxosMessageTypes.COMMIT_RESPONSE, this::handleCommitResponse
        );
    }
    
    // ========== CLIENT REQUEST HANDLING ==========
    
    private void handleClientExecuteRequest(Message clientMessage) {
        ExecuteCommandRequest request = deserializePayload(clientMessage.payload(), ExecuteCommandRequest.class);
        
        if (role != ServerRole.Leader) {
            System.out.println(id + ": Rejecting client request - not leader (role=" + role + ")");
            send(createMessage(clientMessage.source(), clientMessage.correlationId(),
                new ExecuteCommandResponse(false, "Not leader"), MultiPaxosMessageTypes.CLIENT_EXECUTE_RESPONSE));
            return;
        }
        
        String clientKey = "client_" + clientMessage.correlationId();
        
        System.out.println(id + ": Leader received execute request for operation: " + request.operation());
        
        // Store client callback
        waitingList.add(clientKey, new RequestCallback<Object>() {
            @Override
            public void onResponse(Object response, ProcessId fromNode) {
                var responseMsg = createMessage(clientMessage.source(), clientMessage.correlationId(),
                    response, MultiPaxosMessageTypes.CLIENT_EXECUTE_RESPONSE);
                send(responseMsg);
            }
            
            @Override
            public void onError(Exception error) {
                send(createMessage(clientMessage.source(), clientMessage.correlationId(),
                    new ExecuteCommandResponse(false, null), MultiPaxosMessageTypes.CLIENT_EXECUTE_RESPONSE));
            }
        });
        
        // Append to log
        appendToLog(request.operation(), clientKey);
    }
    
    private void handleClientGetRequest(Message clientMessage) {
        GetValueRequest request = deserializePayload(clientMessage.payload(), GetValueRequest.class);
        
        if (role != ServerRole.Leader) {
            System.out.println(id + ": Rejecting client get request - not leader");
            send(createMessage(clientMessage.source(), clientMessage.correlationId(),
                new GetValueResponse(null), MultiPaxosMessageTypes.CLIENT_GET_RESPONSE));
            return;
        }
        
        String clientKey = "client_" + clientMessage.correlationId();
        
        System.out.println(id + ": Leader received get request for key: " + request.key());
        
        // Store client callback
        waitingList.add(clientKey, new RequestCallback<Object>() {
            @Override
            public void onResponse(Object response, ProcessId fromNode) {
                // After no-op commits, read from KV store
                String value = kvStore.get(request.key());
                var responseMsg = createMessage(clientMessage.source(), clientMessage.correlationId(),
                    new GetValueResponse(value), MultiPaxosMessageTypes.CLIENT_GET_RESPONSE);
                send(responseMsg);
            }
            
            @Override
            public void onError(Exception error) {
                send(createMessage(clientMessage.source(), clientMessage.correlationId(),
                    new GetValueResponse(null), MultiPaxosMessageTypes.CLIENT_GET_RESPONSE));
            }
        });
        
        // Execute a no-op to ensure we see committed state
        appendToLog(NO_OP, clientKey);
    }
    
    // ========== LOG APPEND (OPTIMIZED - NO PREPARE!) ==========
    
    private void appendToLog(Operation operation, String clientKey) {
        int index = logIndex.getAndIncrement();
        System.out.println(id + ": Leader appending " + operation + " at index " + index);
        
        // KEY OPTIMIZATION: Skip Prepare phase! Leader already has promise.
        startProposePhase(index, promisedGeneration, operation, clientKey);
    }
    
    // ========== PHASE 1: PROPOSE/ACCEPT (NO PREPARE!) ==========
    
    private void startProposePhase(int logIndex, int generation, Operation operation, String clientKey) {
        System.out.println(id + ": Phase 1 - PROPOSE " + operation + " for index " + logIndex);
        
        var quorumCallback = new AsyncQuorumCallback<ProposeResponse>(
            getAllNodes().size(),
            response -> response != null && response.accepted()
        );
        
        quorumCallback
            .onSuccess(responses -> {
                System.out.println(id + ": PROPOSE quorum reached for index " + logIndex);
                startCommitPhase(logIndex, generation, operation, clientKey);
            })
            .onFailure(error -> {
                System.out.println(id + ": PROPOSE quorum failed for index " + logIndex);
                waitingList.handleResponse(clientKey, new ExecuteCommandResponse(false, null), id);
            });
        
        ProposeRequest proposeReq = new ProposeRequest(logIndex, generation, operation);
        broadcastToAllReplicas(quorumCallback, (node, correlationId) ->
            createMessage(node, correlationId, proposeReq, MultiPaxosMessageTypes.PROPOSE_REQUEST)
        );
    }
    
    private void handleProposeRequest(Message message) {
        ProposeRequest request = deserializePayload(message.payload(), ProposeRequest.class);
        int logIndex = request.logIndex();
        int generation = request.generation();
        Operation value = request.value();
        
        // Accept if generation >= promisedGeneration
        boolean accepted = false;
        if (generation >= promisedGeneration) {
            promisedGeneration = generation; // Update if higher
            var paxosState = getOrCreatePaxosState(logIndex);
            var newState = paxosState.accept(generation, value);
            paxosLog.put(logIndex, newState);
            accepted = true;
            System.out.println(id + ": ACCEPTED proposal for index " + logIndex);
        } else {
            System.out.println(id + ": REJECTED proposal for index " + logIndex + 
                             " (gen=" + generation + " < promised=" + promisedGeneration + ")");
        }
        
        send(createMessage(message.source(), message.correlationId(),
            new ProposeResponse(logIndex, accepted), MultiPaxosMessageTypes.PROPOSE_RESPONSE));
    }
    
    private void handleProposeResponse(Message message) {
        ProposeResponse response = deserializePayload(message.payload(), ProposeResponse.class);
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }
    
    // ========== PHASE 2: COMMIT/LEARN ==========
    
    private void startCommitPhase(int logIndex, int generation, Operation operation, String clientKey) {
        System.out.println(id + ": Phase 2 - COMMIT " + operation + " for index " + logIndex);
        
        var quorumCallback = new AsyncQuorumCallback<CommitResponse>(
            getAllNodes().size(),
            response -> response != null && response.success()
        );
        
        quorumCallback
            .onSuccess(responses -> {
                System.out.println(id + ": COMMIT quorum reached for index " + logIndex);
            })
            .onFailure(error -> {
                System.out.println(id + ": COMMIT quorum failed for index " + logIndex);
            });
        
        // Track which client request this is for
        String indexClientKey = "index_" + logIndex + "_client";
        waitingList.add(indexClientKey, new RequestCallback<Object>() {
            @Override
            public void onResponse(Object response, ProcessId fromNode) {
                waitingList.handleResponse(clientKey, response, fromNode);
            }
            
            @Override
            public void onError(Exception error) {
                waitingList.handleResponse(clientKey, new ExecuteCommandResponse(false, null), id);
            }
        });
        
        CommitRequest commitReq = new CommitRequest(logIndex, generation, operation);
        broadcastToAllReplicas(quorumCallback, (node, correlationId) ->
            createMessage(node, correlationId, commitReq, MultiPaxosMessageTypes.COMMIT_REQUEST)
        );
    }
    
    private void handleCommitRequest(Message message) {
        CommitRequest request = deserializePayload(message.payload(), CommitRequest.class);
        int logIndex = request.logIndex();
        int generation = request.generation();
        Operation value = request.value();
        
        System.out.println(id + ": Received COMMIT for index " + logIndex);
        
        // Accept commit (already accepted during propose)
        var paxosState = getOrCreatePaxosState(logIndex);
        var newState = paxosState.commit(generation, value);
        paxosLog.put(logIndex, newState);
        
        System.out.println(id + ": COMMITTED index " + logIndex);
        
        // Try to execute this entry and any subsequent committed entries
        tryExecuteLogEntries();
        
        send(createMessage(message.source(), message.correlationId(),
            new CommitResponse(true), MultiPaxosMessageTypes.COMMIT_RESPONSE));
    }
    
    private void handleCommitResponse(Message message) {
        CommitResponse response = deserializePayload(message.payload(), CommitResponse.class);
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }
    
    // ========== LEADER ELECTION ==========
    
    public void startLeaderElection() {
        System.out.println(id + ": Starting leader election");
        role = ServerRole.LookingForLeader;
        
        int newGeneration = generationCounter.incrementAndGet();
        sendFullLogPrepare(newGeneration);
    }
    
    private void sendFullLogPrepare(int generation) {
        System.out.println(id + ": Sending FULL_LOG_PREPARE with generation " + generation);
        
        var quorumCallback = new AsyncQuorumCallback<FullLogPrepareResponse>(
            getAllNodes().size(),
            response -> response != null && response.promised()
        );
        
        quorumCallback
            .onSuccess(responses -> {
                System.out.println(id + ": Election succeeded, became LEADER with generation " + generation);
                
                // Merge logs from all responses
                for (FullLogPrepareResponse response : responses.values()) {
                    mergeLog(response);
                }
                
                // Become leader
                promisedGeneration = generation;
                role = ServerRole.Leader;
                
                // Complete uncommitted entries
                completeUncommittedEntries(generation);
            })
            .onFailure(error -> {
                System.out.println(id + ": Election failed: " + error.getMessage());
                role = ServerRole.Follower;
            });
        
        FullLogPrepareRequest prepareReq = new FullLogPrepareRequest(generation);
        broadcastToAllReplicas(quorumCallback, (node, correlationId) ->
            createMessage(node, correlationId, prepareReq, MultiPaxosMessageTypes.FULL_LOG_PREPARE_REQUEST)
        );
    }
    
    private void handleFullLogPrepareRequest(Message message) {
        FullLogPrepareRequest request = deserializePayload(message.payload(), FullLogPrepareRequest.class);
        int generation = request.generation();
        
        System.out.println(id + ": Received FULL_LOG_PREPARE with generation " + generation);
        
        if (generation > promisedGeneration) {
            // Accept: update promised generation and become follower
            promisedGeneration = generation;
            role = ServerRole.Follower;
            
            // Send back all uncommitted entries
            Map<Integer, PaxosState> uncommitted = getUncommittedEntries();
            System.out.println(id + ": PROMISED generation " + generation + ", becoming FOLLOWER, " +
                             "sending " + uncommitted.size() + " uncommitted entries");
            
            send(createMessage(message.source(), message.correlationId(),
                FullLogPrepareResponse.accepted(uncommitted), MultiPaxosMessageTypes.FULL_LOG_PREPARE_RESPONSE));
        } else {
            // Reject
            System.out.println(id + ": REJECTED generation " + generation + " (have " + promisedGeneration + ")");
            send(createMessage(message.source(), message.correlationId(),
                FullLogPrepareResponse.rejected(), MultiPaxosMessageTypes.FULL_LOG_PREPARE_RESPONSE));
        }
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
            
            // Choose entry with highest generation
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
                
                // Propose and commit this value
                String dummyClientKey = "recovery_" + index;
                startProposePhase(index, generation, value, dummyClientKey);
            }
        }
    }
    
    private Map<Integer, PaxosState> getUncommittedEntries() {
        return paxosLog.entrySet().stream()
            .filter(e -> e.getValue().committedValue().isEmpty())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
    
    // ========== LOG EXECUTION (STATE MACHINE) ==========
    
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
}

