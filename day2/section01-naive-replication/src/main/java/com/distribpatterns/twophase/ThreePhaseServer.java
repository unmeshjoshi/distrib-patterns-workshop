package com.distribpatterns.twophase;

import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.Replica;
import com.tickloom.messaging.*;
import com.tickloom.network.MessageCodec;
import com.tickloom.network.PeerType;
import com.tickloom.storage.Storage;
import com.tickloom.util.Clock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Three-Phase Commit Server with coordinator failure recovery.
 * 
 * Phase 0 (Query/CanCommit):
 *   - New coordinator queries all nodes for pending accepted requests
 *   - If any pending request found: complete it first (recovery mode)
 *   - If no pending requests: proceed with new client request (normal mode)
 *   - Recovery strategy: Pick highest requestId (lexicographic order)
 * 
 * Phase 1 (Prepare/Accept):
 *   - Same as Two-Phase Commit
 *   - Coordinator sends ACCEPT to all replicas
 *   - Each replica prepares (stores operation without executing)
 *   - Coordinator waits for quorum
 * 
 * Phase 2 (Commit):
 *   - Same as Two-Phase Commit
 *   - Coordinator sends COMMIT to all replicas
 *   - Each replica executes the prepared operation
 * 
 * Key difference from Two-Phase Commit:
 *   - Query phase enables recovery from coordinator failures
 *   - Can complete orphaned accepted-but-uncommitted requests
 *   - Client request ignored if recovery is needed
 */
public class ThreePhaseServer extends Replica {
    
    private final Map<String, Integer> counters = new HashMap<>();
    
    // Coordinator state
    private String currentRequestId;
    private String currentClientKey;  // Track the waiting list key for client response
    private boolean isRecoveryMode = false;
    
    // Participant state: prepared operations waiting for commit
    private final Map<String, Operation> preparedOperations = new HashMap<>();
    
    public ThreePhaseServer(List<ProcessId> peerIds,Storage storage, ProcessParams processParams) {
        super(peerIds, storage, processParams);
    }
    
    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
            TwoPhaseMessageTypes.CLIENT_EXECUTE_REQUEST, this::handleClientExecuteRequest,
            TwoPhaseMessageTypes.QUERY_REQUEST, this::handleQueryRequest,
            TwoPhaseMessageTypes.QUERY_RESPONSE, this::handleQueryResponse,
            TwoPhaseMessageTypes.ACCEPT_REQUEST, this::handleAcceptRequest,
            TwoPhaseMessageTypes.ACCEPT_RESPONSE, this::handleAcceptResponse,
            TwoPhaseMessageTypes.COMMIT_REQUEST, this::handleCommitRequest
        );
    }
    
    // Phase 0: Coordinator receives client request - first query for pending requests
    private void handleClientExecuteRequest(Message clientMessage) {
        String queryId = java.util.UUID.randomUUID().toString();
        
        System.out.println(id + ": Starting three-phase execution with query phase " + queryId);
        
        // Store client message for later (if no recovery needed)
        String clientRequestIdKey = "client_" + queryId;
        waitingList.add(clientRequestIdKey, new RequestCallback<Object>() {
            @Override
            public void onResponse(Object response, ProcessId fromNode) {
                var responseMsg = new Message(id, clientMessage.source(), PeerType.SERVER, 
                    TwoPhaseMessageTypes.CLIENT_EXECUTE_RESPONSE, messageCodec.encode(response), 
                    clientMessage.correlationId());
                send(responseMsg);
            }

            @Override
            public void onError(Exception error) {
                send(new Message(id, clientMessage.source(), PeerType.SERVER, 
                    TwoPhaseMessageTypes.CLIENT_EXECUTE_RESPONSE, new byte[0], 
                    clientMessage.correlationId()));
            }
        });
        
        // Create quorum callback for QUERY responses
        var quorumCallback = new AsyncQuorumCallback<QueryResponse>(
            getAllNodes().size(),
            response -> response != null
        );
        
        quorumCallback
            .onSuccess(responses -> {
                System.out.println(id + ": Query quorum reached, analyzing responses");
                
                // Check if any node has pending request
                Optional<QueryResponse> highestPending = responses.values().stream()
                    .filter(QueryResponse::hasPendingRequest)
                    .max((r1, r2) -> r1.pendingRequestId().compareTo(r2.pendingRequestId()));
                
                if (highestPending.isPresent()) {
                    // Recovery mode: complete the pending request
                    System.out.println(id + ": RECOVERY MODE - Found pending request " + 
                        highestPending.get().pendingRequestId() + ", completing it first");
                    System.out.println(id + ": Ignoring new client request during recovery");
                    
                    isRecoveryMode = true;
                    completeRecoveredRequest(highestPending.get(), clientRequestIdKey);
                } else {
                    // Normal mode: no pending requests, proceed with client request
                    System.out.println(id + ": Normal mode - no pending requests found");
                    proceedWithTwoPhaseCommit(clientMessage, clientRequestIdKey);
                }
            })
            .onFailure(error -> {
                System.out.println(id + ": Query quorum failed: " + error.getMessage());
                // Fail the client request
                waitingList.handleResponse(clientRequestIdKey, 
                    new ExecuteResponse(false, 0), id);
            });
        
        // Broadcast QUERY to all nodes (including self)
        QueryRequest queryReq = new QueryRequest(queryId);
        broadcastToAllReplicas(quorumCallback, (node, correlationId) ->
            createMessage(node, correlationId, queryReq, TwoPhaseMessageTypes.QUERY_REQUEST)
        );
        
        System.out.println(id + ": Sent QUERY to " + getAllNodes().size() + " nodes");
    }
    
    // Phase 0: Participant receives QUERY request
    private void handleQueryRequest(Message message) {
        QueryRequest request = deserializePayload(message.payload(), QueryRequest.class);
        
        System.out.println(id + ": Received QUERY " + request.queryId());
        
        // Check if we have any pending accepted requests
        QueryResponse response;
        if (preparedOperations.isEmpty()) {
            response = QueryResponse.noPendingRequest(request.queryId());
            System.out.println(id + ": No pending requests");
        } else {
            // Return the first (and should be only) pending request
            // In full implementation, might need to handle multiple pending requests
            Map.Entry<String, Operation> pending = preparedOperations.entrySet().iterator().next();
            response = QueryResponse.withPendingRequest(
                request.queryId(), 
                pending.getKey(), 
                pending.getValue()
            );
            System.out.println(id + ": Reporting pending request " + pending.getKey());
        }
        
        // Send query response back
        Message responseMsg = createMessage(message.source(), message.correlationId(),
            response, TwoPhaseMessageTypes.QUERY_RESPONSE);
        send(responseMsg);
    }
    
    // Phase 0→1: Coordinator receives QUERY responses
    private void handleQueryResponse(Message message) {
        QueryResponse response = deserializePayload(message.payload(), QueryResponse.class);
        
        System.out.println(id + ": Received QUERY response from " + message.source() + 
                         (response.hasPendingRequest() ? 
                          " (has pending: " + response.pendingRequestId() + ")" : 
                          " (no pending)"));
        
        // Delegate to quorum callback
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }
    
    private void completeRecoveredRequest(QueryResponse pending, String clientRequestIdKey) {
        String recoveredRequestId = pending.pendingRequestId();
        Operation recoveredOperation = pending.pendingOperation();
        
        this.currentRequestId = recoveredRequestId;
        
        System.out.println(id + ": Acting as new coordinator for recovered request " + recoveredRequestId);
        System.out.println(id + ": Re-running full two-phase protocol for recovery");
        
        // IMPORTANT: We must re-run Phase 1 (ACCEPT) to ensure quorum
        // Even though some nodes may have already accepted, the old coordinator crashed
        // before completing the protocol. We need fresh quorum confirmation.
        
        // Create quorum callback for ACCEPT responses
        var quorumCallback = new AsyncQuorumCallback<AcceptResponse>(
            getAllNodes().size(),
            response -> response != null && response.accepted()
        );
        
        quorumCallback
            .onSuccess(responses -> {
                System.out.println(id + ": Recovery ACCEPT quorum reached for " + recoveredRequestId + 
                                 ", sending COMMIT");
                sendCommitToAll(recoveredRequestId);
                
                // After commit completes, we're done with recovery
                // The ignored client request would be queued/retried in production
            })
            .onFailure(error -> {
                System.out.println(id + ": Recovery ACCEPT quorum failed for " + recoveredRequestId + 
                                 ": " + error.getMessage());
                // In full 3PC, would need to send ABORT here
                // For now, just log the failure
            });
        
        // Phase 1: Broadcast ACCEPT for the recovered operation
        AcceptRequest acceptReq = new AcceptRequest(recoveredRequestId, recoveredOperation);
        broadcastToAllReplicas(quorumCallback, (node, correlationId) ->
            createMessage(node, correlationId, acceptReq, TwoPhaseMessageTypes.ACCEPT_REQUEST)
        );
        
        System.out.println(id + ": Sent recovery ACCEPT to " + getAllNodes().size() + " nodes");
        
        // Note: We're ignoring the client request in recovery mode
        // In production, might queue it for after recovery completes
    }
    
    private void proceedWithTwoPhaseCommit(Message clientMessage, String clientRequestIdKey) {
        String requestId = java.util.UUID.randomUUID().toString();
        this.currentRequestId = requestId;
        this.currentClientKey = clientRequestIdKey;
        this.isRecoveryMode = false;
        
        System.out.println(id + ": Starting Phase 1 (ACCEPT) for request " + requestId);
        
        // Create quorum callback for ACCEPT responses
        var quorumCallback = new AsyncQuorumCallback<AcceptResponse>(
            getAllNodes().size(),
            response -> response != null && response.accepted()
        );
        
        quorumCallback
            .onSuccess(responses -> {
                System.out.println(id + ": ACCEPT quorum reached for " + requestId + ", sending COMMIT");
                sendCommitToAll(requestId);
            })
            .onFailure(error -> {
                System.out.println(id + ": ACCEPT quorum failed for " + requestId + ": " + error.getMessage());
                waitingList.handleResponse(clientRequestIdKey, 
                    new ExecuteResponse(false, 0), id);
            });
        
        // Phase 1: Broadcast ACCEPT to all nodes
        var request = deserializePayload(clientMessage.payload(), ExecuteRequest.class);
        AcceptRequest acceptReq = new AcceptRequest(requestId, request.operation());
        broadcastToAllReplicas(quorumCallback, (node, correlationId) ->
            createMessage(node, correlationId, acceptReq, TwoPhaseMessageTypes.ACCEPT_REQUEST)
        );
        
        System.out.println(id + ": Sent ACCEPT to " + getAllNodes().size() + " nodes");
    }
    
    // Phase 1: Participant receives ACCEPT request
    private void handleAcceptRequest(Message message) {
        AcceptRequest request = deserializePayload(message.payload(), AcceptRequest.class);
        
        System.out.println(id + ": Received ACCEPT for txn " + request.transactionId());
        
        // Prepare: Store operation WITHOUT executing it yet
        preparedOperations.put(request.transactionId(), request.operation());
        
        System.out.println(id + ": Prepared txn " + request.transactionId() + 
                         " (operation stored, not executed)");
        
        // Send acceptance back to coordinator
        AcceptResponse response = new AcceptResponse(request.transactionId(), true);
        Message responseMsg = createMessage(message.source(), message.correlationId(),
            response, TwoPhaseMessageTypes.ACCEPT_RESPONSE);
        send(responseMsg);
        
        System.out.println(id + ": Sent ACCEPT response for txn " + request.transactionId());
    }
    
    // Phase 1→2: Coordinator receives ACCEPT responses
    private void handleAcceptResponse(Message message) {
        AcceptResponse response = deserializePayload(message.payload(), AcceptResponse.class);
        
        System.out.println(id + ": Received ACCEPT response from " + message.source() + 
                         " for txn " + response.transactionId());
        
        // Delegate to quorum callback
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }
    
    private void sendCommitToAll(String requestId) {
        CommitRequest commitReq = new CommitRequest(requestId);
        
        for (ProcessId node : getAllNodes()) {
            Message commitMsg = createMessage(node, java.util.UUID.randomUUID().toString(),
                commitReq, TwoPhaseMessageTypes.COMMIT_REQUEST);
            send(commitMsg);
        }
        
        System.out.println(id + ": Broadcast COMMIT to all nodes for txn " + requestId);
    }
    
    // Phase 2: All participants receive COMMIT and execute
    private void handleCommitRequest(Message message) {
        CommitRequest request = deserializePayload(message.payload(), CommitRequest.class);
        
        // Retrieve the prepared operation
        Operation operation = preparedOperations.remove(request.requestId());
        if (operation == null) {
            System.out.println(id + ": No prepared operation for txn " + request.requestId());
            return;
        }
        
        System.out.println(id + ": COMMIT received for txn " + request.requestId() +
                         ", executing operation NOW");
        
        // Execute the operation (only happens after quorum + commit!)
        int result = executeOperation(operation);
        
        // If this node is the coordinator, send response to client
        if (request.requestId().equals(currentRequestId)) {
            if (isRecoveryMode) {
                System.out.println(id + ": Recovery complete for txn " + request.requestId());
                // In recovery mode, we don't send client response (request was ignored)
            } else {
                // Normal mode: send response to client using the tracked client key
                ExecuteResponse response = new ExecuteResponse(true, result);
                waitingList.handleResponse(currentClientKey, response, message.source());
                
                System.out.println(id + ": Sent response to client for txn " + request.requestId());
            }
            
            // Clear coordinator state
            currentRequestId = null;
            currentClientKey = null;
            isRecoveryMode = false;
        }
    }
    
    private int executeOperation(Operation operation) {
        if (operation instanceof IncrementCounterOperation inc) {
            int currentValue = counters.getOrDefault(inc.key(), 0);
            int newValue = currentValue + inc.delta();
            counters.put(inc.key(), newValue);
            
            System.out.println(id + ": Executed INCREMENT " + inc.key() + 
                             " by " + inc.delta() + " = " + newValue);
            
            return newValue;
        }
        throw new IllegalArgumentException("Unknown operation type: " + operation);
    }
    
    public Integer getCounterValue(String key) {
        return counters.get(key);
    }
}

