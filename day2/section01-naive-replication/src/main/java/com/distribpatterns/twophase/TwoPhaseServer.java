package com.distribpatterns.twophase;

import com.tickloom.ProcessId;
import com.tickloom.Replica;
import com.tickloom.messaging.*;
import com.tickloom.network.MessageCodec;
import com.tickloom.network.PeerType;
import com.tickloom.storage.Storage;
import com.tickloom.util.Clock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Two-Phase Commit Server implementing consensus-based replication.
 * 
 * Phase 1 (Prepare/Accept):
 *   - Coordinator receives client request
 *   - Coordinator sends ACCEPT to all replicas (including self)
 *   - Each replica prepares (stores operation without executing)
 *   - Each replica responds with ACCEPT
 *   - Coordinator waits for quorum
 * 
 * Phase 2 (Commit):
 *   - Coordinator sends COMMIT to all replicas
 *   - Each replica executes the prepared operation
 *   - Coordinator sends response to client
 * 
 * Key difference from naive replication:
 *   - Operations execute ONLY after quorum acceptance
 *   - No response sent to client until operation is durable
 *   - We need to store and manage 'accepted requests'.
 */
public class TwoPhaseServer extends Replica {
    
    private final Map<String, Integer> counters = new HashMap<>();
    
    // Coordinator state: track the transaction being coordinated
    private String currentRequestId;

    // Participant state: prepared operations waiting for commit
    private final Map<String, Operation> preparedOperations = new HashMap<>();
    
    public TwoPhaseServer(ProcessId id, List<ProcessId> peerIds, MessageBus messageBus,
                         MessageCodec messageCodec, Storage storage, Clock clock,
                         int requestTimeoutTicks) {
        super(id, peerIds, messageBus, messageCodec, storage, clock, requestTimeoutTicks);
    }
    
    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
            TwoPhaseMessageTypes.CLIENT_EXECUTE_REQUEST, this::handleClientExecuteRequest,
            TwoPhaseMessageTypes.ACCEPT_REQUEST, this::handleAcceptRequest,
            TwoPhaseMessageTypes.ACCEPT_RESPONSE, this::handleAcceptResponse,
            TwoPhaseMessageTypes.COMMIT_REQUEST, this::handleCommitRequest
        );
    }
    
    // Phase 1: Coordinator receives client request
    private void handleClientExecuteRequest(Message clientMessage) {
        String requestId = java.util.UUID.randomUUID().toString();
        System.out.println(id + ": Starting two phased execution " + requestId);

        // Store coordinator state
        this.currentRequestId = requestId;
        waitingList.add(requestId, new RequestCallback<Object>() {
            @Override
            public void onResponse(Object response, ProcessId fromNode) {
                var responseMsg = new Message(id, clientMessage.source(), PeerType.SERVER, TwoPhaseMessageTypes.CLIENT_EXECUTE_RESPONSE, messageCodec.encode(response), clientMessage.correlationId());
                System.out.println(id + ": Received response from " + fromNode + " for txn " + requestId);
                send(responseMsg);
            }

            @Override
            public void onError(Exception error) {
                send(new Message(id, clientMessage.source(), PeerType.SERVER, TwoPhaseMessageTypes.CLIENT_EXECUTE_RESPONSE, new byte[0], clientMessage.correlationId()));
            }
        });

        // Create quorum callback for ACCEPT responses
        var quorumCallback = new AsyncQuorumCallback<AcceptResponse>(
            getAllNodes().size(),
            response -> response != null && response.accepted()
        );
        
        quorumCallback
            .onSuccess(responses -> {
                System.out.println(id + ": Quorum reached for txn " + requestId + ", sending COMMIT");
                sendCommitToAll(requestId);
            })
            .onFailure(error -> {
                System.out.println(id + ": Quorum failed for txn " + requestId + ": " + error.getMessage());
                // In full 2PC, would send ABORT here
            });
        
        // Phase 1: Broadcast ACCEPT to all nodes (including self)
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
        
        // Delegate to quorum callback via RequestWaitingList
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
            ExecuteResponse response = new ExecuteResponse(true, result);
            waitingList.handleResponse(currentRequestId, response, message.source());
            
            System.out.println(id + ": Sent response to client for txn " + request.requestId());
            
            // Clear coordinator state
            currentRequestId = null;
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

