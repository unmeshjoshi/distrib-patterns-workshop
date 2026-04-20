package com.distribpatterns.twophase;

import com.distribpatterns.twophase.messages.AcceptRequest;
import com.distribpatterns.twophase.messages.AcceptResponse;
import com.distribpatterns.twophase.messages.CommitRequest;
import com.distribpatterns.twophase.messages.ExecuteRequest;
import com.distribpatterns.twophase.messages.ExecuteResponse;
import com.distribpatterns.twophase.messages.QueryRequest;
import com.distribpatterns.twophase.messages.QueryResponse;
import com.distribpatterns.twophase.messages.TwoPhaseMessageTypes;
import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.Replica;
import com.tickloom.future.ListenableFuture;
import com.tickloom.messaging.AsyncQuorumCallback;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageType;
import com.tickloom.messaging.RequestCallback;
import com.tickloom.network.PeerType;

import java.util.HashMap;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

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

    // Participant state: prepared operations waiting for commit
    private final Map<String, Operation> preparedOperations = new HashMap<>();
    
    public ThreePhaseServer(List<ProcessId> peerIds, ProcessParams processParams) {
        super(peerIds, processParams);
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

        //Phase 0: Query for pending accepted requests

        //Phase 0→1: Analyze query responses, then proceed to accept phase

        sendQueryToAll(queryId)
                .andThen(queryResponses -> analyzeAndAccept(queryResponses, clientMessage))
                .whenComplete((acceptResponses, error) -> {
                    sendCommitToAll(clientMessage, acceptResponses, error);
                });
    }

    private void sendCommitToAll(Message clientMessage, Map<ProcessId, AcceptResponse> acceptResponses, Throwable error) {
        if (error != null) {
            System.out.println(id + ": Protocol failed: " + error.getMessage());
            sendClientExecuteResponse(clientMessage, new ExecuteResponse(false, 0));
            return;
        }
        //Phase 2: Commit — all phases completed successfully
        String requestId = acceptResponses.values().iterator().next().transactionId();
        System.out.println(id + ": ACCEPT quorum reached for " + requestId + ", sending COMMIT");
        sendCommitToAll(requestId);
    }

    private void registerClientCompletionCallback(Message clientMessage, String requestId) {
        waitingList.add(requestId, new RequestCallback<Object>() {
            @Override
            public void onResponse(Object response, ProcessId fromNode) {
                Message responseMsg = new Message(
                    id,
                    clientMessage.source(),
                    PeerType.SERVER,
                    TwoPhaseMessageTypes.CLIENT_EXECUTE_RESPONSE,
                    messageCodec.encode(response),
                    clientMessage.correlationId()
                );
                send(responseMsg);
            }

            @Override
            public void onError(Exception error) {
                send(new Message(
                    id,
                    clientMessage.source(),
                    PeerType.SERVER,
                    TwoPhaseMessageTypes.CLIENT_EXECUTE_RESPONSE,
                    new byte[0],
                    clientMessage.correlationId()
                ));
            }
        });
    }

    private ListenableFuture<Map<ProcessId, AcceptResponse>> analyzeAndAccept(
            Map<ProcessId, QueryResponse> queryResponses,
            Message clientMessage
    ) {
        System.out.println(id + ": Query quorum reached, analyzing responses");

        Optional<QueryResponse> highestPending = highestPendingRequestIn(queryResponses.values());
        if (highestPending.isPresent()) {
            return enterRecoveryMode(highestPending.get());
        }

        System.out.println(id + ": Normal mode - no pending requests found");
        return proceedWithAcceptPhase(clientMessage);
    }

    // Choose the highest request id as a deterministic recovery tie-breaker.
    // This is NOT a correct implementation, just a stop-gap arrangement.
    // Later modules replace it with a proper implementation.
    private Optional<QueryResponse> highestPendingRequestIn(Collection<QueryResponse> queryResponses) {
        return queryResponses.stream()
            .filter(QueryResponse::hasPendingRequest)
            .max((left, right) -> left.pendingRequestId().compareTo(right.pendingRequestId()));
    }

    private ListenableFuture<Map<ProcessId, AcceptResponse>> enterRecoveryMode(QueryResponse pendingRequest) {
        String recoveredRequestId = pendingRequest.pendingRequestId();
        Operation recoveredOperation = pendingRequest.pendingOperation();

        System.out.println(id + ": RECOVERY MODE - Found pending request " +
            recoveredRequestId + ", completing it first");
        System.out.println(id + ": Ignoring new client request during recovery");

        return sendAcceptToAll(recoveredRequestId, recoveredOperation);
    }

    private ListenableFuture<Map<ProcessId, QueryResponse>> sendQueryToAll(String queryId) {
        var quorumCallback = new AsyncQuorumCallback<QueryResponse>(
            getAllNodes().size(),
            response -> response != null
        );

        QueryRequest queryReq = new QueryRequest(queryId);
        broadcastToAllReplicas(quorumCallback, (node, correlationId) ->
            createMessage(node, correlationId, queryReq, TwoPhaseMessageTypes.QUERY_REQUEST)
        );

        System.out.println(id + ": Sent QUERY to " + getAllNodes().size() + " nodes");
        return quorumCallback.getQuorumFuture();
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
    
    private ListenableFuture<Map<ProcessId, AcceptResponse>> proceedWithAcceptPhase(Message clientMessage) {
        String requestId = java.util.UUID.randomUUID().toString();
        registerClientCompletionCallback(clientMessage, requestId);

        System.out.println(id + ": Starting Phase 1 (ACCEPT) for request " + requestId);

        var request = deserializePayload(clientMessage.payload(), ExecuteRequest.class);
        return sendAcceptToAll(requestId, request.operation());
    }

    private ListenableFuture<Map<ProcessId, AcceptResponse>> sendAcceptToAll(
            String requestId,
            Operation operation
    ) {
        var quorumCallback = new AsyncQuorumCallback<AcceptResponse>(
            getAllNodes().size(),
            response -> response != null && response.accepted()
        );

        AcceptRequest acceptReq = new AcceptRequest(requestId, operation);
        broadcastToAllReplicas(quorumCallback, (node, correlationId) ->
            createMessage(node, correlationId, acceptReq, TwoPhaseMessageTypes.ACCEPT_REQUEST)
        );

        System.out.println(id + ": Sent ACCEPT to " + getAllNodes().size() + " nodes");
        return quorumCallback.getQuorumFuture();
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

        waitingList.handleResponse(
            request.requestId(),
            new ExecuteResponse(true, result),
            message.source()
        );
    }

    private void sendClientExecuteResponse(Message clientMessage, ExecuteResponse response) {
        Message responseMsg = new Message(
            id,
            clientMessage.source(),
            PeerType.SERVER,
            TwoPhaseMessageTypes.CLIENT_EXECUTE_RESPONSE,
            messageCodec.encode(response),
            clientMessage.correlationId()
        );
        send(responseMsg);
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
