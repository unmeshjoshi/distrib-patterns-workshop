package com.distribpatterns.twophase;

import com.distribpatterns.twophase.messages.AcceptRequest;
import com.distribpatterns.twophase.messages.AcceptResponse;
import com.distribpatterns.twophase.messages.CommitRequest;
import com.distribpatterns.twophase.messages.ExecuteRequest;
import com.distribpatterns.twophase.messages.ExecuteResponse;
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
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Two-Phase Commit Server implementing consensus-based replication.
 * <p>
 * Phase 1 (Prepare/Accept):
 * - Coordinator receives client request
 * - Coordinator sends ACCEPT to all replicas (including self)
 * - Each replica prepares (stores operation without executing)
 * - Each replica responds with ACCEPT
 * - Coordinator waits for quorum
 * <p>
 * Phase 2 (Commit):
 * - Coordinator sends COMMIT to all replicas
 * - Each replica executes the prepared operation
 * - Coordinator sends response to client
 * <p>
 * Key difference from naive replication:
 * - Operations execute ONLY after quorum acceptance
 * - No response sent to client until operation is durable
 * - We need to store and manage 'accepted requests'.
 */
public class TwoPhaseServer extends Replica {

    private final Map<String, Integer> counters = new HashMap<>();

    // Participant state: prepared operations waiting for commit
    private final Map<String, Operation> preparedOperations = new HashMap<>();

    public TwoPhaseServer(List<ProcessId> peerIds, ProcessParams processParams) {
        super(peerIds, processParams);
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

        registerClientCompletionCallback(clientMessage, requestId);

        ExecuteRequest request = deserializePayload(clientMessage.payload(), ExecuteRequest.class);
        coordinateTwoPhaseExecution(requestId, request);
    }

    private void coordinateTwoPhaseExecution(String requestId, ExecuteRequest request) {
        //send accept request and get quorum future
        ListenableFuture<Map<ProcessId, AcceptResponse>> acceptFuture =
                sendAcceptToAll(requestId, request);

        acceptFuture.whenComplete((responses, error) -> {
            if (error != null) {
                System.out.println(id + ": Quorum failed for txn " + requestId + ": " + error.getMessage());
                return;
            }
            System.out.println(id + ": Quorum reached for txn " + requestId + ", sending COMMIT");
            //send commit request.
            sendCommitToAll(requestId);
        });
    }

    // Phase 1: Participant receives ACCEPT request
    private void handleAcceptRequest(Message message) {
        AcceptRequest request = deserializePayload(message.payload(), AcceptRequest.class);

        System.out.println(id + ": Received ACCEPT for txn " + request.transactionId());

        prepareOperation(request);
        sendAcceptResponse(message, request);
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

    private void prepareOperation(AcceptRequest request) {
        preparedOperations.put(request.transactionId(), request.operation());
        System.out.println(id + ": Prepared txn " + request.transactionId() +
                " (operation stored, not executed)");
    }

    private void registerClientCompletionCallback(Message clientMessage, String requestId) {
        // This deferred callback is triggered when completeClientRequest(...) is called
        // after phase 2 executes the prepared operation on this node.
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
                System.out.println(id + ": Received response from " + fromNode + " for txn " + requestId);
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

    private void sendAcceptResponse(Message message, AcceptRequest request) {
        AcceptResponse response = new AcceptResponse(request.transactionId(), true);
        Message responseMsg = createMessage(
                message.source(),
                message.correlationId(),
                response,
                TwoPhaseMessageTypes.ACCEPT_RESPONSE
        );
        send(responseMsg);

        System.out.println(id + ": Sent ACCEPT response for txn " + request.transactionId());
    }

    protected ListenableFuture<Map<ProcessId, AcceptResponse>> sendAcceptToAll(
            String requestId,
            ExecuteRequest request
    ) {
        return sendAcceptToAll(requestId, request.operation());
    }

    protected ListenableFuture<Map<ProcessId, AcceptResponse>> sendAcceptToAll(
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

    // Phase 2: All participants receive COMMIT and execute
    private void handleCommitRequest(Message message) {
        CommitRequest request = deserializePayload(message.payload(), CommitRequest.class);
        String requestId = request.requestId();

        Operation operation = removePreparedOperationFor(requestId);
        if (operation == null) {
            return;
        }

        int result = executeCommittedOperation(requestId, operation);
        completeClientRequest(requestId, result, message.source());
    }

    private void completeClientRequest(String requestId, int result, ProcessId fromNode) {
        ExecuteResponse response = new ExecuteResponse(true, result);
        waitingList.handleResponse(requestId, response, fromNode);
    }

    private Operation removePreparedOperationFor(String requestId) {
        Operation operation = preparedOperations.remove(requestId);
        if (operation == null) {
            System.out.println(id + ": No prepared operation for txn " + requestId);
        }
        return operation;
    }

    private int executeCommittedOperation(String requestId, Operation operation) {
        System.out.println(id + ": COMMIT received for txn " + requestId +
                ", executing operation NOW");
        return executeOperation(operation);
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
