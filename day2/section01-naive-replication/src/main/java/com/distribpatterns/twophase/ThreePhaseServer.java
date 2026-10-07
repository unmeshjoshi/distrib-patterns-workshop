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
import com.tickloom.future.TickCompletableFuture;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageType;
import com.tickloom.messaging.RequestCallback;
import com.tickloom.network.PeerType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Three-Phase Commit Server with coordinator failure recovery.
 *
 * Every node plays two roles:
 *   - Coordinator: drives the protocol when it receives a client request
 *   - Participant: responds to Query, Accept, and Commit messages from the coordinator
 *
 * Phase 0 (Query):  Coordinator asks all nodes for pending prepared-but-uncommitted operations.
 * Phase 1 (Accept): Coordinator sends ACCEPT — either recovering a pending op, or starting a new one.
 * Phase 2 (Commit): Coordinator broadcasts COMMIT; participants execute the prepared operation.
 */
public class ThreePhaseServer extends Replica {

    private final Map<String, Integer> counters = new HashMap<>();
    private final Map<String, Operation> preparedOperations = new HashMap<>();

    public ThreePhaseServer(List<ProcessId> peerIds, ProcessParams processParams) {
        super(peerIds, processParams);
    }

    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
            TwoPhaseMessageTypes.CLIENT_EXECUTE_REQUEST, this::handleClientExecuteRequest,
            // Coordinator receives quorum responses
            TwoPhaseMessageTypes.QUERY_RESPONSE,  this::handleQueryResponse,
            TwoPhaseMessageTypes.ACCEPT_RESPONSE, this::handleAcceptResponse,
            // Participant handlers
            TwoPhaseMessageTypes.QUERY_REQUEST,  this::handleQueryRequest,
            TwoPhaseMessageTypes.ACCEPT_REQUEST, this::handleAcceptRequest,
            TwoPhaseMessageTypes.COMMIT_REQUEST, this::handleCommitRequest
        );
    }

    private void handleClientExecuteRequest(Message clientMessage) {
        new Coordinator(clientMessage).run();
    }

    // ========== COORDINATOR ==========

    private class Coordinator {
        private final Message clientMessage;

        Coordinator(Message clientMessage) {
            this.clientMessage = clientMessage;
        }

        void run() {
            queryForPendingRequests()
                .thenCompose(this::recoverPendingOrAcceptNew)
                .whenComplete(this::commitOrFail);
        }

        private TickCompletableFuture<Map<ProcessId, AcceptResponse>> recoverPendingOrAcceptNew(
                Map<ProcessId, QueryResponse> queryResponses) {
            var pending = findPendingRequest(queryResponses);
            if (pending.isPresent()) {
                return recoverPendingRequest(pending.get());
            }
            return acceptNewClientRequest();
        }

        private TickCompletableFuture<Map<ProcessId, AcceptResponse>> recoverPendingRequest(
                QueryResponse pending) {
            System.out.println(id + ": RECOVERY - completing pending " + pending.pendingRequestId());
            return sendAcceptToAll(pending.pendingRequestId(), pending.pendingOperation());
        }

        private TickCompletableFuture<Map<ProcessId, AcceptResponse>> acceptNewClientRequest() {
            String requestId = createRequestId();
            registerClientCallback(requestId);
            var request = deserializePayload(clientMessage.payload(), ExecuteRequest.class);
            System.out.println(id + ": Normal path - ACCEPT for request " + requestId);
            return sendAcceptToAll(requestId, request.operation());
        }

        private void commitOrFail(Map<ProcessId, AcceptResponse> acceptResponses, Throwable error) {
            if (error != null) {
                System.out.println(id + ": Protocol failed: " + error.getMessage());
                respondToClient(new ExecuteResponse(false, 0));
                return;
            }
            String requestId = acceptedTransactionId(acceptResponses);
            System.out.println(id + ": ACCEPT quorum reached for " + requestId + ", sending COMMIT");
            broadcastCommit(requestId);
        }

        private String acceptedTransactionId(Map<ProcessId, AcceptResponse> acceptResponses) {
            return acceptResponses.values().iterator().next().transactionId();
        }

        private static String createRequestId() {
            return java.util.UUID.randomUUID().toString();
        }

        private TickCompletableFuture<Map<ProcessId, QueryResponse>> queryForPendingRequests() {
            var queryReq = new QueryRequest(createRequestId());
            return ThreePhaseServer.this.<QueryResponse>quorumRequest(TwoPhaseMessageTypes.QUERY_REQUEST, queryReq)
                    .countResponseIf(response -> response != null)
                    .send();
        }

        private TickCompletableFuture<Map<ProcessId, AcceptResponse>> sendAcceptToAll(
                String requestId, Operation operation) {
            var acceptReq = new AcceptRequest(requestId, operation);
            return ThreePhaseServer.this.<AcceptResponse>quorumRequest(TwoPhaseMessageTypes.ACCEPT_REQUEST, acceptReq)
                    .countResponseIf(response -> response != null && response.accepted())
                    .send();
        }

        private Optional<QueryResponse> findPendingRequest(Map<ProcessId, QueryResponse> queryResponses) {
            return queryResponses.values().stream()
                    .filter(QueryResponse::hasPendingRequest)
                    .max((left, right) -> left.pendingRequestId().compareTo(right.pendingRequestId()));
        }

        private void registerClientCallback(String requestId) {
            waitingList.add(requestId, new RequestCallback<Object>() {
                @Override
                public void onResponse(Object response, ProcessId fromNode) {
                    respondToClient((ExecuteResponse) response);
                }
                @Override
                public void onError(Exception error) {
                    respondToClient(new ExecuteResponse(false, 0));
                }
            });
        }

        private void broadcastCommit(String requestId) {
            var commitReq = new CommitRequest(requestId);
            for (ProcessId node : getAllNodes()) {
                send(createMessage(node, createRequestId(),
                        commitReq, TwoPhaseMessageTypes.COMMIT_REQUEST));
            }
        }

        private void respondToClient(ExecuteResponse response) {
            send(new Message(id, clientMessage.source(), PeerType.SERVER,
                    TwoPhaseMessageTypes.CLIENT_EXECUTE_RESPONSE,
                    messageCodec.encode(response), clientMessage.correlationId()));
        }
    }

    // ========== PARTICIPANT ==========

    private void handleQueryRequest(Message message) {
        QueryRequest request = deserializePayload(message.payload(), QueryRequest.class);
        QueryResponse response = reportPreparedState(request.queryId());
        send(createMessage(message.source(), message.correlationId(),
                response, TwoPhaseMessageTypes.QUERY_RESPONSE));
    }

    private QueryResponse reportPreparedState(String queryId) {
        if (preparedOperations.isEmpty()) {
            return QueryResponse.noPendingRequest(queryId);
        }
        Map.Entry<String, Operation> pending = preparedOperations.entrySet().iterator().next();
        System.out.println(id + ": Reporting pending request " + pending.getKey());
        return QueryResponse.withPendingRequest(queryId, pending.getKey(), pending.getValue());
    }

    private void handleAcceptRequest(Message message) {
        AcceptRequest request = deserializePayload(message.payload(), AcceptRequest.class);
        prepareOperation(request.transactionId(), request.operation());
        send(createMessage(message.source(), message.correlationId(),
                new AcceptResponse(request.transactionId(), true),
                TwoPhaseMessageTypes.ACCEPT_RESPONSE));
    }

    private void prepareOperation(String transactionId, Operation operation) {
        System.out.println(id + ": Preparing operation for txn " + transactionId);
        preparedOperations.put(transactionId, operation);
    }

    private void handleCommitRequest(Message message) {
        CommitRequest request = deserializePayload(message.payload(), CommitRequest.class);
        executePreparedOperation(request.requestId(), message.source());
    }

    private void executePreparedOperation(String requestId, ProcessId source) {
        Operation operation = preparedOperations.remove(requestId);
        if (operation == null) {
            System.out.println(id + ": No prepared operation for txn " + requestId);
            return;
        }
        System.out.println(id + ": COMMIT " + requestId + " — executing operation");
        int result = executeOperation(operation);
        waitingList.handleResponse(requestId, new ExecuteResponse(true, result), source);
    }

    private void handleQueryResponse(Message message) {
        QueryResponse response = deserializePayload(message.payload(), QueryResponse.class);
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }

    private void handleAcceptResponse(Message message) {
        AcceptResponse response = deserializePayload(message.payload(), AcceptResponse.class);
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }

    // ========== STATE MACHINE ==========

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
