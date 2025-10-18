package com.distribpatterns.paxoslog;

import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.algorithms.replication.ClusterClient;
import com.tickloom.future.ListenableFuture;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageType;

import java.util.List;
import java.util.Map;

/**
 * Client for interacting with PaxosLog cluster.
 */
public class PaxosLogClient extends ClusterClient {
    
    public PaxosLogClient(List<ProcessId> replicaEndpoints, ProcessParams processParams) {
        super(replicaEndpoints, processParams);
    }
    
    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
            PaxosLogMessageTypes.CLIENT_EXECUTE_RESPONSE, this::handleExecuteResponse,
            PaxosLogMessageTypes.CLIENT_GET_RESPONSE, this::handleGetResponse
        );
    }
    
    /**
     * Execute an operation in the replicated log.
     */
    public ListenableFuture<ExecuteCommandResponse> execute(ProcessId coordinator, Operation operation) {
        ExecuteCommandRequest request = new ExecuteCommandRequest(operation);
        return sendRequest(request, coordinator, PaxosLogMessageTypes.CLIENT_EXECUTE_REQUEST);
    }
    
    /**
     * Get a value from the key-value store.
     */
    public ListenableFuture<GetValueResponse> getValue(ProcessId coordinator, String key) {
        GetValueRequest request = new GetValueRequest(key);
        return sendRequest(request, coordinator, PaxosLogMessageTypes.CLIENT_GET_REQUEST);
    }
    
    private void handleExecuteResponse(Message message) {
        ExecuteCommandResponse response = deserialize(message.payload(), ExecuteCommandResponse.class);
        handleResponse(message.correlationId(), response, message.source());
    }
    
    private void handleGetResponse(Message message) {
        GetValueResponse response = deserialize(message.payload(), GetValueResponse.class);
        handleResponse(message.correlationId(), response, message.source());
    }
}

