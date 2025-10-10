package com.distribpatterns.multipaxos;

import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.algorithms.replication.ClusterClient;
import com.tickloom.future.ListenableFuture;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageBus;
import com.tickloom.messaging.MessageType;
import com.tickloom.network.MessageCodec;
import com.tickloom.util.Clock;

import java.util.List;
import java.util.Map;

/**
 * Client for interacting with Multi-Paxos cluster.
 */
public class MultiPaxosClient extends ClusterClient {
    
    public MultiPaxosClient(List<ProcessId> replicaEndpoints, ProcessParams processParams) {
        super(replicaEndpoints, processParams);
    }
    
    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
            MultiPaxosMessageTypes.CLIENT_EXECUTE_RESPONSE, this::handleExecuteResponse,
            MultiPaxosMessageTypes.CLIENT_GET_RESPONSE, this::handleGetResponse
        );
    }
    
    /**
     * Execute an operation in the replicated log.
     * Must be sent to current leader.
     */
    public ListenableFuture<ExecuteCommandResponse> execute(ProcessId leader, Operation operation) {
        ExecuteCommandRequest request = new ExecuteCommandRequest(operation);
        return sendRequest(request, leader, MultiPaxosMessageTypes.CLIENT_EXECUTE_REQUEST);
    }
    
    /**
     * Get a value from the key-value store.
     * Must be sent to current leader.
     */
    public ListenableFuture<GetValueResponse> getValue(ProcessId leader, String key) {
        GetValueRequest request = new GetValueRequest(key);
        return sendRequest(request, leader, MultiPaxosMessageTypes.CLIENT_GET_REQUEST);
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

