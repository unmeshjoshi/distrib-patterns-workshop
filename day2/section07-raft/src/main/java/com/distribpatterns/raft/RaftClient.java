package com.distribpatterns.raft;

import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.algorithms.replication.ClusterClient;
import com.tickloom.future.ListenableFuture;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Client for interacting with Raft cluster.
 */
public class RaftClient extends ClusterClient {
    
    private Map<String, ListenableFuture<GetValueResponse>> pendingGetFutures = new HashMap<>();
    
    public RaftClient(List<ProcessId> replicaEndpoints, ProcessParams processParams) {
        super(replicaEndpoints, processParams);
    }
    
    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        Map<MessageType, Handler> handlers = new HashMap<>();
        handlers.put(RaftMessageTypes.CLIENT_EXECUTE_RESPONSE, this::handleExecuteResponse);
        handlers.put(RaftMessageTypes.CLIENT_GET_RESPONSE, this::handleGetResponse);
        return handlers;
    }
    
    public ListenableFuture<ExecuteCommandResponse> execute(ProcessId serverId, Operation operation) {
        return sendRequest(new ExecuteCommandRequest(operation), serverId, 
                          RaftMessageTypes.CLIENT_EXECUTE_REQUEST);
    }
    
    public ListenableFuture<GetValueResponse> getValue(ProcessId serverId, String key) {
        return sendRequest(new GetValueRequest(key), serverId,
                          RaftMessageTypes.CLIENT_GET_REQUEST);
    }
    
    private void handleExecuteResponse(Message message) {
        ExecuteCommandResponse response = deserializePayload(message.payload(), ExecuteCommandResponse.class);
        handleResponse(message.correlationId(), response, message.source());
    }
    
    private void handleGetResponse(Message message) {
        GetValueResponse response = deserializePayload(message.payload(), GetValueResponse.class);
        handleResponse(message.correlationId(), response, message.source());
    }
}

