package com.distribpatterns.naive;

import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.algorithms.replication.ClusterClient;
import com.tickloom.future.ListenableFuture;
import com.tickloom.messaging.*;
import com.tickloom.network.MessageCodec;
import com.tickloom.util.Clock;

import java.util.List;
import java.util.Map;

/**
 * Client for interacting with single server or replicated servers
 */
public class CounterClient extends ClusterClient {
    
    public CounterClient(List<ProcessId> replicaEndpoints, ProcessParams processParams) {
        super(replicaEndpoints, processParams);
    }
    
    /**
     * Send increment request to server
     */
    public ListenableFuture<IncrementCounterResponse> increment(ProcessId server, String key, int delta) {
        IncrementCounterRequest request = new IncrementCounterRequest(key, delta);
        
        System.out.println("Client: Sent INCREMENT " + key + " by " + delta + " to " + server);
        
        return sendRequest(request, server, MessageTypes.INCREMENT_REQUEST);
    }
    
    /**
     * Send get request to server
     */
    public ListenableFuture<GetResponse> get(ProcessId server, String key) {
        GetRequest request = new GetRequest(key);
        
        System.out.println("Client: Sent GET " + key + " to " + server);
        
        return sendRequest(request, server, MessageTypes.GET_REQUEST);
    }
    
    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
            MessageTypes.INCREMENT_RESPONSE, this::handleIncrementResponse,
            MessageTypes.GET_RESPONSE, this::handleGetResponse
        );
    }
    
    private void handleIncrementResponse(Message message) {
        IncrementCounterResponse response = deserialize(message.payload(), IncrementCounterResponse.class);
        handleResponse(message.correlationId(), response, message.source());
        
        System.out.println("Client: Received INCREMENT response - success: " + 
                         response.success() + ", newValue: " + response.newValue());
    }
    
    private void handleGetResponse(Message message) {
        GetResponse response = deserialize(message.payload(), GetResponse.class);
        handleResponse(message.correlationId(), response, message.source());
        
        System.out.println("Client: Received GET response - found: " + 
                         response.found() + ", value: " + response.value());
    }
}
