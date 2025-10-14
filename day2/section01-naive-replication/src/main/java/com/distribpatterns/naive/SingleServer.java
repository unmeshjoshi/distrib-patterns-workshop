package com.distribpatterns.naive;

import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.Replica;
import com.tickloom.messaging.*;
import com.tickloom.network.MessageCodec;
import com.tickloom.storage.Storage;
import com.tickloom.util.Clock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Single server implementation - demonstrates lack of fault tolerance
 * 
 * Issues:
 * - No availability under crash
 * - No durability guarantees
 * - Single point of failure
 */
public class SingleServer extends Replica {
    
    private final Map<String, Integer> counters = new HashMap<>();
    
    public SingleServer(List<ProcessId> peerIds, ProcessParams processParams) {
        super(peerIds, processParams);
    }
    
    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
            MessageTypes.INCREMENT_REQUEST, this::handleIncrementRequest,
            MessageTypes.GET_REQUEST, this::handleGetRequest
        );
    }
    
    private void handleIncrementRequest(Message message) {
        IncrementCounterRequest request = deserializePayload(message.payload(), IncrementCounterRequest.class);
        
        System.out.println("SingleServer: INCREMENT " + request.key() + " by " + request.delta());
        
        // Execute immediately
        int currentValue = counters.getOrDefault(request.key(), 0);
        int newValue = currentValue + request.delta();
        counters.put(request.key(), newValue);
        
        // Send response
        IncrementCounterResponse response = new IncrementCounterResponse(true, newValue);
        Message responseMsg = createMessage(message.source(), message.correlationId(), response, MessageTypes.INCREMENT_RESPONSE);
        send(responseMsg);
        
        System.out.println("SingleServer: " + request.key() + " = " + newValue);
    }
    
    private void handleGetRequest(Message message) {
        GetRequest request = deserializePayload(message.payload(), GetRequest.class);
        
        System.out.println("SingleServer: GET " + request.key());
        
        Integer value = counters.get(request.key());
        GetResponse response = new GetResponse(value != null, value != null ? value : 0);
        
        Message responseMsg = createMessage(message.source(), message.correlationId(), response, MessageTypes.GET_RESPONSE);
        send(responseMsg);
    }
}
