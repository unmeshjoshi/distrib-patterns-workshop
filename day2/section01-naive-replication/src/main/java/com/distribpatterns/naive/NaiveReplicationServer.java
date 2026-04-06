package com.distribpatterns.naive;

import com.distribpatterns.naive.messages.GetRequest;
import com.distribpatterns.naive.messages.GetResponse;
import com.distribpatterns.naive.messages.IncrementCounterRequest;
import com.distribpatterns.naive.messages.IncrementCounterResponse;
import com.distribpatterns.naive.messages.MessageTypes;
import com.distribpatterns.naive.messages.ReplicateOperation;
import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.Replica;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Naive replication: Execute immediately + tell others
 * 
 * Demonstrates the following failure modes:
 * 1. Message loss/reorder can cause divergent states
 * 2. Crash after execute but before replication loses updates
 * 3. No safety guarantees under network partitions
 * 4. Followers can have stale or incorrect data
 */
public class NaiveReplicationServer extends Replica {
    
    private final Map<String, Integer> counters = new HashMap<>();

    public NaiveReplicationServer(List<ProcessId> peerIds, ProcessParams processParams) {
        super(peerIds, processParams);
    }
    
    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
            MessageTypes.INCREMENT_REQUEST, this::handleIncrementRequest,
            MessageTypes.GET_REQUEST, this::handleGetRequest,
            MessageTypes.REPLICATE_OP, this::handleReplicateOp
        );
    }
    
    private void handleIncrementRequest(Message message) {
        IncrementCounterRequest request = deserializePayload(message.payload(), IncrementCounterRequest.class);
        //First increments.
        int newValue = executeIncrement(request);
        sendIncrementResponseToClient(message, newValue);

        //then sends the value to replicas.
        replicateToFollowers(request);
    }
    
    private void replicateToFollowers(IncrementCounterRequest request) {
        System.out.println(this.id + ": Replicating to " + peerIds.size() + " followers");
        
        ReplicateOperation replicateOp = new ReplicateOperation(request.key(), request.delta());
        
        for (ProcessId follower : peerIds) {
            String correlationId = java.util.UUID.randomUUID().toString();
            Message replicateMsg = createMessage(follower, correlationId, replicateOp, MessageTypes.REPLICATE_OP);
            send(replicateMsg);
            System.out.println(this.id + ": Sent replication to " + follower.toString());
        }
        
        // PROBLEMS:
        // - No acknowledgment waited
        // - Messages can be lost
        // - Messages can arrive out of order
        // - No way to detect/fix inconsistencies
    }

    private void handleReplicateOp(Message message) {
        ReplicateOperation op = deserializePayload(message.payload(), ReplicateOperation.class);
        
        System.out.println(this.id + ": REPLICATE " + op.key() + " by " + op.delta());

        int newValue = applyIncrement(op.key(), op.delta());
        System.out.println(this.id + ": Replicated - " + op.key() + " = " + newValue);
    }

    private int executeIncrement(IncrementCounterRequest request) {
        System.out.println(this.id + ": INCREMENT " + request.key() + " by " + request.delta());
        int newValue = applyIncrement(request.key(), request.delta());
        System.out.println(this.id + ": Executed locally - " + request.key() + " = " + newValue);
        return newValue;
    }

    private int applyIncrement(String key, int delta) {
        int currentValue = counters.getOrDefault(key, 0);
        int newValue = currentValue + delta;
        counters.put(key, newValue);
        return newValue;
    }

    private void sendIncrementResponseToClient(Message requestMessage, int newValue) {
        IncrementCounterResponse response = new IncrementCounterResponse(true, newValue);
        Message responseMsg = createMessage(
            requestMessage.source(),
            requestMessage.correlationId(),
            response,
            MessageTypes.INCREMENT_RESPONSE
        );
        send(responseMsg);
        System.out.println(this.id + ": Sent response to client (followers may not have it yet!)");
    }
    
    private void handleGetRequest(Message message) {
        GetRequest request = deserializePayload(message.payload(), GetRequest.class);
        System.out.println(this.id + ": GET " + request.key());

        Integer value = counters.get(request.key());

        sendGetResponse(message, value);
    }

    private void sendGetResponse(Message message, Integer value) {
        GetResponse response = new GetResponse(value != null, value != null ? value : 0);
        Message responseMsg = createMessage(message.source(), message.correlationId(), response, MessageTypes.GET_RESPONSE);
        send(responseMsg);
        System.out.println(this.id + ": Returned " + (value != null ? value : 0));
    }

    public Integer getContainerValue(String counterKey) {
        return counters.get(counterKey);
    }
}
