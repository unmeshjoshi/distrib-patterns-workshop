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

    public NaiveReplicationServer(List<ProcessId> peerIds, Storage storage, ProcessParams processParams) {
        super(peerIds, storage, processParams);
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
        
        System.out.println(this.id + ": INCREMENT " + request.key() + " by " + request.delta());
        
        // NAIVE APPROACH: Execute immediately (step 2)
        int currentValue = counters.getOrDefault(request.key(), 0);
        int newValue = currentValue + request.delta();
        counters.put(request.key(), newValue);
        
        System.out.println(this.id + ": Executed locally - " + request.key() + " = " + newValue);
        
        // PROBLEM WINDOW: If we crash here, update is lost on followers!

        // Step 4: Reply to client immediately (before followers acknowledge!)
        IncrementCounterResponse response = new IncrementCounterResponse(true, newValue);
        Message responseMsg = createMessage(message.source(), message.correlationId(), response, MessageTypes.INCREMENT_RESPONSE);
        send(responseMsg);

        // Step 3: Tell others (best effort - no guarantees!)
        replicateToFollowers(request);

        
        System.out.println(this.id + ": Sent response to client (followers may not have it yet!)");
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
        
        // Apply operation
        int currentValue = counters.getOrDefault(op.key(), 0);
        int newValue = currentValue + op.delta();
        counters.put(op.key(), newValue);
        
        System.out.println(this.id + ": Replicated - " + op.key() + " = " + newValue);
    }
    
    private void handleGetRequest(Message message) {
        GetRequest request = deserializePayload(message.payload(), GetRequest.class);
        
        System.out.println(this.id + ": GET " + request.key());
        
        Integer value = counters.get(request.key());
        GetResponse response = new GetResponse(value != null, value != null ? value : 0);
        
        Message responseMsg = createMessage(message.source(), message.correlationId(), response, MessageTypes.GET_RESPONSE);
        send(responseMsg);
        
        System.out.println(this.id + ": Returned " + request.key() + " = " + (value != null ? value : 0));
    }

    public Integer getContainerValue(String counterKey) {
        return counters.get(counterKey);
    }
}
