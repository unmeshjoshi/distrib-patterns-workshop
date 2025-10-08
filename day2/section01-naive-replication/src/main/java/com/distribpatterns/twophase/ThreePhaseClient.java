package com.distribpatterns.twophase;

import com.tickloom.ProcessId;
import com.tickloom.algorithms.replication.ClusterClient;
import com.tickloom.future.ListenableFuture;
import com.tickloom.messaging.*;
import com.tickloom.network.MessageCodec;
import com.tickloom.util.Clock;

import java.util.List;
import java.util.Map;

/**
 * Client for Three-Phase Commit servers.
 * 
 * Sends execute requests to coordinator, which:
 * 1. Queries all nodes for pending requests (Phase 0)
 * 2. If recovery needed: completes pending request, ignores this client request
 * 3. If no recovery: proceeds with ACCEPT (Phase 1) → COMMIT (Phase 2)
 * 
 * Client is unaware of the phases - just sends request and waits for response.
 * During recovery, the client request may be ignored (or queued in production).
 */
public class ThreePhaseClient extends ClusterClient {
    
    public ThreePhaseClient(ProcessId clientId, List<ProcessId> replicaEndpoints,
                           MessageBus messageBus, MessageCodec messageCodec,
                           Clock clock, int timeoutTicks) {
        super(clientId, replicaEndpoints, messageBus, messageCodec, clock, timeoutTicks);
    }
    
    /**
     * Execute an operation through Three-Phase Commit.
     * 
     * @param coordinator The node to act as transaction coordinator
     * @param operation The operation to execute
     * @return Future that completes when operation is committed (or timeout/recovery)
     */
    public ListenableFuture<ExecuteResponse> execute(ProcessId coordinator, Operation operation) {
        ExecuteRequest request = new ExecuteRequest(operation);
        
        System.out.println("ThreePhaseClient: Sending execute request to " + coordinator);
        
        return sendRequest(request, coordinator, TwoPhaseMessageTypes.CLIENT_EXECUTE_REQUEST);
    }
    
    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
            TwoPhaseMessageTypes.CLIENT_EXECUTE_RESPONSE, this::handleExecuteResponse
        );
    }
    
    private void handleExecuteResponse(Message message) {
        ExecuteResponse response = deserialize(message.payload(), ExecuteResponse.class);
        handleResponse(message.correlationId(), response, message.source());
        
        System.out.println("ThreePhaseClient: Received execute response - success: " + 
                         response.success() + ", newValue: " + response.newValue());
    }
}

