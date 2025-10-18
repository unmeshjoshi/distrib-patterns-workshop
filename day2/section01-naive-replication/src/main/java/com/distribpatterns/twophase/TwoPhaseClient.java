package com.distribpatterns.twophase;

import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.algorithms.replication.ClusterClient;
import com.tickloom.future.ListenableFuture;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageType;

import java.util.List;
import java.util.Map;

/**
 * Client for executing operations through Two-Phase Commit.
 * 
 * Operations are executed through consensus:
 * - Client sends operation to coordinator
 * - Coordinator runs 2PC protocol (ACCEPT → quorum → COMMIT)
 * - Client receives response after operation is committed and executed
 */
public class TwoPhaseClient extends ClusterClient {
    
    public TwoPhaseClient(List<ProcessId> replicaEndpoints, ProcessParams processParams) {
        super(replicaEndpoints, processParams);
    }
    
    /**
     * Execute an operation through Two-Phase Commit.
     * 
     * @param coordinator The node to act as transaction coordinator
     * @param operation The operation to execute
     * @return Future that completes when operation is committed
     */
    public ListenableFuture<ExecuteResponse> execute(ProcessId coordinator, Operation operation) {
        ExecuteRequest request = new ExecuteRequest(operation);
        
        System.out.println("TwoPhaseClient: Sending execute request to " + coordinator);
        
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
        
        System.out.println("TwoPhaseClient: Received execute response - success: " + 
                         response.success() + ", newValue: " + response.newValue());
    }
}

