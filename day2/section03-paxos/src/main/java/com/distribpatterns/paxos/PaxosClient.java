package com.distribpatterns.paxos;

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
 * Client for interacting with Paxos servers.
 * Sends operations to a coordinator which runs the Paxos protocol.
 */
public class PaxosClient extends ClusterClient {
    
    public PaxosClient(List<ProcessId> replicaEndpoints, ProcessParams processParams) {
        super(replicaEndpoints, processParams);
    }
    
    /**
     * Execute an operation using Paxos consensus.
     * 
     * @param coordinator The node to act as coordinator for this request
     * @param operation The operation to execute
     * @return Future containing the execution response
     */
    public ListenableFuture<ExecuteResponse> execute(ProcessId coordinator, Operation operation) {
        ExecuteRequest request = new ExecuteRequest(operation);
        
        System.out.println("PaxosClient: Sending execute request to " + coordinator);
        
        return sendRequest(request, coordinator, PaxosMessageTypes.CLIENT_EXECUTE_REQUEST);
    }
    
    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
            PaxosMessageTypes.CLIENT_EXECUTE_RESPONSE, this::handleExecuteResponse
        );
    }
    
    private void handleExecuteResponse(Message message) {
        ExecuteResponse response = deserialize(message.payload(), ExecuteResponse.class);
        handleResponse(message.correlationId(), response, message.source());
        
        System.out.println("PaxosClient: Received execute response - success: " + 
                         response.success() + ", result: " + response.result());
    }
}

