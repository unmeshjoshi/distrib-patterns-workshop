package com.distribpatterns.generation;

import com.tickloom.ProcessId;
import com.tickloom.algorithms.replication.ClusterClient;
import com.tickloom.future.ListenableFuture;
import com.tickloom.messaging.*;
import com.tickloom.network.MessageCodec;
import com.tickloom.util.Clock;

import java.util.List;
import java.util.Map;

/**
 * Client for requesting next generation numbers from Generation Voting servers.
 * 
 * Usage:
 *   var client = new GenerationVotingClient(...);
 *   ListenableFuture<NextGenerationResponse> future = client.getNextGeneration(coordinatorId);
 *   
 * The server will run leader election and return the elected generation number.
 */
public class GenerationVotingClient extends ClusterClient {
    
    public GenerationVotingClient(ProcessId clientId, List<ProcessId> replicaEndpoints,
                                 MessageBus messageBus, MessageCodec messageCodec,
                                 Clock clock, int timeoutTicks) {
        super(clientId, replicaEndpoints, messageBus, messageCodec, clock, timeoutTicks);
    }
    
    /**
     * Request next generation from server, triggering leader election.
     * 
     * The server will:
     * 1. Propose generation + 1 to all replicas
     * 2. Run quorum voting (prepare/promise)
     * 3. Return elected generation if quorum reached
     * 4. Retry with higher generation if election fails
     * 
     * @param server The server to send request to (will act as coordinator)
     * @return Future that completes with the elected generation number
     */
    public ListenableFuture<NextGenerationResponse> getNextGeneration(ProcessId server) {
        NextGenerationRequest request = new NextGenerationRequest();
        
        System.out.println("Client " + id + ": Requesting next generation from " + server);
        
        return sendRequest(request, server, GenerationMessageTypes.NEXT_GENERATION_REQUEST);
    }
    
    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
            GenerationMessageTypes.NEXT_GENERATION_RESPONSE, this::handleNextGenerationResponse
        );
    }
    
    private void handleNextGenerationResponse(Message message) {
        NextGenerationResponse response = deserialize(message.payload(), NextGenerationResponse.class);
        handleResponse(message.correlationId(), response, message.source());
        
        System.out.println("Client " + id + ": Received generation " + response.generation() + 
                         " from " + message.source());
    }
}

