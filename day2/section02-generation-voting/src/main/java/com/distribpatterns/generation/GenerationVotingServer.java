package com.distribpatterns.generation;

import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.Replica;
import com.tickloom.messaging.*;
import com.tickloom.network.MessageCodec;
import com.tickloom.network.PeerType;
import com.tickloom.storage.Storage;
import com.tickloom.util.Clock;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Generation Voting Server - Distributed monotonic number generation using quorum voting.
 * 
 * Algorithm:
 * 1. Client sends NextGenerationRequest
 * 2. Server proposes generation + 1 to all replicas (PrepareRequest)
 * 3. Each replica votes: accept if proposed > current, reject otherwise
 * 4. If quorum accepts: election WON, return generation
 * 5. If quorum rejects: election LOST, retry with generation + 2
 * 
 * Key insight: Uses strict > (not >=) to ensure uniqueness without server IDs
 */
public class GenerationVotingServer extends Replica {
    
    // Current generation number (monotonically increasing)
    private int generation = 0;
    
    // Maximum retry attempts for failed elections
    private static final int MAX_ELECTION_ATTEMPTS = 5;
    
    public GenerationVotingServer(List<ProcessId> peerIds, Storage storage, ProcessParams params) {
        super(peerIds, storage, params);
    }
    
    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
            GenerationMessageTypes.NEXT_GENERATION_REQUEST, this::handleNextGenerationRequest,
            GenerationMessageTypes.PREPARE_REQUEST, this::handlePrepareRequest,
            GenerationMessageTypes.PREPARE_RESPONSE, this::handlePrepareResponse
        );
    }
    
    /**
     * Client entry point that triggers leader election.
     * Proposes generation + 1 and runs quorum voting.
     */
    private void handleNextGenerationRequest(Message clientMessage) {
        System.out.println(id + ": Received NextGenerationRequest, starting election for generation " + 
                         (generation + 1));
        
        // Store client callback for response
        String requestId = java.util.UUID.randomUUID().toString();
        waitingList.add(requestId, new RequestCallback<Object>() {
            @Override
            public void onResponse(Object response, ProcessId fromNode) {
                var responseMsg = new Message(id, clientMessage.source(), PeerType.SERVER,
                    GenerationMessageTypes.NEXT_GENERATION_RESPONSE,
                    messageCodec.encode(response),
                    clientMessage.correlationId());
                send(responseMsg);
            }

            @Override
            public void onError(Exception error) {
                System.out.println(id + ": Election failed after all retries: " + error.getMessage());
                send(new Message(id, clientMessage.source(), PeerType.SERVER,
                    GenerationMessageTypes.NEXT_GENERATION_RESPONSE,
                    new byte[0],
                    clientMessage.correlationId()));
            }
        });
        
        // Run election starting from generation + 1
        runElection(generation + 1, 0, requestId);
    }
    
    /**
     * Run leader election by proposing a generation number.
     * Retries with higher numbers if election fails.
     */
    private void runElection(int proposedGeneration, int attempt, String requestId) {
        if (attempt >= MAX_ELECTION_ATTEMPTS) {
            System.out.println(id + ": Election failed after " + MAX_ELECTION_ATTEMPTS + " attempts");
            waitingList.handleResponse(requestId, 
                new NextGenerationResponse(0), id);  // 0 = failure
            return;
        }
        
        System.out.println(id + ": Election attempt " + (attempt + 1) + ", proposing generation " + 
                         proposedGeneration);
        
        // Create quorum callback for PREPARE responses
        var quorumCallback = new AsyncQuorumCallback<PrepareResponse>(
            getAllNodes().size(),
            response -> response != null && response.promised()
        );
        
        final int finalProposedGeneration = proposedGeneration;
        quorumCallback
            .onSuccess(responses -> {
                // Quorum reached - election WON!
                System.out.println(id + ": Election WON for generation " + finalProposedGeneration + 
                                 " (quorum: " + responses.size() + "/" + getAllNodes().size() + ")");
                
                // Update our generation
                generation = finalProposedGeneration;
                
                // Send response to client
                waitingList.handleResponse(requestId,
                    new NextGenerationResponse(finalProposedGeneration), id);
            })
            .onFailure(error -> {
                // Quorum failed - election LOST, retry with higher generation
                System.out.println(id + ": Election LOST for generation " + finalProposedGeneration + 
                                 ", retrying with " + (finalProposedGeneration + 1));
                
                // Retry with next generation number
                runElection(finalProposedGeneration + 1, attempt + 1, requestId);
            });
        
        // Broadcast PREPARE to all replicas (including self)
        PrepareRequest prepareReq = new PrepareRequest(proposedGeneration);
        broadcastToAllReplicas(quorumCallback, (node, correlationId) ->
            createMessage(node, correlationId, prepareReq, GenerationMessageTypes.PREPARE_REQUEST)
        );
        
        System.out.println(id + ": Broadcast PREPARE for generation " + proposedGeneration + 
                         " to " + getAllNodes().size() + " nodes");
    }
    
    /**
     * Handle PREPARE request from coordinator.
     * Vote by comparing proposed generation with current:
     * - If proposed > current: ACCEPT (update generation, promise)
     * - Otherwise: REJECT
     */
    private void handlePrepareRequest(Message message) {
        PrepareRequest request = deserializePayload(message.payload(), PrepareRequest.class);
        
        boolean promised = false;
        
        // Strict > comparison ensures uniqueness
        if (request.proposedGeneration() > generation) {
            // Accept this generation
            generation = request.proposedGeneration();
            promised = true;
            System.out.println(id + ": ACCEPTED generation " + generation + 
                             " from " + message.source());
        } else {
            // Reject - already seen higher or equal
            System.out.println(id + ": REJECTED generation " + request.proposedGeneration() + 
                             " from " + message.source() + 
                             " (current: " + generation + ")");
        }
        
        // Send vote back to coordinator
        PrepareResponse response = new PrepareResponse(promised);
        Message responseMsg = createMessage(message.source(), message.correlationId(),
            response, GenerationMessageTypes.PREPARE_RESPONSE);
        send(responseMsg);
    }
    
    /**
     * Handle PREPARE response (vote) from replica.
     * Delegate to quorum callback for counting.
     */
    private void handlePrepareResponse(Message message) {
        PrepareResponse response = deserializePayload(message.payload(), PrepareResponse.class);
        
        System.out.println(id + ": Received vote from " + message.source() + 
                         ": " + (response.promised() ? "PROMISE" : "REJECT"));
        
        // Delegate to quorum callback
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }
    
    /**
     * Get current generation number (for testing/verification).
     */
    public int getGeneration() {
        return generation;
    }
}

