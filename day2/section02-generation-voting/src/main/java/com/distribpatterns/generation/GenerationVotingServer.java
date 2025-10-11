package com.distribpatterns.generation;

import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.Replica;
import com.tickloom.messaging.*;
import com.tickloom.network.PeerType;
import com.tickloom.storage.Storage;

import java.util.List;
import java.util.Map;

/**
 * Generation Voting Server - Distributed monotonic number generation using quorum voting.
 *
 * Algorithm:
 * 1. Client sends NextGenerationRequest
 * 2. Server proposes generation + 1 to all replicas (PrepareRequest)
 * 3. Each replica votes: accept if proposed > current, reject otherwise
 * 4. If quorum accepts: election WON, return generation
 * 5. If quorum rejects: election LOST, retry with generation + 1
 *
 * Key insight: Uses strict > (not >=) to ensure uniqueness without server IDs
 * Design trade-off:
 * - This approach: strict > (no server ID tracking)
 *   ✓ Simpler: only need to track one number (generation)
 *   ✗ Less efficient: must increment generation on every retry
 *
 * - Alternative (Raft/Paxos): >= with server ID tracking (votedFor)
 *   ✓ More efficient: same server can retry same generation
 *   ✗ More complex: must track both generation AND who we accepted from
 *
 * The strict > ensures each generation can only be claimed once across the cluster.
 */
public class GenerationVotingServer extends Replica {

    // Monotonically increasing generation.
    // NOTE: Persist on accept/commit if your Storage API supports it.
    // We can use storage.set("generation", generation) while storing and storage.get("generation") at the start.
    private long generation = 0L;

    // Maximum retry attempts for failed elections
    private static final int MAX_ELECTION_ATTEMPTS = 5;

    public GenerationVotingServer(List<ProcessId> peerIds, Storage storage, ProcessParams params) {
        super(peerIds, storage, params);
        // If you have storage getters, load here. Example (uncomment if available):
        // this.generation = storage.readLong("generation", 0L);
    }

    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
                GenerationMessageTypes.NEXT_GENERATION_REQUEST, this::handleNextGenerationRequest,
                GenerationMessageTypes.PREPARE_REQUEST,         this::handlePrepareRequest,
                GenerationMessageTypes.PREPARE_RESPONSE,        this::handlePrepareResponse
        );
    }

    /**
     * Client entry point that triggers leader election.
     * Proposes generation + 1 and runs quorum voting.
     */
    private void handleNextGenerationRequest(Message clientMessage) {
        long nextGeneration = bumpGeneration(generation);
        System.out.println(id + ": Received NextGenerationRequest, starting election for generation " + nextGeneration);

        // Run election starting from generation + 1
        runElection(nextGeneration, 0, clientMessage);
    }

    private long bumpGeneration(long g) {
        if (g == Long.MAX_VALUE) throw new ArithmeticException("generation overflow");
        return g + 1;
    }

    /**
     * Run leader election by proposing a generation number.
     * Retries with higher numbers if election fails.
     */
    private void runElection(final long proposedGeneration, int attempt, Message clientMessage) {
        if (attemptsExhausted(attempt)) {
            System.out.println(id + ": Election failed after " + MAX_ELECTION_ATTEMPTS + " attempts");
            respondToClient(clientMessage, failureResponse());
            return;
        }

        System.out.println(id + ": Election attempt " + (attempt + 1) + ", proposing generation " + proposedGeneration);

        // Create quorum callback for PREPARE responses
        var quorumCallback = new AsyncQuorumCallback<PrepareResponse>(
                clusterSize(),
                GenerationVotingServer::isPromised
        );

        quorumCallback
                .onSuccess(responses -> {
                    // Quorum reached - election WON!
                    System.out.println(id + ": Election WON for generation " + proposedGeneration +
                            " (quorum: " + responses.size() + "/" + clusterSize() + ")");
                    storeGeneration(proposedGeneration);
                    respondToClient(clientMessage, new NextGenerationResponse(proposedGeneration));
                })
                .onFailure(error -> tryNextGeneration(proposedGeneration, attempt, clientMessage));

        // Broadcast PREPARE to all replicas (including self)
        var prepareReq = new PrepareRequest(proposedGeneration);
        broadcastToAllReplicas(quorumCallback, (node, correlationId) ->
                createMessage(node, correlationId, prepareReq, GenerationMessageTypes.PREPARE_REQUEST)
        );

        System.out.println(id + ": Broadcast PREPARE for generation " + proposedGeneration +
                " to " + clusterSize() + " nodes");
    }

    private static boolean attemptsExhausted(int attempt) {
        return attempt >= MAX_ELECTION_ATTEMPTS;
    }

    private void tryNextGeneration(long lastTriedGeneration, int attempt, Message clientMessage) {
        // Quorum failed - election LOST, retry with higher generation
        long nextGeneration = bumpGeneration(lastTriedGeneration);
        System.out.println(id + ": Election LOST for generation " + lastTriedGeneration + ", retrying with " + nextGeneration);

        // Retry with next generation number
        runElection(nextGeneration, attempt + 1, clientMessage);
    }

    private void storeGeneration(long finalProposedGeneration) {
        // Idempotent commit
        if (finalProposedGeneration > generation) {
            generation = finalProposedGeneration;
        }
        // Persist if your Storage supports it:
        // storage.writeLong("generation", generation);
    }

    private static boolean isPromised(PrepareResponse response) {
        return response != null && response.promised();
    }

    private int clusterSize() {
        return getAllNodes().size();
    }

    private static NextGenerationResponse failureResponse() {
        return new NextGenerationResponse(0);
    }

    private void respondToClient(Message clientMessage, NextGenerationResponse body) {
        var responseMsg = new Message(
                id,
                clientMessage.source(),
                PeerType.SERVER,
                GenerationMessageTypes.NEXT_GENERATION_RESPONSE,
                messageCodec.encode(body),
                clientMessage.correlationId()
        );
        send(responseMsg);
    }

    /**
     * Handle PREPARE request from coordinator.
     * Vote by comparing proposed generation with current:
     * - If proposed > current: ACCEPT (update generation, promise)
     * - Otherwise: REJECT
     */
    private void handlePrepareRequest(Message message) {
        PrepareRequest request = deserializePayload(message.payload(), PrepareRequest.class);

        boolean promised = tryAccept(message, request);

        // Send vote back to coordinator
        var response = new PrepareResponse(promised);
        var responseMsg = createMessage(
                message.source(),
                message.correlationId(),
                response,
                GenerationMessageTypes.PREPARE_RESPONSE
        );
        send(responseMsg);
    }

    private boolean tryAccept(Message message, PrepareRequest request) {
        if (canAccept(request)) {
            // Accept this generation
            storeGeneration(request.proposedGeneration());
            System.out.println(id + ": ACCEPTED generation " + generation + " from " + message.source());
            return true;
        }

        // Reject - already seen higher or equal
        System.out.println(
                id + ": REJECTED generation " + request.proposedGeneration() +
                        " from " + message.source() + " (current: " + generation + ")"
        );
        return false;
    }

    private boolean canAccept(PrepareRequest request) {
        return (long) request.proposedGeneration() > generation;
    }

    /**
     * Handle PREPARE response (vote) from replica.
     * Delegate to quorum callback for counting.
     */
    private void handlePrepareResponse(Message message) {
        PrepareResponse response = deserializePayload(message.payload(), PrepareResponse.class);

        System.out.println(
                id + ": Received vote from " + message.source() + ": " +
                        (response.promised() ? "PROMISE" : "REJECT")
        );

        // Delegate to quorum callback
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }

    /**
     * Get current generation number (for testing/verification).
     */
    public long getGeneration() {
        return generation;
    }
}
