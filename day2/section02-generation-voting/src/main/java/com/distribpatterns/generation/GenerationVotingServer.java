package com.distribpatterns.generation;

import com.distribpatterns.generation.messages.GenerationMessageTypes;
import com.distribpatterns.generation.messages.NextGenerationResponse;
import com.distribpatterns.generation.messages.PrepareRequest;
import com.distribpatterns.generation.messages.PrepareResponse;
import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.Replica;
import com.tickloom.future.ListenableFuture;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageType;
import com.tickloom.messaging.RequestCallback;
import com.tickloom.network.PeerType;

import java.util.List;
import java.util.Map;

/**
 * Generation Voting Server - Distributed monotonic number generation using quorum voting.
 * <p>
 * Algorithm:
 * 1. Client sends NextGenerationRequest
 * 2. Server proposes generation + 1 to all replicas (PrepareRequest)
 * 3. Each replica votes: accept if proposed > current, reject otherwise
 * 4. If quorum accepts: election WON, return generation
 * 5. If quorum rejects: election LOST, retry with generation + 1
 * <p>
 * Key insight: Uses strict > (not >=) to ensure uniqueness without server IDs
 * Design trade-off:
 * - This approach: strict > (no server ID tracking)
 * ✓ Simpler: only need to track one number (generation)
 * ✗ Less efficient: must increment generation on every retry
 * <p>
 * - Alternative (Raft/Paxos): >= with server ID tracking (votedFor)
 * ✓ More efficient: same server can retry same generation
 * ✗ More complex: must track both generation AND who we accepted from
 * <p>
 * The strict > ensures each generation can only be claimed once across the cluster.
 */
public class GenerationVotingServer extends Replica {

    // Monotonically increasing generation.

    /*
     * STORAGE NOTE (generation / HardState)
     *
     * You can persist the monotonic generation in one of two ways:
     *
     * A) Custom WAL (etcd-style)
     *    - Append entries, then HardState (generation), then fsync once.
     *    - On restart, replay the WAL; the last HardState is authoritative.
     *    - Examples:
     *        • etcd (Raft WAL under member/wal)
     *
     * B) RocksDB-backed storage (KV-as-log; rely on RocksDB’s own WAL)
     *    - Store generation under a fixed key (e.g., GEN_KEY) as big-endian 64-bit.
     *    - If batching other state, write together in one WriteBatch with sync=true so
     *      they become durable in the same crash cut (RocksDB WAL handles journaling/fsync).
     *
     * Invariant (critical):
     *   - Generation must be durable and monotonic across restarts.
     *
     * This code currently uses option B for simplicity.
     */

    private long generation = 0L;

    // Maximum retry attempts for failed elections
    private static final int MAX_ELECTION_ATTEMPTS = 5;

    private static final String GENERATION_KEY = "generation";
    private long highestProposal = 0L;

    public GenerationVotingServer(List<ProcessId> peerIds, ProcessParams params) {
        super(peerIds, params);
        // Initialization is handled by Process.initialise()
    }

    @Override
    protected ListenableFuture<?> onInit() {
        // Load initial generation using standardized method
        return load(GENERATION_KEY, Long.class).whenComplete((loadedGeneration, error) -> {
            if (error != null) {
                System.err.println(id + ": Failed to load generation: " + error.getMessage());
                return;
            }

            if (loadedGeneration != null) {
                generation = loadedGeneration;
            }
        });
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
        startElectionFor(clientMessage);
    }

    private void startElectionFor(Message clientMessage) {
        long nextGeneration = bumpGeneration(Math.max(generation, highestProposal));
        highestProposal = nextGeneration;
        System.out.println(id + ": Received NextGenerationRequest, starting election for generation " + nextGeneration);
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
            return;
        }

        attemptElectionFor(proposedGeneration, attempt, clientMessage);
    }

    private void attemptElectionFor(long proposedGeneration, int attempt, Message clientMessage) {
        logElectionAttempt(proposedGeneration, attempt);

        tryProposal(proposedGeneration)
                .map(result -> rejectIfObsolete(proposedGeneration, result))
                .andThen(result -> {
                    logWonElection(proposedGeneration, result);
                    return respondToClient(clientMessage, new NextGenerationResponse(proposedGeneration));
                })
                .whenComplete((result, error) -> {
                    if (error != null && !(error instanceof ObsoleteProposalException)) {
                        tryNextGeneration(proposedGeneration, attempt, clientMessage);
                    }
                });

        System.out.println(id + ": Broadcast PREPARE for generation " + proposedGeneration +
                " to " + clusterSize() + " nodes");
    }

    private Map<ProcessId, PrepareResponse> rejectIfObsolete(long proposedGeneration, Map<ProcessId, PrepareResponse> result) {
        if (isObsoleteProposal(proposedGeneration)) {
            throw new ObsoleteProposalException(proposedGeneration);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private ListenableFuture<Map<ProcessId, PrepareResponse>> tryProposal(long proposedGeneration) {
        var prepareReq = new PrepareRequest(proposedGeneration);
        var ack = new ElectionQuorumCallback(clusterSize(), proposedGeneration);
        for (ProcessId node : getAllNodes()) {
            String correlationId = idGen.generateCorrelationId("internal");
            waitingList.add(correlationId, (RequestCallback<Object>) (RequestCallback<?>) ack);
            send(createMessage(node, correlationId, prepareReq, GenerationMessageTypes.PREPARE_REQUEST));
        }
        return ack.getQuorumFuture();
    }

    private void logWonElection(long proposedGeneration, Map<ProcessId, PrepareResponse> responses) {
        // Quorum reached - election WON!
        System.out.println(id + ": Election WON for generation " + proposedGeneration +
                " (quorum: " + responses.size() + "/" + clusterSize() + ")");
    }

    private void logElectionAttempt(long proposedGeneration, int attempt) {
        System.out.println(id + ": Election attempt " + (attempt + 1) + ", proposing generation " + proposedGeneration);
    }

    private static boolean attemptsExhausted(int attempt) {
        return attempt >= MAX_ELECTION_ATTEMPTS;
    }

    private void tryNextGeneration(long lastTriedGeneration, int attempt, Message clientMessage) {
        // Quorum failed - election LOST, retry with higher generation
        long nextGeneration = bumpGeneration(Math.max(lastTriedGeneration, highestProposal));
        highestProposal = nextGeneration;
        System.out.println(id + ": Election LOST for generation " + lastTriedGeneration + ", retrying with " + nextGeneration);

        // Retry with next generation number
        runElection(nextGeneration, attempt + 1, clientMessage);
    }

    private boolean isObsoleteProposal(long proposedGeneration) {
        return proposedGeneration != highestProposal;
    }


    private static boolean isPromiseFor(PrepareResponse response, long proposedGeneration) {
        return response != null
                && response.promised()
                && response.currentGeneration() == proposedGeneration;
    }

    private int clusterSize() {
        return getAllNodes().size();
    }

    private static NextGenerationResponse failureResponse() {
        return new NextGenerationResponse(0);
    }

    private ListenableFuture<Void> respondToClient(Message clientMessage, NextGenerationResponse body) {
        System.out.println(id + ": Responding to client " + clientMessage.source() + " with generation " + body.generation() + " correlationId=" + clientMessage.correlationId() + " highestProposal=" + highestProposal);
        var responseMsg = new Message(
                id,
                clientMessage.source(),
                PeerType.SERVER,
                GenerationMessageTypes.NEXT_GENERATION_RESPONSE,
                messageCodec.encode(body),
                clientMessage.correlationId()
        );
        send(responseMsg);
        return ListenableFuture.completed(null);
    }

    /**
     * Handle PREPARE request from coordinator.
     * Vote by comparing proposed generation with current:
     * - If proposed > current: ACCEPT (update generation, promise)
     * - Otherwise: REJECT
     */
    private void handlePrepareRequest(Message message) {
        PrepareRequest request = deserializePayload(message.payload(), PrepareRequest.class);

        tryAccept(message, request)
                .whenComplete((result, exception) -> sendPrepareResponse(message, Boolean.TRUE.equals(result)));
    }

    private ListenableFuture<Boolean> tryAccept(Message message, PrepareRequest request) {
        final long proposed = request.proposedGeneration();
        // Strict '>' rule: once this node has moved to generation N, no other
        // request may newly claim N again. Late responses for an already-made
        // promise can still arrive at the proposer, but a fresh PREPARE for the
        // same generation must be rejected here.
        if (proposed <= generation) {
            System.out.println(id + ": REJECTED generation " + proposed +
                    " from " + message.source() + " (current: " + generation + ")");
            return completedFuture(false);
        }

        return accept(message, proposed);
    }

    private ListenableFuture<Boolean> accept(Message message, long proposed) {
        this.generation = proposed; // Update locally but respond only after it is persisted.
        // Immediate local update is important because it rejects any new PREPARE
        // requests at this generation while the persist operation is still in flight.
        // Persist first; only then consider it "promised"
        return persist(GENERATION_KEY, proposed).map(success -> {
            if (success) {
                System.out.println(id + ": ACCEPTED generation " + proposed + " from " + message.source());
            } else {
                System.out.println(id + ": PERSIST FAILED for generation " + proposed +
                        " from " + message.source());
            }
            return success;
        });
    }

    private void sendPrepareResponse(Message prepareMessage, boolean promised) {
        var response = new PrepareResponse(promised, generation);
        var responseMsg = createMessage(
                prepareMessage.source(),
                prepareMessage.correlationId(),
                response,
                GenerationMessageTypes.PREPARE_RESPONSE
        );
        send(responseMsg);
    }

    private static ListenableFuture<Boolean> completedFuture(boolean result) {
        ListenableFuture<Boolean> future = new ListenableFuture<>();
        future.complete(result);
        return future;
    }

    /**
     * Handle PREPARE response (vote) from replica.
     * Delegate to quorum callback for counting.
     */
    private void handlePrepareResponse(Message message) {
        PrepareResponse response = deserializePayload(message.payload(), PrepareResponse.class);

        System.out.println(
                id + ": Received vote from " + message.source() + ": " +
                        (response.promised() ? "PROMISE" : "REJECT") +
                        " (gen=" + response.currentGeneration() + ")"
        );

        // Track highest generation seen from rejections for smarter retries
        if (!response.promised() && response.currentGeneration() > highestProposal) {
            highestProposal = response.currentGeneration();
        }

        // Delegate to quorum callback
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }

    /**
     * Get current generation number (for testing/verification).
     */
    public long getGeneration() {
        return generation;
    }

    private static class ObsoleteProposalException extends RuntimeException {
        ObsoleteProposalException(long generation) {
            super("Obsolete proposal: generation " + generation);
        }
    }
}
