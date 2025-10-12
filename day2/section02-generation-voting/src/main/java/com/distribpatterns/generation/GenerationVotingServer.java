package com.distribpatterns.generation;

import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.Replica;
import com.tickloom.future.ListenableFuture;
import com.tickloom.messaging.*;
import com.tickloom.network.PeerType;
import com.tickloom.storage.Storage;
import com.tickloom.storage.VersionedValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
     *        • YugabyteDB / DocDB (Raft log is the WAL; RocksDB WAL disabled)
     *
     * B) RocksDB-backed storage (KV-as-log; rely on RocksDB’s own WAL)
     *    - Store generation under a fixed key (e.g., GEN_KEY) as big-endian 64-bit.
     *    - If batching other state, write together in one WriteBatch with sync=true so
     *      they become durable in the same crash cut (RocksDB WAL handles journaling/fsync).
     *    - Examples:
     *        • TiKV (older releases: separate RocksDB “raftdb” for Raft log)
     *        • CockroachDB (historical, pre-Pebble: Raft log/state in RocksDB)
     *
     * Invariant (critical):
     *   - Generation must be durable and monotonic across restarts.
     *   - If you persist both entries and generation, make them durable together (same fsync
     *     boundary). Avoid splitting across different durability domains without strict
     *     ordering and recovery rules.
     *
     * This code currently uses option B for simplicity. Swap to option A by routing load/store
     * to your own WAL implementation.
     */

    private long generation = 0L;

    // Maximum retry attempts for failed elections
    private static final int MAX_ELECTION_ATTEMPTS = 5;

    //Move this to tickloom. Each process needs some signal to know it is ready to accept requests
    //This is particularly true for implementations like Paxos and Raft where we need to make decisions
    //based on the persisted state. We can always just work with the storage always.. but thats not optimal.
    //moreover, if storage is used just as a WAL, we need to construct in-memory state on initialization.
    private boolean isInitialised = false;
    // If you have storage getters, load here. Example (uncomment if available):
// this.generation = storage.readLong("generation", 0L);
    private byte[] GENERATION_KEY = "generation".getBytes();

    public GenerationVotingServer(List<ProcessId> peerIds, Storage storage, ProcessParams params) {
        super(peerIds, storage, params);
        load(GENERATION_KEY, storage);
    }


    //TODO: We can extract GenerationStateStore class which manages generation state.
    // We will have it for more complex set of implementations like Paxos and Raft.
    private void load(byte[] generationKey, Storage storage) {
        storage.get(generationKey).handle((response, exception) -> {
            if (exception == null) {
                if (response != null) {
                    this.generation = beToLong(response.value(), 0L);
                }
                isInitialised = true; //Mark process as initialised to handle client requests.
                return;
            }
            System.out.println("Error reading generation = " + exception.getMessage());
        });
    }


    private ListenableFuture<Boolean> store(byte[] generationKey, long finalProposedGeneration, Storage storage) {
        // Idempotent commit
        //TODO: We do not need a versionedvalue here
        return storage.set(generationKey, new VersionedValue(longToBE(finalProposedGeneration), 1)).andThen((success, error)->{
            if (error == null) {
                generation = finalProposedGeneration;
            }
        });

    }

    public boolean isInitialised() {
        return isInitialised;
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

        ListenableFuture<Boolean> promised = tryAccept(message, request);

        promised.handle((result, exception) -> {
            // Send vote back to coordinator
            var response = new PrepareResponse(result);
            var responseMsg = createMessage(
                    message.source(),
                    message.correlationId(),
                    response,
                    GenerationMessageTypes.PREPARE_RESPONSE
            );
            send(responseMsg);

        });

    }

    private ListenableFuture<Boolean> tryAccept(Message message, PrepareRequest request) {
        final long proposed = request.proposedGeneration();
        // Strict '>' rule: fast reject as an already-completed future
        if (proposed <= generation) {
            System.out.println(id + ": REJECTED generation " + proposed +
                    " from " + message.source() + " (current: " + generation + ")");
            ListenableFuture result = new ListenableFuture();
            result.complete(false);
            return result;
        }

        return accept(message, proposed);
    }

    private ListenableFuture accept(Message message, long proposed) {
        ListenableFuture result = new ListenableFuture();
        // Persist first; only then consider it "promised"
        store(GENERATION_KEY, proposed, storage).handle((ok, err) -> {
            if (err == null && Boolean.TRUE.equals(ok)) {
                // If storeGeneration doesn't update in-memory, do it here:
                // this.generation = proposed;
                System.out.println(id + ": ACCEPTED generation " + proposed + " from " + message.source());
                result.complete(true);
            } else {
                System.out.println(id + ": PERSIST FAILED for generation " + proposed +
                        " from " + message.source() + " (err=" + (err != null ? err : "unknown") + ")");
                result.complete(false);
            }
        });
        return result;
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


    private static byte[] longToBE(long v) {
        return ByteBuffer.allocate(Long.BYTES).order(ByteOrder.BIG_ENDIAN).putLong(v).array();
    }

    private static long beToLong(byte[] b, long def) {
        if (b == null) return def;
        if (b.length != Long.BYTES) throw new IllegalStateException("corrupt generation payload len=" + b.length);
        return ByteBuffer.wrap(b).order(ByteOrder.BIG_ENDIAN).getLong();
    }
}
