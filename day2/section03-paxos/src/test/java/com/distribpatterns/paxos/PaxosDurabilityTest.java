package com.distribpatterns.paxos;

import com.distribpatterns.paxos.messages.PaxosMessageTypes;
import com.distribpatterns.paxos.messages.PrepareRequest;
import com.distribpatterns.paxos.messages.PrepareResponse;
import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.algorithms.replication.ClusterClient;
import com.tickloom.future.TickCompletableFuture;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageType;
import com.tickloom.testkit.Cluster;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Every promise, accept and commit is appended to a write-ahead log, and an acceptor replies only once the record is
 * durable. A crash and restart therefore brings back every decision it has told anyone about, and none it has not.
 */
public class PaxosDurabilityTest {
    private static final ProcessId ATHENS = ProcessId.of("athens");
    private static final ProcessId BYZANTIUM = ProcessId.of("byzantium");
    private static final ProcessId CYRENE = ProcessId.of("cyrene");
    private static final ProcessId ALICE = ProcessId.of("alice");
    private static final ProcessId PROPOSER = ProcessId.of("proposer");

    /** Sends a PREPARE straight to one acceptor, so a test controls the generation. */
    static class PrepareClient extends ClusterClient {
        PrepareClient(List<ProcessId> replicaEndpoints, ProcessParams processParams) {
            super(replicaEndpoints, processParams);
        }

        TickCompletableFuture<PrepareResponse> prepare(ProcessId acceptor, int generation) {
            return sendRequest(new PrepareRequest(generation), acceptor, PaxosMessageTypes.PREPARE_REQUEST);
        }

        @Override
        protected Map<MessageType, Handler> initialiseHandlers() {
            return Map.of(PaxosMessageTypes.PREPARE_RESPONSE, this::handlePrepareResponse);
        }

        private void handlePrepareResponse(Message message) {
            handleResponse(message.correlationId(), deserialize(message.payload(), PrepareResponse.class), message.source());
        }
    }

    private static Cluster newCluster() throws IOException {
        return Cluster.createSimulated(List.of(ATHENS, BYZANTIUM, CYRENE),
                (peerIds, processParams) -> new PaxosServer(peerIds, processParams));
    }

    private static PaxosServer server(Cluster cluster, ProcessId id) {
        return cluster.getNode(id);
    }

    private static void crashAndRestart(Cluster cluster, ProcessId id) {
        cluster.crashNode(id);
        cluster.restartNode(id);
        cluster.tickUntil(() -> server(cluster, id).isInitialised());
    }

    @Test
    @DisplayName("A committed value and the state machine survive a crash and restart of an acceptor")
    void committedStateSurvivesACrash() throws IOException {
        try (var cluster = newCluster()) {
            var client = cluster.newClientConnectedTo(ALICE, ATHENS, PaxosClient::new);
            var response = cluster.tickUntilComplete(client.execute(ATHENS, new IncrementCounterOperation("counter")));
            assertTrue(response.success());
            cluster.tickUntil(() -> List.of(ATHENS, BYZANTIUM, CYRENE).stream()
                    .allMatch(id -> server(cluster, id).getCounter("counter") != null
                            && !server(cluster, id).hasDecisionsNotYetDurable()));
            PaxosState before = server(cluster, BYZANTIUM).getPaxosState();
            assertTrue(before.committedValue().isPresent());

            crashAndRestart(cluster, BYZANTIUM);

            PaxosServer restarted = server(cluster, BYZANTIUM);
            assertEquals(before, restarted.getPaxosState(), "every decision came back from the log");
            assertEquals(1, restarted.getCounter("counter"), "the committed operation is executed again on recovery");
        }
    }

    @Test
    @DisplayName("A promise that was answered is still kept after a crash and restart")
    void aPromiseThatWasAnsweredSurvivesACrash() throws IOException {
        try (var cluster = newCluster()) {
            var proposer = cluster.newClient(PROPOSER, PrepareClient::new);

            assertTrue(cluster.tickUntilComplete(proposer.prepare(BYZANTIUM, 5)).promised());

            crashAndRestart(cluster, BYZANTIUM);

            assertEquals(5, server(cluster, BYZANTIUM).getPaxosState().promisedGeneration());
            PrepareResponse lower = cluster.tickUntilComplete(proposer.prepare(BYZANTIUM, 4));
            assertFalse(lower.promised(), "a lower generation is still rejected");
            assertEquals(5, lower.promisedGeneration());
            assertFalse(cluster.tickUntilComplete(proposer.prepare(BYZANTIUM, 5)).promised(), "the same generation too");
            assertTrue(cluster.tickUntilComplete(proposer.prepare(BYZANTIUM, 6)).promised());
        }
    }

    @Test
    @DisplayName("No promise is answered before it is durable, so a crash leaves nobody holding an unkept promise")
    void noPromiseIsAnsweredBeforeItIsDurable() throws IOException {
        try (var cluster = newCluster()) {
            var proposer = cluster.newClient(PROPOSER, PrepareClient::new);
            TickCompletableFuture<PrepareResponse> promise = proposer.prepare(BYZANTIUM, 5);

            // The acceptor has applied the promise to its state, and the record is still on its way to disk.
            cluster.tickUntil(() -> server(cluster, BYZANTIUM).getPaxosState().promisedGeneration() == 5);
            assertTrue(server(cluster, BYZANTIUM).hasDecisionsNotYetDurable());
            assertTrue(promise.isPending(), "no reply while the record can still be lost");

            crashAndRestart(cluster, BYZANTIUM);
            for (int i = 0; i < 100; i++) cluster.tick();

            assertTrue(promise.isPending(), "the promise was never answered");
            assertEquals(0, server(cluster, BYZANTIUM).getPaxosState().promisedGeneration(),
                    "the unrecorded promise is gone, and nobody was told about it");
            assertTrue(cluster.tickUntilComplete(proposer.prepare(BYZANTIUM, 3)).promised());
        }
    }

    @Test
    @DisplayName("A restarted acceptor with no log starts empty")
    void anAcceptorWithNoDecisionsStartsEmpty() throws IOException {
        try (var cluster = newCluster()) {
            crashAndRestart(cluster, CYRENE);
            PaxosServer restarted = server(cluster, CYRENE);
            assertEquals(new PaxosState(), restarted.getPaxosState());
            assertNull(restarted.getCounter("counter"));
        }
    }
}
