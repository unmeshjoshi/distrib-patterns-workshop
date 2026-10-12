package com.distribpatterns.paxoslog;

import com.distribpatterns.paxoslog.messages.PaxosLogMessageTypes;
import com.distribpatterns.paxoslog.messages.PrepareRequest;
import com.distribpatterns.paxoslog.messages.PrepareResponse;
import com.distribpatterns.paxoslog.messages.SetValueOperation;
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
 * One write-ahead log holds the promises, accepts and commits of every log index, and an acceptor replies only once
 * the record is durable. A crash and restart brings back the decisions of every index, in the order they were made.
 */
public class PaxosLogDurabilityTest {
    private static final ProcessId ATHENS = ProcessId.of("athens");
    private static final ProcessId BYZANTIUM = ProcessId.of("byzantium");
    private static final ProcessId CYRENE = ProcessId.of("cyrene");
    private static final ProcessId ALICE = ProcessId.of("alice");
    private static final ProcessId PROPOSER = ProcessId.of("proposer");
    private static final List<ProcessId> ALL = List.of(ATHENS, BYZANTIUM, CYRENE);

    /** Sends a PREPARE for one log index straight to one acceptor, so a test controls the generation. */
    static class PrepareClient extends ClusterClient {
        PrepareClient(List<ProcessId> replicaEndpoints, ProcessParams processParams) {
            super(replicaEndpoints, processParams);
        }

        TickCompletableFuture<PrepareResponse> prepare(ProcessId acceptor, int logIndex, int generation) {
            return sendRequest(new PrepareRequest(logIndex, generation), acceptor, PaxosLogMessageTypes.PREPARE_REQUEST);
        }

        @Override
        protected Map<MessageType, Handler> initialiseHandlers() {
            return Map.of(PaxosLogMessageTypes.PREPARE_RESPONSE, this::handlePrepareResponse);
        }

        private void handlePrepareResponse(Message message) {
            handleResponse(message.correlationId(), deserialize(message.payload(), PrepareResponse.class), message.source());
        }
    }

    private static Cluster newCluster() throws IOException {
        return Cluster.createSimulated(ALL, (peerIds, processParams) -> new PaxosLogServer(peerIds, processParams));
    }

    private static PaxosLogServer server(Cluster cluster, ProcessId id) {
        return cluster.getNode(id);
    }

    private static void crashAndRestart(Cluster cluster, ProcessId id) {
        cluster.crashNode(id);
        cluster.restartNode(id);
        cluster.tickUntil(() -> server(cluster, id).isInitialised());
    }

    @Test
    @DisplayName("Decisions at several indexes and the state machine survive a crash and restart")
    void committedEntriesSurviveACrash() throws IOException {
        try (var cluster = newCluster()) {
            cluster.tickUntil(() -> ALL.stream().allMatch(id -> server(cluster, id).isInitialised()));
            var client = cluster.newClientConnectedTo(ALICE, ATHENS, PaxosLogClient::new);
            assertTrue(cluster.tickUntilComplete(client.execute(ATHENS, new SetValueOperation("title", "Microservices"))).success());
            assertTrue(cluster.tickUntilComplete(client.execute(ATHENS, new SetValueOperation("author", "Martin"))).success());
            cluster.tickUntil(() -> ALL.stream().allMatch(id -> server(cluster, id).getHighWaterMark() == 1
                    && !server(cluster, id).hasDecisionsNotYetDurable()));
            var before = Map.copyOf(server(cluster, BYZANTIUM).getPaxosLog());

            crashAndRestart(cluster, BYZANTIUM);

            PaxosLogServer restarted = server(cluster, BYZANTIUM);
            assertEquals(before, restarted.getPaxosLog(), "the decisions of every index came back from the log");
            assertEquals(1, restarted.getHighWaterMark(), "committed entries are executed again, in order");
            assertEquals("Microservices", restarted.getValue("title"));
            assertEquals("Martin", restarted.getValue("author"));
        }
    }

    @Test
    @DisplayName("A promise for one index is kept after a restart and does not touch another index")
    void aPromiseSurvivesACrashPerIndex() throws IOException {
        try (var cluster = newCluster()) {
            var proposer = cluster.newClient(PROPOSER, PrepareClient::new);
            assertTrue(cluster.tickUntilComplete(proposer.prepare(BYZANTIUM, 3, 5)).promised());

            crashAndRestart(cluster, BYZANTIUM);

            assertFalse(cluster.tickUntilComplete(proposer.prepare(BYZANTIUM, 3, 4)).promised(), "lower generation rejected");
            assertTrue(cluster.tickUntilComplete(proposer.prepare(BYZANTIUM, 4, 4)).promised(), "other index unaffected");
            assertTrue(cluster.tickUntilComplete(proposer.prepare(BYZANTIUM, 3, 6)).promised());
        }
    }

    @Test
    @DisplayName("No promise is answered before it is durable")
    void noPromiseIsAnsweredBeforeItIsDurable() throws IOException {
        try (var cluster = newCluster()) {
            var proposer = cluster.newClient(PROPOSER, PrepareClient::new);
            TickCompletableFuture<PrepareResponse> promise = proposer.prepare(BYZANTIUM, 0, 5);

            cluster.tickUntil(() -> server(cluster, BYZANTIUM).hasDecisionsNotYetDurable());
            assertTrue(promise.isPending(), "no reply while the record can still be lost");

            crashAndRestart(cluster, BYZANTIUM);
            for (int i = 0; i < 100; i++) cluster.tick();

            assertTrue(promise.isPending(), "the promise was never answered");
            assertNull(server(cluster, BYZANTIUM).getLogEntry(0), "the unrecorded promise is gone, and nobody was told");
        }
    }
}
