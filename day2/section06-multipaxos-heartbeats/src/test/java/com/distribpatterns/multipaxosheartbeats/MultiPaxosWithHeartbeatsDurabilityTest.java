package com.distribpatterns.multipaxosheartbeats;

import com.distribpatterns.multipaxosheartbeats.messages.FullLogPrepareRequest;
import com.distribpatterns.multipaxosheartbeats.messages.FullLogPrepareResponse;
import com.distribpatterns.multipaxosheartbeats.messages.MultiPaxosMessageTypes;
import com.distribpatterns.multipaxosheartbeats.messages.SetValueOperation;
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
 * The promise, which covers the whole log, and every accept and commit go into one write-ahead log. An acceptor
 * replies only once the record is durable, so a restart never forgets a promise or an accept it has told anyone about.
 */
public class MultiPaxosWithHeartbeatsDurabilityTest {
    private static final ProcessId ATHENS = ProcessId.of("athens");
    private static final ProcessId BYZANTIUM = ProcessId.of("byzantium");
    private static final ProcessId CYRENE = ProcessId.of("cyrene");
    private static final ProcessId ALICE = ProcessId.of("alice");
    private static final ProcessId CANDIDATE = ProcessId.of("candidate");
    private static final List<ProcessId> ALL = List.of(ATHENS, BYZANTIUM, CYRENE);

    /** Sends a FULL_LOG_PREPARE straight to one acceptor, so a test controls the generation. */
    static class PrepareClient extends ClusterClient {
        PrepareClient(List<ProcessId> replicaEndpoints, ProcessParams processParams) {
            super(replicaEndpoints, processParams);
        }

        TickCompletableFuture<FullLogPrepareResponse> prepare(ProcessId acceptor, int generation) {
            return sendRequest(new FullLogPrepareRequest(generation), acceptor, MultiPaxosMessageTypes.FULL_LOG_PREPARE_REQUEST);
        }

        @Override
        protected Map<MessageType, Handler> initialiseHandlers() {
            return Map.of(MultiPaxosMessageTypes.FULL_LOG_PREPARE_RESPONSE, this::handlePrepareResponse);
        }

        private void handlePrepareResponse(Message message) {
            handleResponse(message.correlationId(), deserialize(message.payload(), FullLogPrepareResponse.class), message.source());
        }
    }

    private static Cluster newCluster() throws IOException {
        return Cluster.createSimulated(ALL, (peerIds, processParams) -> new MultiPaxosWithHeartbeatsServer(peerIds, processParams));
    }

    private static MultiPaxosWithHeartbeatsServer server(Cluster cluster, ProcessId id) {
        return cluster.getNode(id);
    }

    private static void crashAndRestart(Cluster cluster, ProcessId id) {
        cluster.crashNode(id);
        cluster.restartNode(id);
        cluster.tickUntil(() -> server(cluster, id).isInitialised());
    }

    @Test
    @DisplayName("A follower gets its promise, its log and its state machine back after a crash and restart")
    void aFollowerRecoversPromiseLogAndState() throws IOException {
        try (var cluster = newCluster()) {
            cluster.tickUntil(() -> ALL.stream().allMatch(id -> server(cluster, id).isInitialised()));
            // The first node whose election timeout fires becomes the leader.
            cluster.tickUntil(() -> ALL.stream().anyMatch(id -> server(cluster, id).isLeader()));
            ProcessId leader = ALL.stream().filter(id -> server(cluster, id).isLeader()).findFirst().orElseThrow();
            var client = cluster.newClientConnectedTo(ALICE, leader, MultiPaxosWithHeartbeatsClient::new);
            assertTrue(cluster.tickUntilComplete(client.execute(leader, new SetValueOperation("title", "Microservices"))).success());
            assertTrue(cluster.tickUntilComplete(client.execute(leader, new SetValueOperation("author", "Martin"))).success());
            cluster.tickUntil(() -> ALL.stream().allMatch(id -> server(cluster, id).getHighWaterMark() == 1
                    && !server(cluster, id).hasDecisionsNotYetDurable()));
            ProcessId follower = ALL.stream().filter(id -> !id.equals(leader)).findFirst().orElseThrow();
            var logBefore = Map.copyOf(server(cluster, follower).getPaxosLog());
            int promiseBefore = server(cluster, follower).getPromisedGeneration();
            assertTrue(promiseBefore > 0);

            crashAndRestart(cluster, follower);

            MultiPaxosWithHeartbeatsServer restarted = server(cluster, follower);
            assertEquals(promiseBefore, restarted.getPromisedGeneration());
            assertEquals(logBefore, restarted.getPaxosLog());
            assertEquals(1, restarted.getHighWaterMark());
            assertEquals("Microservices", restarted.getValue("title"));
            assertEquals("Martin", restarted.getValue("author"));
        }
    }

    @Test
    @DisplayName("A promise to a leader is kept after a restart, so an older leader cannot get it back")
    void aPromiseSurvivesACrash() throws IOException {
        try (var cluster = newCluster()) {
            var candidate = cluster.newClient(CANDIDATE, PrepareClient::new);
            assertTrue(cluster.tickUntilComplete(candidate.prepare(BYZANTIUM, 5)).promised());

            crashAndRestart(cluster, BYZANTIUM);

            assertEquals(5, server(cluster, BYZANTIUM).getPromisedGeneration());
            assertFalse(cluster.tickUntilComplete(candidate.prepare(BYZANTIUM, 4)).promised());
            assertFalse(cluster.tickUntilComplete(candidate.prepare(BYZANTIUM, 5)).promised());
            assertTrue(cluster.tickUntilComplete(candidate.prepare(BYZANTIUM, 6)).promised());
        }
    }

    @Test
    @DisplayName("No promise is answered before it is durable")
    void noPromiseIsAnsweredBeforeItIsDurable() throws IOException {
        try (var cluster = newCluster()) {
            var candidate = cluster.newClient(CANDIDATE, PrepareClient::new);
            TickCompletableFuture<FullLogPrepareResponse> promise = candidate.prepare(BYZANTIUM, 5);

            cluster.tickUntil(() -> server(cluster, BYZANTIUM).hasDecisionsNotYetDurable());
            assertTrue(promise.isPending(), "no reply while the record can still be lost");

            crashAndRestart(cluster, BYZANTIUM);
            for (int i = 0; i < 100; i++) cluster.tick();

            // The nodes keep holding elections of their own, so the promised generation says nothing here.
            // What matters is that the candidate was never told about a promise that could be lost.
            assertTrue(promise.isPending(), "the promise was never answered");
        }
    }

    @Test
    @DisplayName("An election overtaken by a higher promise does not make the candidate leader")
    void anOvertakenElectionDoesNotLowerThePromise() throws IOException {
        try (var cluster = newCluster()) {
            cluster.tickUntil(cluster::areAllNodesInitialized);
            MultiPaxosWithHeartbeatsServer athens = server(cluster, ATHENS);
            // Athens's election can complete only through byzantium, whose promise arrives late.
            cluster.partitionOneWay(ATHENS, CYRENE);
            cluster.setNetworkDelay(BYZANTIUM, ATHENS, 30);

            athens.startElection();
            cluster.tickUntil(() -> athens.getPromisedGeneration() == 1);

            // While the election runs, athens promises a higher generation to another candidate.
            var candidate = cluster.newClient(CANDIDATE, PrepareClient::new);
            assertTrue(cluster.tickUntilComplete(candidate.prepare(ATHENS, 5)).promised());

            for (int i = 0; i < 50; i++) cluster.tick();   // byzantium's late promise completes athens's election

            assertFalse(athens.isLeader(), "leading at generation 1 would break the promise to generation 5");
            assertEquals(5, athens.getPromisedGeneration());
        }
    }
}
