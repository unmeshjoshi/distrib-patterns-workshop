package com.distribpatterns.multipaxosheartbeats;

import com.distribpatterns.multipaxosheartbeats.messages.ExecuteCommandResponse;
import com.distribpatterns.multipaxosheartbeats.messages.SetValueOperation;
import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Multi-Paxos with automatic heartbeat-based leader election.
 */
public class MultiPaxosWithHeartbeatsTest {

    private static final ProcessId ATHENS = ProcessId.of("athens");
    private static final ProcessId BYZANTIUM = ProcessId.of("byzantium");
    private static final ProcessId CYRENE = ProcessId.of("cyrene");
    private static final List<ProcessId> REPLICAS = List.of(ATHENS, BYZANTIUM, CYRENE);

    private static final ProcessId CLIENT_ALICE = ProcessId.of("alice");
    private static final ProcessId CLIENT_BOB = ProcessId.of("bob");
    private static final String TITLE_KEY = "title";
    private static final String AUTHOR_KEY = "author";

    @Test
    @DisplayName("Automatic Leader Election: Heartbeat timeout triggers election")
    void testAutomaticLeaderElection() throws IOException {
        try (var cluster = new Cluster()
            .withProcessIds(REPLICAS)
            .useSimulatedNetwork()
            .build((peerIds, processParams) -> new MultiPaxosWithHeartbeatsServer(peerIds, processParams))
            .start()) {

            waitUntilAllNodesInitialised(cluster);

            ProcessId leaderId = waitUntilLeaderElected(cluster);

            assertNotNull(leaderId, "A leader should be elected automatically");
            assertEquals(1, leaderCount(cluster), "Exactly one leader should be elected");
        }
    }

    @Test
    @DisplayName("Automatic Heartbeats: Leader remains stable while heartbeats flow")
    void testAutomaticHeartbeats() throws IOException {
        try (var cluster = new Cluster()
            .withProcessIds(REPLICAS)
            .useSimulatedNetwork()
            .build((peerIds, processParams) -> new MultiPaxosWithHeartbeatsServer(peerIds, processParams))
            .start()) {

            waitUntilAllNodesInitialised(cluster);

            ProcessId initialLeader = waitUntilLeaderElected(cluster);
            tickFor(cluster, 50);

            assertEquals(initialLeader, currentLeaderId(cluster), "Heartbeats should keep the same leader in place");
            assertEquals(1, leaderCount(cluster), "Heartbeats should prevent a second election");
        }
    }

    @Test
    @DisplayName("Leader Replication: Leader replicates values with automatic heartbeats")
    void testLeaderReplication() throws IOException {
        try (var cluster = new Cluster()
            .withProcessIds(REPLICAS)
            .useSimulatedNetwork()
            .build((peerIds, processParams) -> new MultiPaxosWithHeartbeatsServer(peerIds, processParams))
            .start()) {

            waitUntilAllNodesInitialised(cluster);

            ProcessId leaderId = waitUntilLeaderElected(cluster);
            var client = cluster.newClientConnectedTo(CLIENT_ALICE, leaderId, MultiPaxosWithHeartbeatsClient::new);

            ExecuteCommandResponse response = cluster.tickUntilComplete(client.execute(
                leaderId,
                new SetValueOperation(TITLE_KEY, "Microservices")
            ));

            assertSuccessfulCommand(response);
            assertEquals("Microservices", response.result());
            assertValueOnAllReplicas(cluster, TITLE_KEY, "Microservices");
        }
    }

    @Test
    @DisplayName("Stable Leadership: Heartbeats prevent unnecessary elections")
    void testStableLeadership() throws IOException {
        try (var cluster = new Cluster()
            .withProcessIds(REPLICAS)
            .useSimulatedNetwork()
            .build((peerIds, processParams) -> new MultiPaxosWithHeartbeatsServer(peerIds, processParams))
            .start()) {

            waitUntilAllNodesInitialised(cluster);

            ProcessId initialLeader = waitUntilLeaderElected(cluster);
            int initialGeneration = getServer(cluster, initialLeader).getPromisedGeneration();

            tickFor(cluster, 200);

            assertEquals(initialLeader, currentLeaderId(cluster), "The same leader should remain in charge");
            assertEquals(initialGeneration, getServer(cluster, initialLeader).getPromisedGeneration(),
                "Stable heartbeats should avoid a new generation");
        }
    }

    @Test
    @DisplayName("Generation Convergence: All nodes converge to the leader generation")
    void testGenerationConvergence() throws IOException {
        try (var cluster = new Cluster()
            .withProcessIds(REPLICAS)
            .useSimulatedNetwork()
            .build((peerIds, processParams) -> new MultiPaxosWithHeartbeatsServer(peerIds, processParams))
            .start()) {

            waitUntilAllNodesInitialised(cluster);

            ProcessId leaderId = waitUntilLeaderElected(cluster);
            int leaderGeneration = getServer(cluster, leaderId).getPromisedGeneration();

            cluster.tickUntil(() -> allNodesHaveGeneration(cluster, leaderGeneration));

            assertGenerationOnAllReplicas(cluster, leaderGeneration);
        }
    }

    @Test
    @DisplayName("Leader Change: A new leader appends after the committed entries, not over them")
    void testNewLeaderAppendsAfterCommittedEntries() throws IOException {
        try (var cluster = new Cluster()
            .withProcessIds(REPLICAS)
            .useSimulatedNetwork()
            .build((peerIds, processParams) -> new MultiPaxosWithHeartbeatsServer(peerIds, processParams))
            .start()) {

            waitUntilAllNodesInitialised(cluster);

            ProcessId firstLeader = waitUntilLeaderElected(cluster);
            var client = cluster.newClientConnectedTo(CLIENT_ALICE, firstLeader, MultiPaxosWithHeartbeatsClient::new);
            assertSuccessfulCommand(cluster.tickUntilComplete(
                client.execute(firstLeader, new SetValueOperation(TITLE_KEY, "Microservices"))));
            tickFor(cluster, 20);

            cluster.crashNode(firstLeader);
            List<ProcessId> survivors = REPLICAS.stream().filter(id -> !id.equals(firstLeader)).toList();
            cluster.tickUntil(() -> survivors.stream().anyMatch(id -> getServer(cluster, id).isLeader()));
            ProcessId secondLeader = survivors.stream().filter(id -> getServer(cluster, id).isLeader()).findFirst().get();

            var secondClient = cluster.newClientConnectedTo(CLIENT_BOB, secondLeader, MultiPaxosWithHeartbeatsClient::new);
            assertSuccessfulCommand(cluster.tickUntilComplete(
                secondClient.execute(secondLeader, new SetValueOperation(AUTHOR_KEY, "Martin"))));
            tickFor(cluster, 20);

            for (ProcessId id : survivors) {
                assertEquals("Microservices", getServer(cluster, id).getValue(TITLE_KEY));
                assertEquals("Martin", getServer(cluster, id).getValue(AUTHOR_KEY));
            }
        }
    }

    @Test
    @DisplayName("Leader Change: A new leader learns an entry it missed that the others committed")
    void testNewLeaderLearnsCommittedEntryItMissed() throws IOException {
        try (var cluster = new Cluster()
            .withProcessIds(REPLICAS)
            .useSimulatedNetwork()
            .build((peerIds, processParams) -> new MultiPaxosWithHeartbeatsServer(peerIds, processParams))
            .start()) {

            waitUntilAllNodesInitialised(cluster);

            ProcessId firstLeader = waitUntilLeaderElected(cluster);
            List<ProcessId> survivors = REPLICAS.stream().filter(id -> !id.equals(firstLeader)).toList();
            ProcessId lagging = survivors.get(0);
            ProcessId upToDate = survivors.get(1);

            // The lagging node hears nothing from the leader, so the title is committed on the other two only.
            // The write finishes well inside the election timeout, so the lagging node does not start an election.
            cluster.partitionOneWay(firstLeader, lagging);
            var client = cluster.newClientConnectedTo(CLIENT_ALICE, firstLeader, MultiPaxosWithHeartbeatsClient::new);
            assertSuccessfulCommand(cluster.tickUntilComplete(
                client.execute(firstLeader, new SetValueOperation(TITLE_KEY, "Microservices"))));
            assertTrue(getServer(cluster, lagging).getPaxosLog().isEmpty(), "The lagging node should have missed the write");

            cluster.crashNode(firstLeader);
            getServer(cluster, lagging).startElection();
            cluster.tickUntil(() -> getServer(cluster, lagging).isLeader());

            var secondClient = cluster.newClientConnectedTo(CLIENT_BOB, lagging, MultiPaxosWithHeartbeatsClient::new);
            assertSuccessfulCommand(cluster.tickUntilComplete(
                secondClient.execute(lagging, new SetValueOperation(AUTHOR_KEY, "Martin"))));
            tickFor(cluster, 20);

            for (ProcessId id : List.of(lagging, upToDate)) {
                assertEquals("Microservices", getServer(cluster, id).getValue(TITLE_KEY));
                assertEquals("Martin", getServer(cluster, id).getValue(AUTHOR_KEY));
            }
        }
    }

    private static void waitUntilAllNodesInitialised(Cluster cluster) {
        cluster.tickUntil(cluster::areAllNodesInitialized);
    }

    private static ProcessId waitUntilLeaderElected(Cluster cluster) {
        cluster.tickUntil(() -> leaderCount(cluster) == 1);
        ProcessId leaderId = currentLeaderId(cluster);
        assertNotNull(leaderId, "A leader should exist once leader election completes");
        return leaderId;
    }

    private static long leaderCount(Cluster cluster) {
        return REPLICAS.stream()
            .map(id -> getServer(cluster, id))
            .filter(MultiPaxosWithHeartbeatsServer::isLeader)
            .count();
    }

    private static ProcessId currentLeaderId(Cluster cluster) {
        return REPLICAS.stream()
            .filter(id -> getServer(cluster, id).isLeader())
            .findFirst()
            .orElse(null);
    }

    private static boolean allNodesHaveGeneration(Cluster cluster, int expectedGeneration) {
        return Stream.of(ATHENS, BYZANTIUM, CYRENE)
            .allMatch(id -> getServer(cluster, id).getPromisedGeneration() == expectedGeneration);
    }

    private static MultiPaxosWithHeartbeatsServer getServer(Cluster cluster, ProcessId id) {
        return cluster.getNode(id);
    }

    private static void assertSuccessfulCommand(ExecuteCommandResponse response) {
        assertTrue(response.success(), "Command should succeed");
    }

    private static void assertValueOnAllReplicas(Cluster cluster, String key, String expectedValue) {
        assertEquals(expectedValue, getServer(cluster, ATHENS).getValue(key));
        assertEquals(expectedValue, getServer(cluster, BYZANTIUM).getValue(key));
        assertEquals(expectedValue, getServer(cluster, CYRENE).getValue(key));
    }

    private static void assertGenerationOnAllReplicas(Cluster cluster, int expectedGeneration) {
        assertEquals(expectedGeneration, getServer(cluster, ATHENS).getPromisedGeneration());
        assertEquals(expectedGeneration, getServer(cluster, BYZANTIUM).getPromisedGeneration());
        assertEquals(expectedGeneration, getServer(cluster, CYRENE).getPromisedGeneration());
    }

    private static void tickFor(Cluster cluster, int ticks) {
        for (int i = 0; i < ticks; i++) {
            cluster.tick();
        }
    }
}
