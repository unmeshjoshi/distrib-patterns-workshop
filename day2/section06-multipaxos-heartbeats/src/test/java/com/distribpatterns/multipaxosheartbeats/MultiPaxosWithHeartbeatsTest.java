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
    private static final String TITLE_KEY = "title";

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
            cluster.delayForClusterTicks(50);

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

            cluster.delayForClusterTicks(200);

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
}
