package com.distribpatterns.paxos;

import com.distribpatterns.paxos.messages.ExecuteResponse;
import com.tickloom.ProcessId;
import com.tickloom.future.ListenableFuture;
import com.tickloom.testkit.Cluster;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Single Value Paxos demonstrating core consensus scenarios.
 * <p>
 * Test scenarios:
 * 1. Basic consensus: Single proposer reaches agreement (happy path)
 * 2. Concurrent proposals: Multiple proposers, automatic serialization
 * 3. Set value operation: Key-value store operation through Paxos
 * <p>
 * NOTE: This test suite focuses on demonstrating the basic Paxos mechanism with
 * simple scenarios. More comprehensive testing including failure scenarios,
 * network partitions, retry mechanisms, and edge cases are covered in the
 * advanced implementations:
 * - MultiPaxosWithHeartbeatsTest: Tests replicated log with automatic leader election
 * - RaftTest: Tests production-ready consensus with comprehensive failure handling
 * <p>
 * The single-value Paxos here serves as a foundation to understand the core
 * three-phase protocol before moving to the more practical replicated log variants.
 */
public class PaxosTest {

    // Ancient Greek city-states representing distributed nodes
    private static final ProcessId ATHENS = ProcessId.of("athens");
    private static final ProcessId BYZANTIUM = ProcessId.of("byzantium");
    private static final ProcessId CYRENE = ProcessId.of("cyrene");

    // Clients
    private static final ProcessId CLIENT_ALICE = ProcessId.of("alice");
    private static final ProcessId CLIENT_BOB = ProcessId.of("bob");
    private static final String COUNTER_KEY = "counter";
    private static final String TITLE_KEY = "title";
    private static final String AUTHOR_KEY = "author";

    @Test
    @DisplayName("Basic Consensus: Single proposer reaches consensus (happy path)")
    void testBasicConsensus() throws IOException {
        try (var cluster = Cluster.createSimulated(List.of(ATHENS, BYZANTIUM, CYRENE), (peerIds, processParams) -> new PaxosServer(peerIds, processParams))) {
            waitUntilAllNodesInitialised(cluster);

            var client = cluster.newClientConnectedTo(CLIENT_ALICE, ATHENS, PaxosClient::new);

            ExecuteResponse response = cluster.tickUntilComplete(client.execute(
                    ATHENS,
                    new IncrementCounterOperation(COUNTER_KEY)
            ));
            assertTrue(response.success(), "Operation should succeed");
            assertEquals("1", response.result(), "Counter should be 1");

            assertCounterOnAllReplicas(cluster, COUNTER_KEY, 1);
            assertCommittedValueOnAllReplicas(cluster);
        }
    }

    @Test
    @DisplayName("Concurrent Proposals: Multiple proposers converge on one chosen value")
    void testConcurrentProposals() throws IOException {
        try (var cluster = Cluster.createSimulated(List.of(ATHENS, BYZANTIUM, CYRENE), (peerIds, processParams) -> new PaxosServer(peerIds, processParams))) {
            waitUntilAllNodesInitialised(cluster);

            var aliceClient = cluster.newClientConnectedTo(CLIENT_ALICE, ATHENS, PaxosClient::new);
            var bobClient = cluster.newClientConnectedTo(CLIENT_BOB, BYZANTIUM, PaxosClient::new);

            ListenableFuture<ExecuteResponse> aliceFuture = aliceClient.execute(ATHENS,
                    new SetValueOperation(TITLE_KEY, "Microservices"));

            ListenableFuture<ExecuteResponse> bobFuture = bobClient.execute(BYZANTIUM,
                    new SetValueOperation(TITLE_KEY, "Distributed Systems"));

            cluster.tickUntilComplete(aliceFuture);
            cluster.tickUntilComplete(bobFuture);

            assertExactlyOneClientRequestSucceeds(aliceFuture, bobFuture);
            String aliceValue = aliceFuture.getResult().result();
            String bobValue = bobFuture.getResult().result();
            assertEquals(aliceValue, bobValue, "Both clients should observe the same chosen value");

            cluster.tickUntil(() -> allReplicasHaveValue(cluster, TITLE_KEY, aliceValue));
        }
    }

    private static void assertExactlyOneClientRequestSucceeds(ListenableFuture<ExecuteResponse> aliceFuture,
                                                              ListenableFuture<ExecuteResponse> bobFuture) {
        ExecuteResponse aliceResult = aliceFuture.getResult();
        ExecuteResponse bobResult = bobFuture.getResult();
        boolean aliceSucceeded = aliceResult.success();
        boolean bobSucceeded = bobResult.success();

        assertTrue(aliceSucceeded ^ bobSucceeded,
                "Exactly one client request should be chosen as the successful proposal");
    }

    @Test
    @DisplayName("Set Value Operation: Key-value store through Paxos consensus")
    void testSetValueOperation() throws IOException {
        try (var cluster = Cluster.createSimulated(List.of(ATHENS, BYZANTIUM, CYRENE), (peerIds, processParams) -> new PaxosServer(peerIds, processParams))) {
            waitUntilAllNodesInitialised(cluster);

            var client = cluster.newClientConnectedTo(CLIENT_ALICE, ATHENS, PaxosClient::new);

            ExecuteResponse response = cluster.tickUntilComplete(client.execute(
                    ATHENS,
                    new SetValueOperation(AUTHOR_KEY, "Martin")
            ));
            assertTrue(response.success(), "Operation should succeed");
            assertEquals("Martin", response.result(), "Result should match the set value");

            assertValueOnAllReplicas(cluster, AUTHOR_KEY, "Martin");
        }
    }

    private static void waitUntilAllNodesInitialised(Cluster cluster) {
        cluster.tickUntil(() ->
                getServer(cluster, ATHENS).isInitialised()
                        && getServer(cluster, BYZANTIUM).isInitialised()
                        && getServer(cluster, CYRENE).isInitialised());
    }

    private static boolean allReplicasHaveValue(Cluster cluster, String key, String expectedValue) {
        return List.of(ATHENS, BYZANTIUM, CYRENE).stream()
                .allMatch(processId -> expectedValue.equals(getServer(cluster, processId).getValue(key)));
    }

    private static void assertCounterOnAllReplicas(Cluster cluster, String key, int expectedValue) {
        assertEquals(expectedValue, getServer(cluster, ATHENS).getCounter(key), "Athens should have the expected counter value");
        assertEquals(expectedValue, getServer(cluster, BYZANTIUM).getCounter(key), "Byzantium should have the expected counter value");
        assertEquals(expectedValue, getServer(cluster, CYRENE).getCounter(key), "Cyrene should have the expected counter value");
    }

    private static void assertValueOnAllReplicas(Cluster cluster, String key, String expectedValue) {
        assertEquals(expectedValue, getServer(cluster, ATHENS).getValue(key));
        assertEquals(expectedValue, getServer(cluster, BYZANTIUM).getValue(key));
        assertEquals(expectedValue, getServer(cluster, CYRENE).getValue(key));
    }

    private static void assertCommittedValueOnAllReplicas(Cluster cluster) {
        assertTrue(getServer(cluster, ATHENS).getState().committedValue().isPresent(), "Athens should have committed");
        assertTrue(getServer(cluster, BYZANTIUM).getState().committedValue().isPresent(), "Byzantium should have committed");
        assertTrue(getServer(cluster, CYRENE).getState().committedValue().isPresent(), "Cyrene should have committed");
    }

    private static PaxosServer getServer(Cluster cluster, ProcessId id) {
        return cluster.getNode(id);
    }
}
