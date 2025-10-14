package com.distribpatterns.paxos;

import clojure.lang.Compiler;
import com.tickloom.ProcessId;
import com.tickloom.future.ListenableFuture;
import com.tickloom.storage.VersionedValue;
import com.tickloom.testkit.Cluster;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static com.tickloom.testkit.ClusterAssertions.*;
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

    @Test
    @DisplayName("Basic Consensus: Single proposer reaches consensus (happy path)")
    void testBasicConsensus() throws IOException {
        System.out.println("\n=== TEST: Basic Consensus ===\n");

        try (var cluster = Cluster.createSimulated(List.of(ATHENS, BYZANTIUM, CYRENE), PaxosServer::new)) {

            var client = cluster.newClientConnectedTo(CLIENT_ALICE, ATHENS, PaxosClient::new);

            // GIVEN: A cluster of 3 nodes (athens, byzantium, cyrene)
            // WHEN: Athens proposes an increment operation
            ListenableFuture<ExecuteResponse> future = client.execute(ATHENS,
                    new IncrementCounterOperation("counter"));

            // Wait for operation to complete
            assertEventually(cluster, () -> future.isCompleted() && !future.isFailed());

            // THEN: Consensus is reached
            var response = future.getResult();
            assertTrue(response.success(), "Operation should succeed");
            assertEquals("1", response.result(), "Counter should be 1");

            // AND: All nodes should have the same value
            PaxosServer athens = getServer(cluster, ATHENS);
            PaxosServer byzantium = getServer(cluster, BYZANTIUM);
            PaxosServer cyrene = getServer(cluster, CYRENE);

            assertEquals(1, athens.getCounter("counter"), "Athens should have counter=1");
            assertEquals(1, byzantium.getCounter("counter"), "Byzantium should have counter=1");
            assertEquals(1, cyrene.getCounter("counter"), "Cyrene should have counter=1");

            // AND: All nodes should have committed the value
            assertNotNull(athens.getState().committedValue(), "Athens should have committed");
            assertNotNull(byzantium.getState().committedValue(), "Byzantium should have committed");
            assertNotNull(cyrene.getState().committedValue(), "Cyrene should have committed");

            System.out.println("\n=== Test passed: Basic consensus achieved ===\n");
        }
    }

    @Test
    @DisplayName("Concurrent Proposals: Multiple proposers, Only one succeeds")
    void testConcurrentProposals() throws IOException {
        System.out.println("\n=== TEST: Concurrent Proposals ===\n");

        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build(PaxosServer::new)
                .start()) {

            assertEventually(cluster, () ->
                    getProcess(cluster, ATHENS).isInitialised() && getProcess(cluster, BYZANTIUM).isInitialised() && getProcess(cluster, CYRENE).isInitialised());


            var aliceClient = cluster.newClientConnectedTo(CLIENT_ALICE, ATHENS, PaxosClient::new);
            var bobClient = cluster.newClientConnectedTo(CLIENT_BOB, BYZANTIUM, PaxosClient::new);

            // GIVEN: A cluster of 3 nodes
            // WHEN: Two clients propose operations simultaneously to different coordinators
            ListenableFuture<ExecuteResponse> aliceFuture = aliceClient.execute(ATHENS,
                    new SetValueOperation("title", "Microservices"));

            ListenableFuture<ExecuteResponse> bobFuture = bobClient.execute(BYZANTIUM,
                    new SetValueOperation("title", "Distributed Systems"));

            assertEventually(cluster, () -> aliceFuture.isCompleted() && bobFuture.isCompleted());
            //only one request succeeds. After the successful request commits,
            // the other request gets the value from the execution of commited operation.
            assertOnlyOneSucceeds(bobFuture, aliceFuture);
            String aliceValue = aliceFuture.getResult().result();
            String bobValue = bobFuture.getResult().result();
            assertEquals(aliceValue, bobValue);
            //Both alice and bob get the same value back. Only one request out of the two is actually successful,
            //the other gets the value from the execution of commited operation.

            assertEventually(cluster, () -> {
                return List.of(ATHENS, BYZANTIUM, CYRENE).stream().anyMatch(pid -> {
                    PaxosServer server = getServer(cluster, pid);
                    String title = server.getValue("title");
                    return title != null && title.equals(aliceValue);
                });
            });
        }
    }

    private static void assertOnlyOneSucceeds(ListenableFuture<ExecuteResponse> bobFuture, ListenableFuture<ExecuteResponse> aliceFuture) {
        ExecuteResponse aliceResult = aliceFuture.getResult();
        ExecuteResponse bobResult = bobFuture.getResult();
        assertTrue((bobResult.success() || aliceResult.success()) && !(bobFuture.getResult().success() && aliceResult.success()));
    }

    private static PaxosServer getProcess(Cluster cluster, ProcessId processId) {
        return (PaxosServer) cluster.getProcess(processId);
    }

    @Test
    @DisplayName("Set Value Operation: Key-value store through Paxos consensus")
    void testSetValueOperation() throws IOException {
        System.out.println("\n=== TEST: Set Value Operation ===\n");

        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build(PaxosServer::new)
                .start()) {

            var client = cluster.newClientConnectedTo(CLIENT_ALICE, ATHENS, PaxosClient::new);

            // WHEN: Client sets a key-value pair
            ListenableFuture<ExecuteResponse> future = client.execute(ATHENS,
                    new SetValueOperation("author", "Martin"));

            // Wait for operation to complete
            assertEventually(cluster, () -> future.isCompleted() && !future.isFailed());

            // THEN: Operation succeeds
            var response = future.getResult();
            assertTrue(response.success(), "Operation should succeed");
            assertEquals("Martin", response.result(), "Result should match the set value");

            // AND: All nodes should have the value
            PaxosServer athens = getServer(cluster, ATHENS);
            PaxosServer byzantium = getServer(cluster, BYZANTIUM);
            PaxosServer cyrene = getServer(cluster, CYRENE);

            assertEquals("Martin", athens.getValue("author"));
            assertEquals("Martin", byzantium.getValue("author"));
            assertEquals("Martin", cyrene.getValue("author"));

            System.out.println("\n=== Test passed: Set value operation ===\n");
        }
    }

    /**
     * Helper to get server instance from cluster.
     */
    private static PaxosServer getServer(Cluster cluster, ProcessId id) {
        return (PaxosServer) cluster.getProcess(id);
    }
}
