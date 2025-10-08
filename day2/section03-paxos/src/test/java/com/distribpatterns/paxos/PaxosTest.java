package com.distribpatterns.paxos;

import com.tickloom.ProcessId;
import com.tickloom.future.ListenableFuture;
import com.tickloom.testkit.Cluster;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static com.tickloom.testkit.ClusterAssertions.assertEventually;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Single Value Paxos demonstrating core consensus scenarios.
 * 
 * Test scenarios:
 * 1. Basic consensus: Single proposer reaches agreement (happy path)
 * 2. Concurrent proposals: Multiple proposers, automatic serialization
 * 3. Set value operation: Key-value store operation through Paxos
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
        
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build(PaxosServer::new)
                .start()) {
            
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
    @DisplayName("Concurrent Proposals: Multiple proposers, automatic serialization")
    void testConcurrentProposals() throws IOException {
        System.out.println("\n=== TEST: Concurrent Proposals ===\n");
        
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build(PaxosServer::new)
                .start()) {
            
            var aliceClient = cluster.newClientConnectedTo(CLIENT_ALICE, ATHENS, PaxosClient::new);
            var bobClient = cluster.newClientConnectedTo(CLIENT_BOB, BYZANTIUM, PaxosClient::new);
            
            // GIVEN: A cluster of 3 nodes
            // WHEN: Two clients propose operations simultaneously to different coordinators
            ListenableFuture<ExecuteResponse> aliceFuture = aliceClient.execute(ATHENS, 
                new SetValueOperation("title", "Microservices"));
            
            // Small delay between requests
            for (int i = 0; i < 10; i++) {
                cluster.tick();
            }
            
            ListenableFuture<ExecuteResponse> bobFuture = bobClient.execute(BYZANTIUM, 
                new SetValueOperation("title", "Distributed Systems"));
            
            // Wait for both operations to complete (or timeout)
            assertEventually(cluster, () -> 
                (aliceFuture.isCompleted() || aliceFuture.isFailed()) && 
                (bobFuture.isCompleted() || bobFuture.isFailed())
            );
            
            // THEN: At least one should succeed (both could succeed if serialized)
            boolean aliceSuccess = aliceFuture.isCompleted() && !aliceFuture.isFailed() && 
                                  aliceFuture.getResult().success();
            boolean bobSuccess = bobFuture.isCompleted() && !bobFuture.isFailed() && 
                                bobFuture.getResult().success();
            
            assertTrue(aliceSuccess || bobSuccess, "At least one proposal should succeed");
            
            // AND: All nodes should converge on the same value
            PaxosServer athens = getServer(cluster, ATHENS);
            PaxosServer byzantium = getServer(cluster, BYZANTIUM);
            PaxosServer cyrene = getServer(cluster, CYRENE);
            
            String athensValue = athens.getValue("title");
            String byzantiumValue = byzantium.getValue("title");
            String cyreneValue = cyrene.getValue("title");
            
            // At least one node should have a value (the successful one)
            boolean hasValue = athensValue != null || byzantiumValue != null || cyreneValue != null;
            assertTrue(hasValue, "At least one node should have a committed value");
            
            // If nodes have values, they should agree
            if (athensValue != null && byzantiumValue != null) {
                assertEquals(athensValue, byzantiumValue, "Athens and Byzantium should agree");
            }
            if (athensValue != null && cyreneValue != null) {
                assertEquals(athensValue, cyreneValue, "Athens and Cyrene should agree");
            }
            
            // The agreed value should be one of the two proposed values
            String agreedValue = athensValue != null ? athensValue : (byzantiumValue != null ? byzantiumValue : cyreneValue);
            if (agreedValue != null) {
                assertTrue(agreedValue.equals("Microservices") || agreedValue.equals("Distributed Systems"),
                    "Agreed value should be one of the proposed values");
                System.out.println("Final agreed value: " + agreedValue);
            }
            
            System.out.println("\n=== Test passed: Concurrent proposals handled correctly ===\n");
        }
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
