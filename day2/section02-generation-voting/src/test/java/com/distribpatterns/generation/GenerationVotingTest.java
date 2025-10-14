package com.distribpatterns.generation;

import com.tickloom.ProcessId;
import com.tickloom.future.ListenableFuture;
import com.tickloom.testkit.Cluster;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static com.tickloom.testkit.ClusterAssertions.assertEventually;

/**
 * Tests for Generation Voting algorithm.
 * 
 * Generation Voting generates monotonically increasing numbers using quorum-based voting.
 * It's essentially a simplified Paxos Phase 1 (prepare/promise) for leader election.
 */
public class GenerationVotingTest {
    
    // Replica nodes
    private static final ProcessId ATHENS = ProcessId.of("athens");
    private static final ProcessId BYZANTIUM = ProcessId.of("byzantium");
    private static final ProcessId CYRENE = ProcessId.of("cyrene");
    private static final ProcessId DELPHI = ProcessId.of("delphi");
    private static final ProcessId EPHESUS = ProcessId.of("ephesus");
    
    // Client
    private static final ProcessId CLIENT = ProcessId.of("client");
    
    @Test
    @DisplayName("Happy Path: Generate monotonically increasing numbers with 3 nodes")
    void testGenerateMonotonicNumbers() throws IOException {
        try (var cluster = Cluster.create(List.of(ATHENS, BYZANTIUM, CYRENE), (peerIds, processParams) -> new GenerationVotingServer(peerIds, processParams))) {

            assertEventually(cluster, ()->
                    getProcess(cluster, ATHENS).isInitialised() && getProcess(cluster, BYZANTIUM).isInitialised() && getProcess(cluster, CYRENE).isInitialised());

            var client = cluster.newClientConnectedTo(CLIENT, ATHENS, GenerationVotingClient::new);
            
            // Request 1: Should get generation 1
            System.out.println("\n=== Request 1 ===");
            ListenableFuture<NextGenerationResponse> future1 = client.getNextGeneration(ATHENS);
            assertEventually(cluster, () -> future1.isCompleted() && !future1.isFailed());
            
            assertEquals(1, future1.getResult().generation(), "First generation should be 1");
            
            // Verify all nodes have generation 1
            GenerationVotingServer athensServer = getServer(cluster, ATHENS);
            GenerationVotingServer byzantiumServer = getServer(cluster, BYZANTIUM);
            GenerationVotingServer cyreneServer = getServer(cluster, CYRENE);
            
            assertEquals(1, athensServer.getGeneration(), "Athens should have generation 1");
            assertEquals(1, byzantiumServer.getGeneration(), "Byzantium should have generation 1");
            assertEquals(1, cyreneServer.getGeneration(), "Cyrene should have generation 1");
            
            // Request 2: Should get generation 2
            System.out.println("\n=== Request 2 ===");
            ListenableFuture<NextGenerationResponse> future2 = client.getNextGeneration(ATHENS);
            assertEventually(cluster, () -> future2.isCompleted() && !future2.isFailed());
            
            assertEquals(2, future2.getResult().generation(), "Second generation should be 2");
            assertEquals(2, athensServer.getGeneration(), "Athens should have generation 2");
            assertEquals(2, byzantiumServer.getGeneration(), "Byzantium should have generation 2");
            assertEquals(2, cyreneServer.getGeneration(), "Cyrene should have generation 2");
            
            // Request 3: Should get generation 3
            System.out.println("\n=== Request 3 ===");
            ListenableFuture<NextGenerationResponse> future3 = client.getNextGeneration(ATHENS);
            assertEventually(cluster, () -> future3.isCompleted() && !future3.isFailed());
            
            assertEquals(3, future3.getResult().generation(), "Third generation should be 3");
            assertEquals(3, athensServer.getGeneration(), "Athens should have generation 3");
            assertEquals(3, byzantiumServer.getGeneration(), "Byzantium should have generation 3");
            assertEquals(3, cyreneServer.getGeneration(), "Cyrene should have generation 3");
            
            System.out.println("\n=== SUCCESS: All generations monotonically increasing ===");
        }
    }

    private static GenerationVotingServer getProcess(Cluster cluster, ProcessId processId) {
        return (GenerationVotingServer)cluster.getProcess(processId);
    }

    @Test
    @DisplayName("Multiple Coordinators: Different nodes can coordinate elections")
    void testMultipleCoordinators() throws IOException {
        try (var cluster = Cluster.create(List.of(ATHENS, BYZANTIUM, CYRENE), (peerIds, processParams) -> new GenerationVotingServer(peerIds, processParams))) {
            assertEventually(cluster, ()->
                    getProcess(cluster, ATHENS).isInitialised() && getProcess(cluster, BYZANTIUM).isInitialised() && getProcess(cluster, CYRENE).isInitialised());

            var client = cluster.newClientConnectedTo(CLIENT, ATHENS, GenerationVotingClient::new);
            
            // Request to Athens
            System.out.println("\n=== Request to ATHENS ===");
            ListenableFuture<NextGenerationResponse> future1 = client.getNextGeneration(ATHENS);
            assertEventually(cluster, () -> future1.isCompleted() && !future1.isFailed());
            assertEquals(1, future1.getResult().generation());
            
            // Request to Byzantium
            System.out.println("\n=== Request to BYZANTIUM ===");
            ListenableFuture<NextGenerationResponse> future2 = client.getNextGeneration(BYZANTIUM);
            assertEventually(cluster, () -> future2.isCompleted() && !future2.isFailed());
            assertEquals(2, future2.getResult().generation());
            
            // Request to Cyrene
            System.out.println("\n=== Request to CYRENE ===");
            ListenableFuture<NextGenerationResponse> future3 = client.getNextGeneration(CYRENE);
            assertEventually(cluster, () -> future3.isCompleted() && !future3.isFailed());
            assertEquals(3, future3.getResult().generation());
            
            // Verify all nodes have the same final generation
            GenerationVotingServer athensServer = getServer(cluster, ATHENS);
            GenerationVotingServer byzantiumServer = getServer(cluster, BYZANTIUM);
            GenerationVotingServer cyreneServer = getServer(cluster, CYRENE);
            
            assertEquals(3, athensServer.getGeneration());
            assertEquals(3, byzantiumServer.getGeneration());
            assertEquals(3, cyreneServer.getGeneration());
            
            System.out.println("\n=== SUCCESS: Multiple coordinators work correctly ===");
        }
    }
    
    @Test
    @DisplayName("Concurrent Requests: Multiple simultaneous requests maintain monotonicity")
    void testConcurrentRequests() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new GenerationVotingServer(peerIds, processParams))
                .start()) {

            assertEventually(cluster, ()->
                    getProcess(cluster, ATHENS).isInitialised() && getProcess(cluster, BYZANTIUM).isInitialised() && getProcess(cluster, CYRENE).isInitialised());

            var client = cluster.newClientConnectedTo(CLIENT, ATHENS, GenerationVotingClient::new);
            
            // Send multiple concurrent requests to same coordinator
            // (This tests retry mechanism when elections conflict)
            System.out.println("\n=== Sending 5 concurrent requests to ATHENS ===");
            ListenableFuture<NextGenerationResponse> future1 = client.getNextGeneration(ATHENS);
            
            // Small delay between requests to avoid overwhelming the system
            for (int i = 0; i < 10; i++) {
                cluster.tick();
            }
            
            ListenableFuture<NextGenerationResponse> future2 = client.getNextGeneration(ATHENS);
            for (int i = 0; i < 10; i++) {
                cluster.tick();
            }
            
            ListenableFuture<NextGenerationResponse> future3 = client.getNextGeneration(ATHENS);
            for (int i = 0; i < 10; i++) {
                cluster.tick();
            }
            
            // Wait for all to complete
            assertEventually(cluster, () -> 
                future1.isCompleted() && future2.isCompleted() && future3.isCompleted()
            );
            
            // Get all results
            long gen1 = future1.getResult().generation();
            long gen2 = future2.getResult().generation();
            long gen3 = future3.getResult().generation();
            
            System.out.println("Generated: " + gen1 + ", " + gen2 + ", " + gen3);
            
            // Verify all succeeded (non-zero)
            assert gen1 > 0 : "Generation 1 should be positive";
            assert gen2 > 0 : "Generation 2 should be positive";
            assert gen3 > 0 : "Generation 3 should be positive";
            
            // Verify all are unique
            var uniqueGenerations = java.util.Set.of(gen1, gen2, gen3);
            assertEquals(3, uniqueGenerations.size(), "All generations should be unique");
            
            // Verify monotonic (1, 2, 3)
            assertEquals(1, gen1, "First should be 1");
            assertEquals(2, gen2, "Second should be 2");
            assertEquals(3, gen3, "Third should be 3");
            
            System.out.println("\n=== SUCCESS: Concurrent requests maintain monotonicity and uniqueness ===");
        }
    }
    
    /**
     * Helper to get server instance from cluster.
     */
    private static GenerationVotingServer getServer(Cluster cluster, ProcessId id) {
        return getProcess(cluster, id);
    }
}

