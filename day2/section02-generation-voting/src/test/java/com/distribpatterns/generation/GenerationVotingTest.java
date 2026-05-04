package com.distribpatterns.generation;

import com.distribpatterns.generation.messages.NextGenerationResponse;
import com.tickloom.ProcessId;
import com.tickloom.future.ListenableFuture;
import com.tickloom.testkit.Cluster;
import com.tickloom.testkit.NodeGroup;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static com.tickloom.testkit.ClusterAssertions.assertEventually;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    
    // Client
    private static final ProcessId CLIENT = ProcessId.of("client");
    
    @Test
    @DisplayName("Majority available: generations stay monotonic across changing partitions")
    void testGenerateMonotonicNumbers() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build( (peerIds, processParams) -> new GenerationVotingServer(peerIds, processParams))
                .start()) {
            waitUntilAllNodesInitialised(cluster);

            var athensClient = cluster.newClientConnectedTo(CLIENT, ATHENS, GenerationVotingClient::new);
            var cyreneClient = cluster.newClientConnectedTo(ProcessId.of("client2"), CYRENE, GenerationVotingClient::new);

            // Isolate CYRENE; ATHENS and BYZANTIUM still form a majority.
            cluster.partitionNodes(NodeGroup.of(ATHENS, BYZANTIUM), NodeGroup.of(CYRENE));
            NextGenerationResponse response1 = cluster.tickUntilComplete(athensClient.getNextGeneration(ATHENS));
            assertEquals(1, response1.generation(), "First generation should be 1");
            assertEquals(1, cluster.<GenerationVotingServer>getNode(ATHENS).getGeneration());
            assertEquals(1, cluster.<GenerationVotingServer>getNode(BYZANTIUM).getGeneration());

            cluster.healAllPartitions();

            // Isolate ATHENS; BYZANTIUM and CYRENE still form a majority.
            // Use BYZANTIUM as coordinator because it already knows generation 1
            // and can propose generation 2 directly.
            cluster.partitionNodes(NodeGroup.of(CYRENE, BYZANTIUM), NodeGroup.of(ATHENS));
            NextGenerationResponse response2 = cluster.tickUntilComplete(cyreneClient.getNextGeneration(BYZANTIUM));
            assertEquals(2, response2.generation(), "Second generation should be 2");
            assertEquals(2, cluster.<GenerationVotingServer>getNode(CYRENE).getGeneration());
            assertEquals(2, cluster.<GenerationVotingServer>getNode(BYZANTIUM).getGeneration());

            cluster.healAllPartitions();

            // Isolate BYZANTIUM; ATHENS and CYRENE still form a majority.
            // Use CYRENE as coordinator because it already knows generation 2
            // and can propose generation 3 directly.
            cluster.partitionNodes(NodeGroup.of(ATHENS, CYRENE), NodeGroup.of(BYZANTIUM));
            NextGenerationResponse response3 = cluster.tickUntilComplete(cyreneClient.getNextGeneration(CYRENE));
            assertEquals(3, response3.generation(), "Third generation should be 3");
            assertEquals(3, cluster.<GenerationVotingServer>getNode(ATHENS).getGeneration());
            assertEquals(3, cluster.<GenerationVotingServer>getNode(CYRENE).getGeneration());

            NextGenerationResponse response4 = cluster.tickUntilComplete(cyreneClient.getNextGeneration(CYRENE));
            assertEquals(4, response4.generation(), "Fourth generation should be 4");
            assertEquals(4, cluster.<GenerationVotingServer>getNode(ATHENS).getGeneration());
            assertEquals(4, cluster.<GenerationVotingServer>getNode(CYRENE).getGeneration());
            assertEquals(2, cluster.<GenerationVotingServer>getNode(BYZANTIUM).getGeneration());

            cluster.healAllPartitions();

            // Byzantium gen=2 while Athens and Cyrene are at gen=4.
            // Byzantium proposes gen=3, gets rejected by both (gen=4),
            // retries with gen=5 and wins.
            NextGenerationResponse response5 = cluster.tickUntilComplete(cyreneClient.getNextGeneration(BYZANTIUM));
            assertEquals(5, response5.generation(), "Fifth generation should be 5");


        }
    }

    @Test
    @DisplayName("Multiple Coordinators: Different nodes can coordinate elections")
    void testMultipleCoordinators() throws IOException {
        try (var cluster = Cluster.create(List.of(ATHENS, BYZANTIUM, CYRENE), (peerIds, processParams) -> new GenerationVotingServer(peerIds, processParams))) {
            waitUntilAllNodesInitialised(cluster);

            var client = cluster.newClientConnectedTo(CLIENT, ATHENS, GenerationVotingClient::new);
            
            // First Athens coordinates generation 1.
            NextGenerationResponse response1 = cluster.tickUntilComplete(client.getNextGeneration(ATHENS));
            assertEquals(1, response1.generation());
            
            // Then Byzantium coordinates generation 2.
            NextGenerationResponse response2 = cluster.tickUntilComplete(client.getNextGeneration(BYZANTIUM));
            assertEquals(2, response2.generation());
            
            // Finally Cyrene coordinates generation 3.
            NextGenerationResponse response3 = cluster.tickUntilComplete(client.getNextGeneration(CYRENE));
            assertEquals(3, response3.generation());
            
            // Verify all nodes have the same final generation
            assertGenerationOnAllNodes(cluster, 3);
        }
    }
    
    @Test
    @DisplayName("Overlapping Requests: Concurrent elections produce unique monotonic generations")
    void testOverlappingRequestsProduceUniqueGenerations() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .withRequestTimeoutTicks(8000)
                .build((peerIds, processParams) -> new GenerationVotingServer(peerIds, processParams))
                .start()) {
            waitUntilAllNodesInitialised(cluster);

            var client = cluster.newClientConnectedTo(CLIENT, ATHENS, GenerationVotingClient::new);

            ListenableFuture<NextGenerationResponse> future1 = client.getNextGeneration(ATHENS);
            ListenableFuture<NextGenerationResponse> future2 = client.getNextGeneration(ATHENS);
            ListenableFuture<NextGenerationResponse> future3 = client.getNextGeneration(ATHENS);

            assertFalse(future1.isCompleted(), "First request should still be in flight before ticking");
            assertFalse(future2.isCompleted(), "Second request should still be in flight before ticking");
            assertFalse(future3.isCompleted(), "Third request should still be in flight before ticking");

            assertEventually(cluster, () ->
                !future1.isPending() && !future2.isPending() && !future3.isPending()
            );

            List<ListenableFuture<NextGenerationResponse>> futures = List.of(future1, future2, future3);
            List<ListenableFuture<NextGenerationResponse>> successfulFutures = successfulFuturesFrom(futures);
            List<ListenableFuture<NextGenerationResponse>> failedFutures = failedFuturesFrom(futures);

            assertFalse(successfulFutures.isEmpty(), "At least one request should succeed");
            assertEquals(3, successfulFutures.size() + failedFutures.size(), "All futures should resolve");

            List<Long> generations = successfulFutures.stream()
                    .map(f -> f.getResult().generation())
                    .sorted()
                    .toList();
            for (int i = 1; i < generations.size(); i++) {
                assertTrue(generations.get(i) > generations.get(i - 1),
                        "Successful generations must be unique and monotonic: " + generations);
            }

            long highestGeneration = generations.getLast();
            assertGenerationOnAllNodes(cluster, highestGeneration);
        }
    }

    private static void waitUntilAllNodesInitialised(Cluster cluster) {
        assertEventually(cluster, () ->
                cluster.<GenerationVotingServer>getNode(ATHENS).isInitialised()
                        && cluster.<GenerationVotingServer>getNode(BYZANTIUM).isInitialised()
                        && cluster.<GenerationVotingServer>getNode(CYRENE).isInitialised());
    }

    private static void assertGenerationOnAllNodes(Cluster cluster, long expectedGeneration) {
        assertEquals(expectedGeneration, cluster.<GenerationVotingServer>getNode(ATHENS).getGeneration());
        assertEquals(expectedGeneration, cluster.<GenerationVotingServer>getNode(BYZANTIUM).getGeneration());
        assertEquals(expectedGeneration, cluster.<GenerationVotingServer>getNode(CYRENE).getGeneration());
    }

    private static List<ListenableFuture<NextGenerationResponse>> successfulFuturesFrom(
            List<ListenableFuture<NextGenerationResponse>> futures
    ) {
        return futures.stream()
                .filter(ListenableFuture::isCompleted)
                .toList();
    }

    private static List<ListenableFuture<NextGenerationResponse>> failedFuturesFrom(
            List<ListenableFuture<NextGenerationResponse>> futures
    ) {
        return futures.stream()
                .filter(ListenableFuture::isFailed)
                .toList();
    }
}
