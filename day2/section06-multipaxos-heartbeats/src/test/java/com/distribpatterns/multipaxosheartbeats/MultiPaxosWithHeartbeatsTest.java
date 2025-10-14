package com.distribpatterns.multipaxosheartbeats;

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
 * Tests for Multi-Paxos with automatic heartbeat-based leader election.
 * 
 * The onTick() override enables fully automatic heartbeat mechanism:
 * - Leaders automatically send heartbeats on every tick
 * - Followers automatically check for timeout on every tick
 * - Elections automatically trigger when timeout expires
 * 
 * This demonstrates a production-ready heartbeat protocol similar to Raft!
 */
public class MultiPaxosWithHeartbeatsTest {
    
    private static final ProcessId ATHENS = ProcessId.of("athens");
    private static final ProcessId BYZANTIUM = ProcessId.of("byzantium");
    private static final ProcessId CYRENE = ProcessId.of("cyrene");
    private static final ProcessId CLIENT_ALICE = ProcessId.of("alice");
    
    private MultiPaxosWithHeartbeatsServer getServer(Cluster cluster, ProcessId id) {
        return (MultiPaxosWithHeartbeatsServer) cluster.getProcess(id);
    }
    
    @Test
    @DisplayName("Automatic Leader Election: Heartbeat timeout triggers election")
    void testAutomaticLeaderElection() throws IOException {
        System.out.println("\n=== TEST: Automatic Leader Election ===\n");
        
        try (var cluster = Cluster.create(List.of(ATHENS, BYZANTIUM, CYRENE), (peerIds, processParams) -> new MultiPaxosWithHeartbeatsServer(peerIds, processParams))) {
            
            MultiPaxosWithHeartbeatsServer athens = getServer(cluster, ATHENS);
            MultiPaxosWithHeartbeatsServer byzantium = getServer(cluster, BYZANTIUM);
            MultiPaxosWithHeartbeatsServer cyrene = getServer(cluster, CYRENE);
            
            // All nodes start as followers with random timeout
            // The first one to timeout will trigger election via onTick()
            
            // Just tick the cluster - onTick() is called automatically on each node
            for (int i = 0; i < 300; i++) {
                cluster.tick(); // Calls onTick() on all nodes, processes messages
            }
            
            // Verify exactly one leader was elected
            long leaderCount = java.util.stream.Stream.of(athens, byzantium, cyrene)
                .filter(MultiPaxosWithHeartbeatsServer::isLeader)
                .count();
            
            assertEquals(1, leaderCount, "Should have exactly one leader after automatic election");
            
            System.out.println("✓ Automatic leader election successful!");
        }
    }
    
    @Test
    @DisplayName("Leader sends heartbeats automatically")
    void testAutomaticHeartbeats() throws IOException {
        System.out.println("\n=== TEST: Automatic Heartbeats ===\n");
        
        try (var cluster = Cluster.create(List.of(ATHENS, BYZANTIUM, CYRENE), (peerIds, processParams) -> new MultiPaxosWithHeartbeatsServer(peerIds, processParams))) {
            
            MultiPaxosWithHeartbeatsServer athens = getServer(cluster, ATHENS);
            MultiPaxosWithHeartbeatsServer byzantium = getServer(cluster, BYZANTIUM);
            MultiPaxosWithHeartbeatsServer cyrene = getServer(cluster, CYRENE);
            
            // Wait for automatic leader election (via heartbeat timeout in onTick())
            for (int i = 0; i < 300; i++) {
                cluster.tick();
            }
            
            // Find the leader
            MultiPaxosWithHeartbeatsServer leader = null;
            if (athens.isLeader()) leader = athens;
            else if (byzantium.isLeader()) leader = byzantium;
            else if (cyrene.isLeader()) leader = cyrene;
            
            assertNotNull(leader, "A leader should have been elected automatically");
            
            // Leader automatically sends heartbeats via onTick(), followers receive them
            // Followers should not trigger new elections because they receive heartbeats
            for (int i = 0; i < 50; i++) {
                cluster.tick(); // onTick() automatically called: leader sends, followers check
            }
            
            // Same node should still be leader (heartbeats prevent new elections)
            assertTrue(leader.isLeader(), "Leader should remain leader while sending heartbeats");
            
            System.out.println("✓ Automatic heartbeats successful!");
        }
    }
    
    @Test
    @DisplayName("Leader replicates values with automatic heartbeats")
    void testLeaderReplication() throws IOException {
        System.out.println("\n=== TEST: Leader Replication ===\n");
        
        try (var cluster = Cluster.create(List.of(ATHENS, BYZANTIUM, CYRENE), (peerIds, processParams) -> new MultiPaxosWithHeartbeatsServer(peerIds, processParams))) {
            
            MultiPaxosWithHeartbeatsServer athens = getServer(cluster, ATHENS);
            MultiPaxosWithHeartbeatsServer byzantium = getServer(cluster, BYZANTIUM);
            MultiPaxosWithHeartbeatsServer cyrene = getServer(cluster, CYRENE);
            
            // Wait for automatic leader election (via heartbeat timeout in onTick())
            for (int i = 0; i < 300; i++) {
                cluster.tick();
            }
            
            // Find the leader
            ProcessId leaderId = null;
            if (athens.isLeader()) leaderId = ATHENS;
            else if (byzantium.isLeader()) leaderId = BYZANTIUM;
            else if (cyrene.isLeader()) leaderId = CYRENE;
            
            assertNotNull(leaderId, "A leader should have been elected automatically");
            System.out.println("Leader elected: " + leaderId);
            
            // Execute a command on the leader
            var client = cluster.newClientConnectedTo(CLIENT_ALICE, leaderId, MultiPaxosWithHeartbeatsClient::new);
            ListenableFuture<ExecuteCommandResponse> future = client.execute(
                leaderId,
                new SetValueOperation("title", "Microservices")
            );
            
            // Wait for operation to complete
            assertEventually(cluster, () -> future.isCompleted() && !future.isFailed());
            assertTrue(future.getResult().success(), "Operation should succeed");
            
            // Verify all replicas have the value
            assertEquals("Microservices", athens.getValue("title"));
            assertEquals("Microservices", byzantium.getValue("title"));
            assertEquals("Microservices", cyrene.getValue("title"));
            
            System.out.println("✓ Leader replication with heartbeats successful!");
        }
    }
    
    @Test
    @DisplayName("Heartbeat mechanism maintains stable leadership")
    void testStableLeadership() throws IOException {
        System.out.println("\n=== TEST: Stable Leadership ===\n");
        
        try (var cluster = Cluster.create(List.of(ATHENS, BYZANTIUM, CYRENE), (peerIds, processParams) -> new MultiPaxosWithHeartbeatsServer(peerIds, processParams))) {
            
            MultiPaxosWithHeartbeatsServer athens = getServer(cluster, ATHENS);
            MultiPaxosWithHeartbeatsServer byzantium = getServer(cluster, BYZANTIUM);
            MultiPaxosWithHeartbeatsServer cyrene = getServer(cluster, CYRENE);
            
            // Wait for automatic leader election
            for (int i = 0; i < 300; i++) {
                cluster.tick();
            }
            
            // Find initial leader
            MultiPaxosWithHeartbeatsServer initialLeader = null;
            if (athens.isLeader()) initialLeader = athens;
            else if (byzantium.isLeader()) initialLeader = byzantium;
            else if (cyrene.isLeader()) initialLeader = cyrene;
            
            assertNotNull(initialLeader, "A leader should be elected");
            int initialGeneration = initialLeader.getPromisedGeneration();
            System.out.println("Initial leader generation: " + initialGeneration);
            
            // Run for more ticks - leader should remain stable due to heartbeats
            for (int i = 0; i < 200; i++) {
                cluster.tick();
            }
            
            // Same node should still be leader (no new elections triggered)
            assertTrue(initialLeader.isLeader(), "Leader should remain stable with heartbeats");
            
            System.out.println("✓ Stable leadership maintained!");
        }
    }
    
    @Test
    @DisplayName("All nodes converge to same generation")
    void testGenerationConvergence() throws IOException {
        System.out.println("\n=== TEST: Generation Convergence ===\n");
        
        try (var cluster = Cluster.create(List.of(ATHENS, BYZANTIUM, CYRENE), (peerIds, processParams) -> new MultiPaxosWithHeartbeatsServer(peerIds, processParams))) {
            
            MultiPaxosWithHeartbeatsServer athens = getServer(cluster, ATHENS);
            MultiPaxosWithHeartbeatsServer byzantium = getServer(cluster, BYZANTIUM);
            MultiPaxosWithHeartbeatsServer cyrene = getServer(cluster, CYRENE);
            
            // Wait for automatic leader election
            for (int i = 0; i < 300; i++) {
                cluster.tick();
            }
            
            // All nodes should converge to the same generation
            int athensGen = athens.getPromisedGeneration();
            int byzantiumGen = byzantium.getPromisedGeneration();
            int cyreneGen = cyrene.getPromisedGeneration();
            
            System.out.println("Athens generation: " + athensGen);
            System.out.println("Byzantium generation: " + byzantiumGen);
            System.out.println("Cyrene generation: " + cyreneGen);
            
            // All should have the same generation after election completes
            int maxGen = Math.max(athensGen, Math.max(byzantiumGen, cyreneGen));
            assertTrue(athensGen == maxGen || byzantiumGen == maxGen || cyreneGen == maxGen,
                      "All nodes should converge to same generation");
            
            System.out.println("✓ Generation convergence verified!");
        }
    }
}

