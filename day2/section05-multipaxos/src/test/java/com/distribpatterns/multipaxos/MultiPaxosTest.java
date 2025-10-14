package com.distribpatterns.multipaxos;

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
 * Tests for Multi-Paxos demonstrating leader election and optimized consensus.
 */
public class MultiPaxosTest {
    
    // Greek city-states
    private static final ProcessId ATHENS = ProcessId.of("athens");
    private static final ProcessId BYZANTIUM = ProcessId.of("byzantium");
    private static final ProcessId CYRENE = ProcessId.of("cyrene");
    
    // Client
    private static final ProcessId CLIENT = ProcessId.of("client");
    
    @Test
    @DisplayName("Leader Election: Single node becomes leader")
    void testLeaderElection() throws IOException {
        System.out.println("\n=== TEST: Leader Election ===\n");
        
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new MultiPaxosServer(peerIds, processParams))
                .start()) {
            
            var athens = getServer(cluster, ATHENS);
            
            // WHEN: Athens triggers leader election
            athens.startLeaderElection();
            
            // THEN: Athens should become leader
            assertEventually(cluster, () -> athens.isLeader());
            assertEquals(ServerRole.Leader, athens.getRole());
            
            System.out.println("\n=== Test passed: Athens became leader ===\n");
        }
    }
    
    @Test
    @DisplayName("Leader Replication: Leader can replicate values")
    void testLeaderCanReplicateValues() throws IOException {
        System.out.println("\n=== TEST: Leader Replication ===\n");
        
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new MultiPaxosServer(peerIds, processParams))
                .start()) {
            
            var client = cluster.newClientConnectedTo(CLIENT, ATHENS, MultiPaxosClient::new);
            var athens = getServer(cluster, ATHENS);
            
            // GIVEN: Athens is the leader
            athens.startLeaderElection();
            assertEventually(cluster, () -> athens.isLeader());
            
            // WHEN: Client sends SetValue operation
            ListenableFuture<ExecuteCommandResponse> future = client.execute(ATHENS,
                new SetValueOperation("title", "Microservices"));
            
            // THEN: Operation should succeed
            assertEventually(cluster, () -> future.isCompleted() && !future.isFailed());
            
            var response = future.getResult();
            assertTrue(response.success(), "Operation should succeed");
            assertEquals("Microservices", response.result());
            
            // AND: All nodes should have the value
            var byzantium = getServer(cluster, BYZANTIUM);
            var cyrene = getServer(cluster, CYRENE);
            
            assertEquals("Microservices", athens.getValue("title"));
            assertEquals("Microservices", byzantium.getValue("title"));
            assertEquals("Microservices", cyrene.getValue("title"));
            
            System.out.println("\n=== Test passed: Leader replicated value ===\n");
        }
    }
    
    @Test
    @DisplayName("Multiple Operations: Sequential consistency")
    void testMultipleOperations() throws IOException {
        System.out.println("\n=== TEST: Multiple Operations ===\n");
        
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new MultiPaxosServer(peerIds, processParams))
                .start()) {
            
            var client = cluster.newClientConnectedTo(CLIENT, ATHENS, MultiPaxosClient::new);
            var athens = getServer(cluster, ATHENS);
            
            // GIVEN: Athens is the leader
            athens.startLeaderElection();
            assertEventually(cluster, () -> athens.isLeader());
            
            // WHEN: Multiple operations are executed
            ListenableFuture<ExecuteCommandResponse> future1 = client.execute(ATHENS,
                new SetValueOperation("title", "Microservices"));
            assertEventually(cluster, () -> future1.isCompleted() && !future1.isFailed());
            
            ListenableFuture<ExecuteCommandResponse> future2 = client.execute(ATHENS,
                new SetValueOperation("author", "Martin Fowler"));
            assertEventually(cluster, () -> future2.isCompleted() && !future2.isFailed());
            
            // THEN: All operations should be replicated
            var byzantium = getServer(cluster, BYZANTIUM);
            var cyrene = getServer(cluster, CYRENE);
            
            assertEquals(2, athens.getPaxosLog().size());
            assertEquals(2, byzantium.getPaxosLog().size());
            assertEquals(2, cyrene.getPaxosLog().size());
            
            assertEquals("Microservices", athens.getValue("title"));
            assertEquals("Martin Fowler", athens.getValue("author"));
            
            System.out.println("\n=== Test passed: Multiple operations ===\n");
        }
    }
    
    @Test
    @DisplayName("Non-Leader Rejection: Followers reject client requests")
    void testNonLeaderRejectsRequests() throws IOException {
        System.out.println("\n=== TEST: Non-Leader Rejection ===\n");
        
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new MultiPaxosServer(peerIds, processParams))
                .start()) {
            
            var client = cluster.newClientConnectedTo(CLIENT, ATHENS, MultiPaxosClient::new);
            var athens = getServer(cluster, ATHENS);
            var byzantium = getServer(cluster, BYZANTIUM);
            
            // GIVEN: Athens is the leader
            athens.startLeaderElection();
            assertEventually(cluster, () -> athens.isLeader());
            
            // WHEN: Client sends request to Byzantium (not leader)
            ListenableFuture<ExecuteCommandResponse> future = client.execute(BYZANTIUM,
                new SetValueOperation("title", "Microservices"));
            
            // THEN: Request should be rejected
            assertEventually(cluster, () -> future.isCompleted());
            
            var response = future.getResult();
            assertFalse(response.success(), "Non-leader should reject requests");
            assertEquals("Not leader", response.result());
            
            System.out.println("\n=== Test passed: Non-leader rejection ===\n");
        }
    }
    
    @Test
    @DisplayName("Get Value: Read with no-op")
    void testGetValue() throws IOException {
        System.out.println("\n=== TEST: Get Value ===\n");
        
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new MultiPaxosServer(peerIds, processParams))
                .start()) {
            
            var client = cluster.newClientConnectedTo(CLIENT, ATHENS, MultiPaxosClient::new);
            var athens = getServer(cluster, ATHENS);
            
            // GIVEN: Athens is the leader
            athens.startLeaderElection();
            assertEventually(cluster, () -> athens.isLeader());
            
            // AND: A value is set
            ListenableFuture<ExecuteCommandResponse> setFuture = client.execute(ATHENS,
                new SetValueOperation("author", "Martin Fowler"));
            assertEventually(cluster, () -> setFuture.isCompleted() && !setFuture.isFailed());
            
            // WHEN: Client gets the value
            ListenableFuture<GetValueResponse> getFuture = client.getValue(ATHENS, "author");
            assertEventually(cluster, () -> getFuture.isCompleted() && !getFuture.isFailed());
            
            // THEN: Value should be returned
            var response = getFuture.getResult();
            assertEquals("Martin Fowler", response.value());
            
            System.out.println("\n=== Test passed: Get value ===\n");
        }
    }
    
    private static MultiPaxosServer getServer(Cluster cluster, ProcessId id) {
        return (MultiPaxosServer) cluster.getProcess(id);
    }
}

