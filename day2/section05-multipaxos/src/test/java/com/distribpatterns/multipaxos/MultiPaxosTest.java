package com.distribpatterns.multipaxos;

import com.distribpatterns.multipaxos.messages.ExecuteCommandResponse;
import com.distribpatterns.multipaxos.messages.GetValueResponse;
import com.distribpatterns.multipaxos.messages.SetValueOperation;
import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

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
    private static final String TITLE_KEY = "title";
    private static final String AUTHOR_KEY = "author";
    
    @Test
    @DisplayName("Leader Election: Single node becomes leader")
    void testLeaderElection() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new MultiPaxosServer(peerIds, processParams))
                .start()) {
            
            var athens = getServer(cluster, ATHENS);
            
            athens.startLeaderElection();
            
            cluster.tickUntil(athens::isLeader);
            assertEquals(ServerRole.Leader, athens.getRole());
        }
    }
    
    @Test
    @DisplayName("Leader Replication: Leader can replicate values")
    void testLeaderCanReplicateValues() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new MultiPaxosServer(peerIds, processParams))
                .start()) {
            
            var client = cluster.newClientConnectedTo(CLIENT, ATHENS, MultiPaxosClient::new);
            var athens = getServer(cluster, ATHENS);
            
            athens.startLeaderElection();
            cluster.tickUntil(athens::isLeader);
            
            ExecuteCommandResponse response = cluster.tickUntilComplete(client.execute(
                ATHENS,
                new SetValueOperation(TITLE_KEY, "Microservices")
            ));
            assertTrue(response.success(), "Operation should succeed");
            assertEquals("Microservices", response.result());
            
            assertValueOnAllReplicas(cluster, TITLE_KEY, "Microservices");
        }
    }
    
    @Test
    @DisplayName("Multiple Operations: Sequential consistency")
    void testMultipleOperations() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new MultiPaxosServer(peerIds, processParams))
                .start()) {
            
            var client = cluster.newClientConnectedTo(CLIENT, ATHENS, MultiPaxosClient::new);
            var athens = getServer(cluster, ATHENS);
            
            athens.startLeaderElection();
            cluster.tickUntil(athens::isLeader);
            
            ExecuteCommandResponse firstResponse = cluster.tickUntilComplete(client.execute(
                ATHENS,
                new SetValueOperation(TITLE_KEY, "Microservices")
            ));
            assertSuccessfulCommand(firstResponse);
            
            ExecuteCommandResponse secondResponse = cluster.tickUntilComplete(client.execute(
                ATHENS,
                new SetValueOperation(AUTHOR_KEY, "Martin Fowler")
            ));
            assertSuccessfulCommand(secondResponse);
            
            assertLogSizeOnAllReplicas(cluster, 2);
            assertValueOnAllReplicas(cluster, TITLE_KEY, "Microservices");
            assertValueOnAllReplicas(cluster, AUTHOR_KEY, "Martin Fowler");
        }
    }
    
    @Test
    @DisplayName("Non-Leader Rejection: Followers reject client requests")
    void testNonLeaderRejectsRequests() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new MultiPaxosServer(peerIds, processParams))
                .start()) {
            
            var client = cluster.newClientConnectedTo(CLIENT, ATHENS, MultiPaxosClient::new);
            var athens = getServer(cluster, ATHENS);
            
            athens.startLeaderElection();
            cluster.tickUntil(athens::isLeader);
            
            ExecuteCommandResponse response = cluster.tickUntilComplete(client.execute(
                BYZANTIUM,
                new SetValueOperation(TITLE_KEY, "Microservices")
            ));
            assertFalse(response.success(), "Non-leader should reject requests");
            assertEquals("Not leader", response.result());
        }
    }
    
    @Test
    @DisplayName("Get Value: Read with no-op")
    void testGetValue() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new MultiPaxosServer(peerIds, processParams))
                .start()) {
            
            var client = cluster.newClientConnectedTo(CLIENT, ATHENS, MultiPaxosClient::new);
            var athens = getServer(cluster, ATHENS);
            
            athens.startLeaderElection();
            cluster.tickUntil(athens::isLeader);
            
            cluster.tickUntilComplete(client.execute(
                ATHENS,
                new SetValueOperation(AUTHOR_KEY, "Martin Fowler")
            ));
            
            // Reads also go through Multi-Paxos by committing a no-op first.
            // That guarantees the leader has applied earlier committed log entries
            // before reading from its local state machine.
            GetValueResponse response = cluster.tickUntilComplete(client.getValue(ATHENS, AUTHOR_KEY));
            assertEquals("Martin Fowler", response.value());
        }
    }
    
    private static MultiPaxosServer getServer(Cluster cluster, ProcessId id) {
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

    private static void assertLogSizeOnAllReplicas(Cluster cluster, int expectedLogSize) {
        assertEquals(expectedLogSize, getServer(cluster, ATHENS).getPaxosLog().size());
        assertEquals(expectedLogSize, getServer(cluster, BYZANTIUM).getPaxosLog().size());
        assertEquals(expectedLogSize, getServer(cluster, CYRENE).getPaxosLog().size());
    }
}
