package com.distribpatterns.paxoslog;

import com.distribpatterns.paxoslog.messages.CompareAndSwapOperation;
import com.distribpatterns.paxoslog.messages.ExecuteCommandResponse;
import com.distribpatterns.paxoslog.messages.GetValueResponse;
import com.distribpatterns.paxoslog.messages.SetValueOperation;
import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for PaxosLog demonstrating replicated log with consensus.
 */
public class PaxosLogTest {
    
    // Greek city-states
    private static final ProcessId ATHENS = ProcessId.of("athens");
    private static final ProcessId BYZANTIUM = ProcessId.of("byzantium");
    private static final ProcessId CYRENE = ProcessId.of("cyrene");
    
    // Client
    private static final ProcessId CLIENT = ProcessId.of("client");
    private static final String TITLE_KEY = "title";
    private static final String AUTHOR_KEY = "author";
    
    @Test
    @DisplayName("Basic Command Execution: Single SetValue command")
    void testBasicSetValue() throws IOException {
        try (var cluster = Cluster.create(List.of(ATHENS, BYZANTIUM, CYRENE), (peerIds, processParams) -> new PaxosLogServer(peerIds, processParams))) {
            waitUntilAllNodesInitialised(cluster);
            
            var client = cluster.newClientConnectedTo(CLIENT, ATHENS, PaxosLogClient::new);
            
            ExecuteCommandResponse response = cluster.tickUntilComplete(client.execute(
                ATHENS,
                new SetValueOperation(TITLE_KEY, "Microservices")
            ));
            assertTrue(response.success(), "Command should succeed");
            assertEquals("Microservices", response.result());
            
            assertValueOnAllReplicas(cluster, TITLE_KEY, "Microservices");
            assertHighWaterMarkOnAllReplicas(cluster, 0);
        }
    }
    
    @Test
    @DisplayName("Multiple Commands: Sequential log building")
    void testMultipleCommands() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new PaxosLogServer(peerIds, processParams))
                .start()) {
            waitUntilAllNodesInitialised(cluster);
            
            var client = cluster.newClientConnectedTo(CLIENT, ATHENS, PaxosLogClient::new);
            
            ExecuteCommandResponse firstResponse = cluster.tickUntilComplete(client.execute(
                ATHENS,
                new SetValueOperation(TITLE_KEY, "Microservices")
            ));
            assertSuccessfulCommand(firstResponse);
            
            ExecuteCommandResponse secondResponse = cluster.tickUntilComplete(client.execute(
                BYZANTIUM,
                new SetValueOperation(AUTHOR_KEY, "Martin")
            ));
            assertSuccessfulCommand(secondResponse);
            
            PaxosLogServer athens = getServer(cluster, ATHENS);
            assertHighWaterMarkOnAllReplicas(cluster, 1);
            assertEquals("Microservices", athens.getValue(TITLE_KEY));
            assertEquals("Martin", athens.getValue(AUTHOR_KEY));
        }
    }
    
    @Test
    @DisplayName("Compare-And-Swap: Atomic conditional update")
    void testCompareAndSwap() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new PaxosLogServer(peerIds, processParams))
                .start()) {
            waitUntilAllNodesInitialised(cluster);
            
            var client = cluster.newClientConnectedTo(CLIENT, ATHENS, PaxosLogClient::new);
            
            cluster.tickUntilComplete(client.execute(
                ATHENS,
                new SetValueOperation(TITLE_KEY, "Microservices")
            ));
            
            ExecuteCommandResponse cas1Result = cluster.tickUntilComplete(client.execute(
                BYZANTIUM,
                new CompareAndSwapOperation(TITLE_KEY, "Wrong Value", "Updated")
            ));
            assertFalse(cas1Result.success(), "CAS should fail when expected value doesn't match");
            assertEquals("Microservices", cas1Result.result(), "Should return current value");
            
            ExecuteCommandResponse cas2Result = cluster.tickUntilComplete(client.execute(
                CYRENE,
                new CompareAndSwapOperation(TITLE_KEY, "Microservices", "Event Driven Microservices")
            ));
            assertTrue(cas2Result.success(), "CAS should succeed when expected value matches");
            assertEquals("Microservices", cas2Result.result(), "Should return previous value");
            
            assertValueOnAllReplicas(cluster, TITLE_KEY, "Event Driven Microservices");
        }
    }
    
    @Test
    @DisplayName("Get Value: Read with no-op")
    void testGetValue() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new PaxosLogServer(peerIds, processParams))
                .start()) {
            waitUntilAllNodesInitialised(cluster);
            
            var client = cluster.newClientConnectedTo(CLIENT, ATHENS, PaxosLogClient::new);
            
            cluster.tickUntilComplete(client.execute(
                ATHENS,
                new SetValueOperation(AUTHOR_KEY, "Martin")
            ));
            
            // Reads also go through Paxos by committing a no-op entry first.
            // That guarantees this node has applied all earlier committed log entries
            // before reading from the local state machine.
            GetValueResponse response = cluster.tickUntilComplete(client.getValue(BYZANTIUM, AUTHOR_KEY));
            assertEquals("Martin", response.value());
        }
    }
    
    @Test
    @DisplayName("Committed Log: Entries remain available in the replica log")
    void testCommittedLogEntries() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new PaxosLogServer(peerIds, processParams))
                .start()) {
            waitUntilAllNodesInitialised(cluster);
            
            var client = cluster.newClientConnectedTo(CLIENT, ATHENS, PaxosLogClient::new);
            
            ExecuteCommandResponse firstResponse = cluster.tickUntilComplete(client.execute(
                ATHENS,
                new SetValueOperation("key1", "value1")
            ));
            assertSuccessfulCommand(firstResponse);
            
            ExecuteCommandResponse secondResponse = cluster.tickUntilComplete(client.execute(
                BYZANTIUM,
                new SetValueOperation("key2", "value2")
            ));
            assertSuccessfulCommand(secondResponse);
            assertHighWaterMarkOnAllReplicas(cluster, 1);
            assertCommittedLogEntryOnAllReplicas(cluster, 0);
            assertCommittedLogEntryOnAllReplicas(cluster, 1);
        }
    }
    
    @Test
    @DisplayName("Log Consistency: All nodes have same log entries")
    void testLogConsistency() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new PaxosLogServer(peerIds, processParams))
                .start()) {
            waitUntilAllNodesInitialised(cluster);
            
            var client = cluster.newClientConnectedTo(CLIENT, ATHENS, PaxosLogClient::new);
            
            for (int i = 0; i < 3; i++) {
                ExecuteCommandResponse response = cluster.tickUntilComplete(client.execute(
                    ATHENS,
                    new SetValueOperation("key" + i, "value" + i)
                ));
                assertSuccessfulCommand(response, "Entry " + i + " should succeed");
            }
            
            PaxosLogServer athens = getServer(cluster, ATHENS);
            PaxosLogServer byzantium = getServer(cluster, BYZANTIUM);
            PaxosLogServer cyrene = getServer(cluster, CYRENE);
            
            int hwm = athens.getHighWaterMark();
            assertEquals(hwm, byzantium.getHighWaterMark(), "Byzantium HWM should match Athens");
            assertEquals(hwm, cyrene.getHighWaterMark(), "Cyrene HWM should match Athens");
            assertEquals(2, hwm, "High water mark should be 2 (indices 0, 1, 2)");
            
            // All nodes should have committed entries at the same indices
            var athensLog = athens.getPaxosLog();
            var byzantiumLog = byzantium.getPaxosLog();
            var cyreneLog = cyrene.getPaxosLog();
            
            for (int i = 0; i <= hwm; i++) {
                assertCommittedEntryExists(athensLog, i, "Athens should have a committed entry at index " + i);
                assertCommittedEntryExists(byzantiumLog, i, "Byzantium should have a committed entry at index " + i);
                assertCommittedEntryExists(cyreneLog, i, "Cyrene should have a committed entry at index " + i);
            }
        }
    }
    
    private static void waitUntilAllNodesInitialised(Cluster cluster) {
        cluster.tickUntil(() ->
                getServer(cluster, ATHENS).isInitialised()
                        && getServer(cluster, BYZANTIUM).isInitialised()
                        && getServer(cluster, CYRENE).isInitialised());
    }

    private static void assertValueOnAllReplicas(Cluster cluster, String key, String expectedValue) {
        assertEquals(expectedValue, getServer(cluster, ATHENS).getValue(key));
        assertEquals(expectedValue, getServer(cluster, BYZANTIUM).getValue(key));
        assertEquals(expectedValue, getServer(cluster, CYRENE).getValue(key));
    }

    private static void assertHighWaterMarkOnAllReplicas(Cluster cluster, int expectedHighWaterMark) {
        assertEquals(expectedHighWaterMark, getServer(cluster, ATHENS).getHighWaterMark());
        assertEquals(expectedHighWaterMark, getServer(cluster, BYZANTIUM).getHighWaterMark());
        assertEquals(expectedHighWaterMark, getServer(cluster, CYRENE).getHighWaterMark());
    }

    private static void assertSuccessfulCommand(ExecuteCommandResponse response) {
        assertSuccessfulCommand(response, "Command should succeed");
    }

    private static void assertSuccessfulCommand(ExecuteCommandResponse response, String message) {
        assertTrue(response.success(), message);
    }

    private static void assertCommittedEntryExists(java.util.Map<Integer, PaxosState> log, int index) {
        assertCommittedEntryExists(log, index, "Entry at index " + index + " should exist and be committed");
    }

    private static void assertCommittedEntryExists(java.util.Map<Integer, PaxosState> log, int index, String message) {
        PaxosState entry = log.get(index);
        assertNotNull(entry, message);
        assertTrue(entry.committedValue().isPresent(), message);
    }

    private static void assertCommittedLogEntryOnAllReplicas(Cluster cluster, int index) {
        assertCommittedLogEntry(cluster, ATHENS, index);
        assertCommittedLogEntry(cluster, BYZANTIUM, index);
        assertCommittedLogEntry(cluster, CYRENE, index);
    }

    private static void assertCommittedLogEntry(Cluster cluster, ProcessId processId, int index) {
        PaxosState entry = getServer(cluster, processId).getLogEntry(index);
        assertNotNull(entry, processId + " should have a log entry at index " + index);
        assertTrue(entry.committedValue().isPresent(),
                processId + " should have a committed log entry at index " + index);
    }

    private static PaxosLogServer getServer(Cluster cluster, ProcessId id) {
        return cluster.getNode(id);
    }
}
