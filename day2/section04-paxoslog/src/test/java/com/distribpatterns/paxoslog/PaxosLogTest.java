package com.distribpatterns.paxoslog;

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
 * Tests for PaxosLog demonstrating replicated log with consensus.
 */
public class PaxosLogTest {
    
    // Greek city-states
    private static final ProcessId ATHENS = ProcessId.of("athens");
    private static final ProcessId BYZANTIUM = ProcessId.of("byzantium");
    private static final ProcessId CYRENE = ProcessId.of("cyrene");
    
    // Client
    private static final ProcessId CLIENT = ProcessId.of("client");
    
    @Test
    @DisplayName("Basic Command Execution: Single SetValue command")
    void testBasicSetValue() throws IOException {
        System.out.println("\n=== TEST: Basic SetValue ===\n");

        //Not doing createSimulated just to the same code if we switch to real networking with Java NIO.
        try (var cluster = Cluster.create(List.of(ATHENS, BYZANTIUM, CYRENE), (peerIds, processParams) -> new PaxosLogServer(peerIds, processParams))) {
            
            // Wait for all processes to initialize
            assertEventually(cluster, () -> cluster.areAllNodesInitialized());
            
            var client = cluster.newClientConnectedTo(CLIENT, ATHENS, PaxosLogClient::new);
            
            // Execute SetValue command
            ListenableFuture<ExecuteCommandResponse> future = client.execute(ATHENS, 
                new SetValueOperation("title", "Microservices"));
            
            // Wait for command to commit
            assertEventually(cluster, () -> future.isCompleted() && !future.isFailed());
            
            // Verify response
            var response = future.getResult();
            assertTrue(response.success(), "Command should succeed");
            assertEquals("Microservices", response.result());
            
            // Verify all nodes have the value
            PaxosLogServer athens = getServer(cluster, ATHENS);
            PaxosLogServer byzantium = getServer(cluster, BYZANTIUM);
            PaxosLogServer cyrene = getServer(cluster, CYRENE);
            
            assertEquals("Microservices", athens.getValue("title"));
            assertEquals("Microservices", byzantium.getValue("title"));
            assertEquals("Microservices", cyrene.getValue("title"));
            
            // Verify high water mark
            assertEquals(0, athens.getHighWaterMark());
            
            System.out.println("\n=== Test passed: Basic SetValue ===\n");
        }
    }
    
    @Test
    @DisplayName("Multiple Commands: Sequential log building")
    void testMultipleCommands() throws IOException {
        System.out.println("\n=== TEST: Multiple Commands ===\n");
        
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new PaxosLogServer(peerIds, processParams))
                .start()) {
            
            // Wait for all processes to initialize
            assertEventually(cluster, () -> cluster.areAllNodesInitialized());
            
            var client = cluster.newClientConnectedTo(CLIENT, ATHENS, PaxosLogClient::new);
            
            // Command 1: Set title
            ListenableFuture<ExecuteCommandResponse> future1 = client.execute(ATHENS,
                new SetValueOperation("title", "Microservices"));
            assertEventually(cluster, () -> future1.isCompleted() && !future1.isFailed());
            assertTrue(future1.getResult().success());
            
            // Command 2: Set author
            ListenableFuture<ExecuteCommandResponse> future2 = client.execute(BYZANTIUM,
                new SetValueOperation("author", "Martin"));
            assertEventually(cluster, () -> future2.isCompleted() && !future2.isFailed());
            assertTrue(future2.getResult().success());
            
            // Verify log consistency
            PaxosLogServer athens = getServer(cluster, ATHENS);
            PaxosLogServer byzantium = getServer(cluster, BYZANTIUM);
            PaxosLogServer cyrene = getServer(cluster, CYRENE);
            
            // Verify high water mark
            assertEquals(1, athens.getHighWaterMark());
            assertEquals(1, byzantium.getHighWaterMark());
            assertEquals(1, cyrene.getHighWaterMark());
            
            assertEquals("Microservices", athens.getValue("title"));
            assertEquals("Martin", athens.getValue("author"));
            
            System.out.println("\n=== Test passed: Multiple Commands ===\n");
        }
    }
    
    @Test
    @DisplayName("Compare-And-Swap: Atomic conditional update")
    void testCompareAndSwap() throws IOException {
        System.out.println("\n=== TEST: Compare-And-Swap ===\n");
        
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new PaxosLogServer(peerIds, processParams))
                .start()) {
            
            // Wait for all processes to initialize
            assertEventually(cluster, () -> cluster.areAllNodesInitialized());
            
            var client = cluster.newClientConnectedTo(CLIENT, ATHENS, PaxosLogClient::new);
            
            // Set initial value
            ListenableFuture<ExecuteCommandResponse> future1 = client.execute(ATHENS,
                new SetValueOperation("title", "Microservices"));
            assertEventually(cluster, () -> future1.isCompleted() && !future1.isFailed());
            
            // CAS: Should fail (expected value doesn't match)
            ListenableFuture<ExecuteCommandResponse> future2 = client.execute(BYZANTIUM,
                new CompareAndSwapOperation("title", "Wrong Value", "Updated"));
            assertEventually(cluster, () -> future2.isCompleted() && !future2.isFailed());
            
            var cas1Result = future2.getResult();
            assertFalse(cas1Result.success(), "CAS should fail when expected value doesn't match");
            assertEquals("Microservices", cas1Result.result(), "Should return current value");
            
            // CAS: Should succeed (expected value matches)
            ListenableFuture<ExecuteCommandResponse> future3 = client.execute(CYRENE,
                new CompareAndSwapOperation("title", "Microservices", "Event Driven Microservices"));
            assertEventually(cluster, () -> future3.isCompleted() && !future3.isFailed());
            
            var cas2Result = future3.getResult();
            assertTrue(cas2Result.success(), "CAS should succeed when expected value matches");
            assertEquals("Microservices", cas2Result.result(), "Should return previous value");
            
            // Verify final value
            PaxosLogServer athens = getServer(cluster, ATHENS);
            assertEquals("Event Driven Microservices", athens.getValue("title"));
            
            System.out.println("\n=== Test passed: Compare-And-Swap ===\n");
        }
    }
    
    @Test
    @DisplayName("Get Value: Read with no-op")
    void testGetValue() throws IOException {
        System.out.println("\n=== TEST: Get Value ===\n");
        
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new PaxosLogServer(peerIds, processParams))
                .start()) {
            
            // Wait for all processes to initialize
            assertEventually(cluster, () -> cluster.areAllNodesInitialized());
            
            var client = cluster.newClientConnectedTo(CLIENT, ATHENS, PaxosLogClient::new);
            
            // Set a value
            ListenableFuture<ExecuteCommandResponse> setFuture = client.execute(ATHENS,
                new SetValueOperation("author", "Martin"));
            assertEventually(cluster, () -> setFuture.isCompleted() && !setFuture.isFailed());
            
            // Get the value
            ListenableFuture<GetValueResponse> getFuture = client.getValue(BYZANTIUM, "author");
            assertEventually(cluster, () -> getFuture.isCompleted() && !getFuture.isFailed());
            
            var response = getFuture.getResult();
            assertEquals("Martin", response.value());
            
            System.out.println("\n=== Test passed: Get Value ===\n");
        }
    }
    
    @Test
    @DisplayName("Persistence: Log entries are stored in Storage")
    void testPersistence() throws IOException {
        System.out.println("\n=== TEST: Persistence ===\n");
        
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new PaxosLogServer(peerIds, processParams))
                .start()) {
            
            // Wait for all processes to initialize
            assertEventually(cluster, () -> cluster.areAllNodesInitialized());
            
            var client = cluster.newClientConnectedTo(CLIENT, ATHENS, PaxosLogClient::new);
            
            // Write multiple entries
            ListenableFuture<ExecuteCommandResponse> future1 = client.execute(ATHENS,
                new SetValueOperation("key1", "value1"));
            assertEventually(cluster, () -> future1.isCompleted() && !future1.isFailed());
            assertTrue(future1.getResult().success());
            
            ListenableFuture<ExecuteCommandResponse> future2 = client.execute(BYZANTIUM,
                new SetValueOperation("key2", "value2"));
            assertEventually(cluster, () -> future2.isCompleted() && !future2.isFailed());
            assertTrue(future2.getResult().success());
            
            // Verify all nodes have the entries in their log cache
            PaxosLogServer athens = getServer(cluster, ATHENS);
            PaxosLogServer byzantium = getServer(cluster, BYZANTIUM);
            PaxosLogServer cyrene = getServer(cluster, CYRENE);
            
            // Check that log entries exist in cache (they should be loaded from storage)
            var athensLog = athens.getPaxosLog();
            var byzantiumLog = byzantium.getPaxosLog();
            var cyreneLog = cyrene.getPaxosLog();
            
            // All nodes should have at least 2 entries (indices 0 and 1)
            assertTrue(athensLog.size() >= 2, "Athens should have at least 2 log entries");
            assertTrue(byzantiumLog.size() >= 2, "Byzantium should have at least 2 log entries");
            assertTrue(cyreneLog.size() >= 2, "Cyrene should have at least 2 log entries");
            
            // Verify high water mark
            assertEquals(1, athens.getHighWaterMark(), "High water mark should be 1");
            assertEquals(1, byzantium.getHighWaterMark(), "High water mark should be 1");
            assertEquals(1, cyrene.getHighWaterMark(), "High water mark should be 1");
            
            // Verify committed values
            var entry0 = athensLog.get(0);
            var entry1 = athensLog.get(1);
            assertNotNull(entry0, "Entry at index 0 should exist");
            assertNotNull(entry1, "Entry at index 1 should exist");
            assertTrue(entry0.committedValue().isPresent(), "Entry 0 should be committed");
            assertTrue(entry1.committedValue().isPresent(), "Entry 1 should be committed");
            
            System.out.println("\n=== Test passed: Persistence ===\n");
        }
    }
    
    @Test
    @DisplayName("Log Consistency: All nodes have same log entries")
    void testLogConsistency() throws IOException {
        System.out.println("\n=== TEST: Log Consistency ===\n");
        
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new PaxosLogServer(peerIds, processParams))
                .start()) {
            
            // Wait for all processes to initialize
            assertEventually(cluster, () -> cluster.areAllNodesInitialized());
            
            var client = cluster.newClientConnectedTo(CLIENT, ATHENS, PaxosLogClient::new);
            
            // Write 3 entries
            for (int i = 0; i < 3; i++) {
                ListenableFuture<ExecuteCommandResponse> future = client.execute(ATHENS,
                    new SetValueOperation("key" + i, "value" + i));
                assertEventually(cluster, () -> future.isCompleted() && !future.isFailed());
                assertTrue(future.getResult().success(), "Entry " + i + " should succeed");
            }
            
            // Verify all nodes have consistent logs
            PaxosLogServer athens = getServer(cluster, ATHENS);
            PaxosLogServer byzantium = getServer(cluster, BYZANTIUM);
            PaxosLogServer cyrene = getServer(cluster, CYRENE);
            
            // All nodes should have the same high water mark
            int hwm = athens.getHighWaterMark();
            assertEquals(hwm, byzantium.getHighWaterMark(), "Byzantium HWM should match Athens");
            assertEquals(hwm, cyrene.getHighWaterMark(), "Cyrene HWM should match Athens");
            assertEquals(2, hwm, "High water mark should be 2 (indices 0, 1, 2)");
            
            // All nodes should have committed entries at the same indices
            var athensLog = athens.getPaxosLog();
            var byzantiumLog = byzantium.getPaxosLog();
            var cyreneLog = cyrene.getPaxosLog();
            
            for (int i = 0; i <= hwm; i++) {
                var athensEntry = athensLog.get(i);
                var byzantiumEntry = byzantiumLog.get(i);
                var cyreneEntry = cyreneLog.get(i);
                
                assertNotNull(athensEntry, "Athens should have entry at index " + i);
                assertNotNull(byzantiumEntry, "Byzantium should have entry at index " + i);
                assertNotNull(cyreneEntry, "Cyrene should have entry at index " + i);
                
                // All should be committed
                assertTrue(athensEntry.committedValue().isPresent(), 
                    "Athens entry " + i + " should be committed");
                assertTrue(byzantiumEntry.committedValue().isPresent(), 
                    "Byzantium entry " + i + " should be committed");
                assertTrue(cyreneEntry.committedValue().isPresent(), 
                    "Cyrene entry " + i + " should be committed");
            }
            
            System.out.println("\n=== Test passed: Log Consistency ===\n");
        }
    }
    
    /**
     * Note: Full recovery testing (crash and restart) would require using RocksDbStorage
     * with a shared database path. With SimulatedStorage (in-memory), each cluster instance
     * gets a fresh storage, so we can't test cross-instance recovery.
     * 
     * To test full recovery:
     * 1. Use RocksDbStorage with a shared path
     * 2. Write entries in first cluster
     * 3. Close first cluster
     * 4. Create new cluster with same RocksDbStorage path
     * 5. Verify entries are recovered
     */
    
    private static PaxosLogServer getServer(Cluster cluster, ProcessId id) {
        return (PaxosLogServer) cluster.getProcess(id);
    }
}

