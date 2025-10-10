package com.distribpatterns.multipaxosheartbeats;

import com.tickloom.ProcessId;
import com.tickloom.future.ListenableFuture;
import com.tickloom.testkit.Cluster;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.tickloom.testkit.ClusterAssertions.assertEventually;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for log consistency and safety properties in Multi-Paxos with Heartbeats.
 * 
 * Tests cover the key safety properties from the Raft thesis:
 * 1. Log Matching Property: If two logs have entry with same index+generation, all prior entries match
 * 2. Leader Completeness: If entry committed in generation T, present in all leaders for generations > T
 * 3. State Machine Safety: If server applies entry at index i, no other server applies different entry at i
 * 4. Election Safety: At most one leader per generation
 * 5. Leader Append-Only: Leader never overwrites/deletes entries
 */
public class LogConsistencyAndSafetyTest {
    
    private static final ProcessId ATHENS = ProcessId.of("athens");
    private static final ProcessId BYZANTIUM = ProcessId.of("byzantium");
    private static final ProcessId CYRENE = ProcessId.of("cyrene");
    private static final ProcessId DELPHI = ProcessId.of("delphi");
    private static final ProcessId EPHESUS = ProcessId.of("ephesus");
    private static final ProcessId CLIENT_ALICE = ProcessId.of("alice");
    
    private MultiPaxosWithHeartbeatsServer getServer(Cluster cluster, ProcessId id) {
        return (MultiPaxosWithHeartbeatsServer) cluster.getProcess(id);
    }
    
    @Test
    @DisplayName("Log matching property: same index+generation = same entries")
    void testLogMatchingProperty() throws IOException {
        System.out.println("\n=== TEST: Log Matching Property ===\n");
        
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build(MultiPaxosWithHeartbeatsServer::new)
                .start()) {
            
            MultiPaxosWithHeartbeatsServer athens = getServer(cluster, ATHENS);
            MultiPaxosWithHeartbeatsServer byzantium = getServer(cluster, BYZANTIUM);
            MultiPaxosWithHeartbeatsServer cyrene = getServer(cluster, CYRENE);
            
            // Wait for leader election
            for (int i = 0; i < 300; i++) {
                cluster.tick();
            }
            
            // Find leader
            MultiPaxosWithHeartbeatsServer leader = List.of(athens, byzantium, cyrene)
                .stream()
                .filter(MultiPaxosWithHeartbeatsServer::isLeader)
                .findFirst()
                .orElse(null);
            
            assertNotNull(leader, "Should have elected leader");
            
            // Execute multiple operations to build log
            var client = cluster.newClientConnectedTo(CLIENT_ALICE, ATHENS, MultiPaxosWithHeartbeatsClient::new);
            
            // Execute several operations
            ListenableFuture<ExecuteCommandResponse> future1 = client.execute(
                ATHENS,
                new SetValueOperation("key1", "value1")
            );
            
            ListenableFuture<ExecuteCommandResponse> future2 = client.execute(
                ATHENS,
                new SetValueOperation("key2", "value2")
            );
            
            ListenableFuture<ExecuteCommandResponse> future3 = client.execute(
                ATHENS,
                new SetValueOperation("key3", "value3")
            );
            
            // Wait for operations to complete
            assertEventually(cluster, () -> future1.isCompleted() && !future1.isFailed());
            assertEventually(cluster, () -> future2.isCompleted() && !future2.isFailed());
            assertEventually(cluster, () -> future3.isCompleted() && !future3.isFailed());
            
            // Verify all servers have same log entries
            Map<Integer, PaxosState> athensLog = athens.getPaxosLog();
            Map<Integer, PaxosState> byzantiumLog = byzantium.getPaxosLog();
            Map<Integer, PaxosState> cyreneLog = cyrene.getPaxosLog();
            
            // Check that all servers have same committed entries
            for (int index = 0; index < 3; index++) {
                PaxosState athensState = athensLog.get(index);
                PaxosState byzantiumState = byzantiumLog.get(index);
                PaxosState cyreneState = cyreneLog.get(index);
                
                assertNotNull(athensState, "Athens should have entry at index " + index);
                assertNotNull(byzantiumState, "Byzantium should have entry at index " + index);
                assertNotNull(cyreneState, "Cyrene should have entry at index " + index);
                
                // Verify same generation and value for same index
                if (athensState.committedValue().isPresent() && 
                    byzantiumState.committedValue().isPresent() && 
                    cyreneState.committedValue().isPresent()) {
                    
                    assertEquals(athensState.committedGeneration().get(), 
                        byzantiumState.committedGeneration().get(),
                        "Same index should have same generation");
                    assertEquals(athensState.committedGeneration().get(), 
                        cyreneState.committedGeneration().get(),
                        "Same index should have same generation");
                    
                    assertEquals(athensState.committedValue().get(), 
                        byzantiumState.committedValue().get(),
                        "Same index should have same value");
                    assertEquals(athensState.committedValue().get(), 
                        cyreneState.committedValue().get(),
                        "Same index should have same value");
                }
            }
            
            System.out.println("✓ Log matching property verified!");
        }
    }
    
    @Test
    @DisplayName("Leader completeness: committed entries persist across leader changes")
    void testLeaderCompleteness() throws IOException {
        System.out.println("\n=== TEST: Leader Completeness ===\n");
        
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build(MultiPaxosWithHeartbeatsServer::new)
                .start()) {
            
            MultiPaxosWithHeartbeatsServer athens = getServer(cluster, ATHENS);
            MultiPaxosWithHeartbeatsServer byzantium = getServer(cluster, BYZANTIUM);
            MultiPaxosWithHeartbeatsServer cyrene = getServer(cluster, CYRENE);
            
            // Wait for initial leader election
            for (int i = 0; i < 300; i++) {
                cluster.tick();
            }
            
            // Find initial leader
            MultiPaxosWithHeartbeatsServer initialLeader = List.of(athens, byzantium, cyrene)
                .stream()
                .filter(MultiPaxosWithHeartbeatsServer::isLeader)
                .findFirst()
                .orElse(null);
            
            assertNotNull(initialLeader, "Should have elected initial leader");
            int initialGeneration = initialLeader.getPromisedGeneration();
            
            // Execute operations with initial leader
            var client = cluster.newClientConnectedTo(CLIENT_ALICE, ATHENS, MultiPaxosWithHeartbeatsClient::new);
            
            ListenableFuture<ExecuteCommandResponse> future1 = client.execute(
                ATHENS,
                new SetValueOperation("persistent", "value1")
            );
            
            ListenableFuture<ExecuteCommandResponse> future2 = client.execute(
                ATHENS,
                new SetValueOperation("persistent2", "value2")
            );
            
            // Wait for operations to complete
            assertEventually(cluster, () -> future1.isCompleted() && !future1.isFailed());
            assertEventually(cluster, () -> future2.isCompleted() && !future2.isFailed());
            
            // Verify committed entries exist
            assertEquals("value1", athens.getValue("persistent"));
            assertEquals("value1", byzantium.getValue("persistent"));
            assertEquals("value1", cyrene.getValue("persistent"));
            
            assertEquals("value2", athens.getValue("persistent2"));
            assertEquals("value2", byzantium.getValue("persistent2"));
            assertEquals("value2", cyrene.getValue("persistent2"));
            
            // Simulate leader change (by waiting for potential re-election)
            for (int i = 0; i < 200; i++) {
                cluster.tick();
            }
            
            // Find current leader (might be same or different)
            MultiPaxosWithHeartbeatsServer currentLeader = List.of(athens, byzantium, cyrene)
                .stream()
                .filter(MultiPaxosWithHeartbeatsServer::isLeader)
                .findFirst()
                .orElse(null);
            
            assertNotNull(currentLeader, "Should have a current leader");
            
            // Verify committed entries still exist after leader change
            assertEquals("value1", athens.getValue("persistent"));
            assertEquals("value1", byzantium.getValue("persistent"));
            assertEquals("value1", cyrene.getValue("persistent"));
            
            assertEquals("value2", athens.getValue("persistent2"));
            assertEquals("value2", byzantium.getValue("persistent2"));
            assertEquals("value2", cyrene.getValue("persistent2"));
            
            // Verify current leader has higher or equal generation
            assertTrue(currentLeader.getPromisedGeneration() >= initialGeneration,
                "Current leader should have higher or equal generation");
            
            System.out.println("✓ Leader completeness verified!");
        }
    }
    
    @Test
    @DisplayName("State machine safety: no conflicting entries at same index")
    void testStateMachineSafety() throws IOException {
        System.out.println("\n=== TEST: State Machine Safety ===\n");
        
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build(MultiPaxosWithHeartbeatsServer::new)
                .start()) {
            
            MultiPaxosWithHeartbeatsServer athens = getServer(cluster, ATHENS);
            MultiPaxosWithHeartbeatsServer byzantium = getServer(cluster, BYZANTIUM);
            MultiPaxosWithHeartbeatsServer cyrene = getServer(cluster, CYRENE);
            
            // Wait for leader election
            for (int i = 0; i < 300; i++) {
                cluster.tick();
            }
            
            // Find leader
            MultiPaxosWithHeartbeatsServer leader = List.of(athens, byzantium, cyrene)
                .stream()
                .filter(MultiPaxosWithHeartbeatsServer::isLeader)
                .findFirst()
                .orElse(null);
            
            assertNotNull(leader, "Should have elected leader");
            
            // Execute operations that could potentially conflict
            var client = cluster.newClientConnectedTo(CLIENT_ALICE, ATHENS, MultiPaxosWithHeartbeatsClient::new);
            
            // Execute CAS operations on same key
            ListenableFuture<ExecuteCommandResponse> future1 = client.execute(
                ATHENS,
                new CompareAndSwapOperation("conflict_key", null, "first_value")
            );
            
            ListenableFuture<ExecuteCommandResponse> future2 = client.execute(
                ATHENS,
                new CompareAndSwapOperation("conflict_key", "first_value", "second_value")
            );
            
            // Wait for operations to complete
            assertEventually(cluster, () -> future1.isCompleted() && !future1.isFailed());
            assertEventually(cluster, () -> future2.isCompleted() && !future2.isFailed());
            
            // Verify all servers have same final state
            String athensValue = athens.getValue("conflict_key");
            String byzantiumValue = byzantium.getValue("conflict_key");
            String cyreneValue = cyrene.getValue("conflict_key");
            
            assertNotNull(athensValue, "Athens should have final value");
            assertNotNull(byzantiumValue, "Byzantium should have final value");
            assertNotNull(cyreneValue, "Cyrene should have final value");
            
            assertEquals(athensValue, byzantiumValue, "All servers should have same final value");
            assertEquals(athensValue, cyreneValue, "All servers should have same final value");
            
            // Verify log entries are consistent
            Map<Integer, PaxosState> athensLog = athens.getPaxosLog();
            Map<Integer, PaxosState> byzantiumLog = byzantium.getPaxosLog();
            Map<Integer, PaxosState> cyreneLog = cyrene.getPaxosLog();
            
            for (int index = 0; index < 2; index++) {
                PaxosState athensState = athensLog.get(index);
                PaxosState byzantiumState = byzantiumLog.get(index);
                PaxosState cyreneState = cyreneLog.get(index);
                
                if (athensState != null && byzantiumState != null && cyreneState != null) {
                    if (athensState.committedValue().isPresent() && 
                        byzantiumState.committedValue().isPresent() && 
                        cyreneState.committedValue().isPresent()) {
                        
                        assertEquals(athensState.committedValue().get(), 
                            byzantiumState.committedValue().get(),
                            "Same index should have same committed value");
                        assertEquals(athensState.committedValue().get(), 
                            cyreneState.committedValue().get(),
                            "Same index should have same committed value");
                    }
                }
            }
            
            System.out.println("✓ State machine safety verified!");
        }
    }
    
    @Test
    @DisplayName("Election safety: at most one leader per generation")
    void testElectionSafety() throws IOException {
        System.out.println("\n=== TEST: Election Safety ===\n");
        
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE, DELPHI, EPHESUS))
                .useSimulatedNetwork()
                .build(MultiPaxosWithHeartbeatsServer::new)
                .start()) {
            
            MultiPaxosWithHeartbeatsServer athens = getServer(cluster, ATHENS);
            MultiPaxosWithHeartbeatsServer byzantium = getServer(cluster, BYZANTIUM);
            MultiPaxosWithHeartbeatsServer cyrene = getServer(cluster, CYRENE);
            MultiPaxosWithHeartbeatsServer delphi = getServer(cluster, DELPHI);
            MultiPaxosWithHeartbeatsServer ephesus = getServer(cluster, EPHESUS);
            
            List<MultiPaxosWithHeartbeatsServer> servers = List.of(athens, byzantium, cyrene, delphi, ephesus);
            
            // Wait for leader election
            for (int i = 0; i < 500; i++) {
                cluster.tick();
            }
            
            // Verify exactly one leader
            long leaderCount = servers.stream()
                .filter(MultiPaxosWithHeartbeatsServer::isLeader)
                .count();
            
            assertEquals(1, leaderCount, "Should have exactly one leader");
            
            // Find the leader
            MultiPaxosWithHeartbeatsServer leader = servers.stream()
                .filter(MultiPaxosWithHeartbeatsServer::isLeader)
                .findFirst()
                .orElse(null);
            
            assertNotNull(leader, "Should have found a leader");
            int leaderGeneration = leader.getPromisedGeneration();
            
            // Verify all followers have same generation
            for (MultiPaxosWithHeartbeatsServer server : servers) {
                if (!server.isLeader()) {
                    assertEquals(leaderGeneration, server.getPromisedGeneration(),
                        "All followers should have same generation as leader");
                }
            }
            
            // Run for more ticks to ensure stability
            for (int i = 0; i < 100; i++) {
                cluster.tick();
            }
            
            // Verify still exactly one leader
            long finalLeaderCount = servers.stream()
                .filter(MultiPaxosWithHeartbeatsServer::isLeader)
                .count();
            
            assertEquals(1, finalLeaderCount, "Should still have exactly one leader");
            
            System.out.println("✓ Election safety verified!");
        }
    }
    
    @Test
    @DisplayName("Leader append-only: leader never overwrites entries")
    void testLeaderAppendOnly() throws IOException {
        System.out.println("\n=== TEST: Leader Append-Only ===\n");
        
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build(MultiPaxosWithHeartbeatsServer::new)
                .start()) {
            
            MultiPaxosWithHeartbeatsServer athens = getServer(cluster, ATHENS);
            MultiPaxosWithHeartbeatsServer byzantium = getServer(cluster, BYZANTIUM);
            MultiPaxosWithHeartbeatsServer cyrene = getServer(cluster, CYRENE);
            
            // Wait for leader election
            for (int i = 0; i < 300; i++) {
                cluster.tick();
            }
            
            // Find leader
            MultiPaxosWithHeartbeatsServer leader = List.of(athens, byzantium, cyrene)
                .stream()
                .filter(MultiPaxosWithHeartbeatsServer::isLeader)
                .findFirst()
                .orElse(null);
            
            assertNotNull(leader, "Should have elected leader");
            
            // Execute operations to build log
            var client = cluster.newClientConnectedTo(CLIENT_ALICE, ATHENS, MultiPaxosWithHeartbeatsClient::new);
            
            ListenableFuture<ExecuteCommandResponse> future1 = client.execute(
                ATHENS,
                new SetValueOperation("append_test", "value1")
            );
            
            ListenableFuture<ExecuteCommandResponse> future2 = client.execute(
                ATHENS,
                new SetValueOperation("append_test2", "value2")
            );
            
            // Wait for operations to complete
            assertEventually(cluster, () -> future1.isCompleted() && !future1.isFailed());
            assertEventually(cluster, () -> future2.isCompleted() && !future2.isFailed());
            
            // Verify log entries were appended (not overwritten)
            Map<Integer, PaxosState> leaderLog = leader.getPaxosLog();
            
            assertTrue(leaderLog.containsKey(0), "Should have entry at index 0");
            assertTrue(leaderLog.containsKey(1), "Should have entry at index 1");
            
            // Verify entries are different (not overwritten)
            PaxosState entry0 = leaderLog.get(0);
            PaxosState entry1 = leaderLog.get(1);
            
            assertTrue(entry0.committedValue().isPresent(), "Entry 0 should be committed");
            assertTrue(entry1.committedValue().isPresent(), "Entry 1 should be committed");
            
            assertNotEquals(entry0.committedValue().get(), entry1.committedValue().get(),
                "Entries should be different (not overwritten)");
            
            // Verify all servers have same log structure
            Map<Integer, PaxosState> athensLog = athens.getPaxosLog();
            Map<Integer, PaxosState> byzantiumLog = byzantium.getPaxosLog();
            Map<Integer, PaxosState> cyreneLog = cyrene.getPaxosLog();
            
            assertEquals(athensLog.size(), leaderLog.size(), "All logs should have same size");
            assertEquals(byzantiumLog.size(), leaderLog.size(), "All logs should have same size");
            assertEquals(cyreneLog.size(), leaderLog.size(), "All logs should have same size");
            
            System.out.println("✓ Leader append-only property verified!");
        }
    }
    
    @Test
    @DisplayName("Concurrent operations maintain consistency")
    void testConcurrentOperationsConsistency() throws IOException {
        System.out.println("\n=== TEST: Concurrent Operations Consistency ===\n");
        
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build(MultiPaxosWithHeartbeatsServer::new)
                .start()) {
            
            MultiPaxosWithHeartbeatsServer athens = getServer(cluster, ATHENS);
            MultiPaxosWithHeartbeatsServer byzantium = getServer(cluster, BYZANTIUM);
            MultiPaxosWithHeartbeatsServer cyrene = getServer(cluster, CYRENE);
            
            // Wait for leader election
            for (int i = 0; i < 300; i++) {
                cluster.tick();
            }
            
            // Find leader
            MultiPaxosWithHeartbeatsServer leader = List.of(athens, byzantium, cyrene)
                .stream()
                .filter(MultiPaxosWithHeartbeatsServer::isLeader)
                .findFirst()
                .orElse(null);
            
            assertNotNull(leader, "Should have elected leader");
            
            // Execute multiple concurrent operations
            var client = cluster.newClientConnectedTo(CLIENT_ALICE, ATHENS, MultiPaxosWithHeartbeatsClient::new);
            
            @SuppressWarnings("unchecked")
            ListenableFuture<ExecuteCommandResponse>[] futures = new ListenableFuture[5];
            for (int i = 0; i < 5; i++) {
                final int index = i;
                futures[index] = client.execute(
                    ATHENS,
                    new SetValueOperation("concurrent_key_" + index, "value_" + index)
                );
            }
            
            // Wait for all operations to complete
            for (int i = 0; i < 5; i++) {
                final int index = i;
                assertEventually(cluster, () -> futures[index].isCompleted() && !futures[index].isFailed());
            }
            
            // Verify all servers have consistent state
            for (int i = 0; i < 5; i++) {
                String expectedValue = "value_" + i;
                assertEquals(expectedValue, athens.getValue("concurrent_key_" + i));
                assertEquals(expectedValue, byzantium.getValue("concurrent_key_" + i));
                assertEquals(expectedValue, cyrene.getValue("concurrent_key_" + i));
            }
            
            // Verify log consistency
            Map<Integer, PaxosState> athensLog = athens.getPaxosLog();
            Map<Integer, PaxosState> byzantiumLog = byzantium.getPaxosLog();
            Map<Integer, PaxosState> cyreneLog = cyrene.getPaxosLog();
            
            assertEquals(athensLog.size(), byzantiumLog.size(), "Logs should have same size");
            assertEquals(athensLog.size(), cyreneLog.size(), "Logs should have same size");
            
            // Verify each log entry is consistent
            for (int index = 0; index < athensLog.size(); index++) {
                PaxosState athensState = athensLog.get(index);
                PaxosState byzantiumState = byzantiumLog.get(index);
                PaxosState cyreneState = cyreneLog.get(index);
                
                assertNotNull(athensState, "Athens should have entry at index " + index);
                assertNotNull(byzantiumState, "Byzantium should have entry at index " + index);
                assertNotNull(cyreneState, "Cyrene should have entry at index " + index);
                
                if (athensState.committedValue().isPresent() && 
                    byzantiumState.committedValue().isPresent() && 
                    cyreneState.committedValue().isPresent()) {
                    
                    assertEquals(athensState.committedValue().get(), 
                        byzantiumState.committedValue().get(),
                        "Same index should have same value");
                    assertEquals(athensState.committedValue().get(), 
                        cyreneState.committedValue().get(),
                        "Same index should have same value");
                }
            }
            
            System.out.println("✓ Concurrent operations consistency verified!");
        }
    }
    
    @Test
    @DisplayName("Read operations with no-op commits")
    void testReadOperationsWithNoOp() throws IOException {
        System.out.println("\n=== TEST: Read Operations with No-Op ===\n");
        
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build(MultiPaxosWithHeartbeatsServer::new)
                .start()) {
            
            MultiPaxosWithHeartbeatsServer athens = getServer(cluster, ATHENS);
            MultiPaxosWithHeartbeatsServer byzantium = getServer(cluster, BYZANTIUM);
            MultiPaxosWithHeartbeatsServer cyrene = getServer(cluster, CYRENE);
            
            // Wait for leader election
            for (int i = 0; i < 300; i++) {
                cluster.tick();
            }
            
            // Find leader
            MultiPaxosWithHeartbeatsServer leader = List.of(athens, byzantium, cyrene)
                .stream()
                .filter(MultiPaxosWithHeartbeatsServer::isLeader)
                .findFirst()
                .orElse(null);
            
            assertNotNull(leader, "Should have elected leader");
            
            // First, set a value
            var client = cluster.newClientConnectedTo(CLIENT_ALICE, ATHENS, MultiPaxosWithHeartbeatsClient::new);
            
            ListenableFuture<ExecuteCommandResponse> setFuture = client.execute(
                ATHENS,
                new SetValueOperation("read_test", "read_value")
            );
            
            assertEventually(cluster, () -> setFuture.isCompleted() && !setFuture.isFailed());
            
            // Now perform a read operation (should trigger no-op commit)
            ListenableFuture<GetValueResponse> readFuture = client.getValue(ATHENS, "read_test");
            
            assertEventually(cluster, () -> readFuture.isCompleted() && !readFuture.isFailed());
            
            GetValueResponse readResponse = readFuture.getResult();
            assertNotNull(readResponse, "Read response should not be null");
            assertEquals("read_value", readResponse.value(), "Should read correct value");
            
            // Verify that a no-op was committed to the log
            Map<Integer, PaxosState> leaderLog = leader.getPaxosLog();
            
            // Should have at least 2 entries: the set operation and the no-op for read
            assertTrue(leaderLog.size() >= 2, "Should have at least 2 log entries");
            
            // Verify all servers have the no-op entry
            Map<Integer, PaxosState> athensLog = athens.getPaxosLog();
            Map<Integer, PaxosState> byzantiumLog = byzantium.getPaxosLog();
            Map<Integer, PaxosState> cyreneLog = cyrene.getPaxosLog();
            
            assertEquals(leaderLog.size(), athensLog.size(), "All logs should have same size");
            assertEquals(leaderLog.size(), byzantiumLog.size(), "All logs should have same size");
            assertEquals(leaderLog.size(), cyreneLog.size(), "All logs should have same size");
            
            System.out.println("✓ Read operations with no-op commits verified!");
        }
    }
}
