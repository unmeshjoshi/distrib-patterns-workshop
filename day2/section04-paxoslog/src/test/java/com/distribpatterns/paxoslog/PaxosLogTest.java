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
    
    private static PaxosLogServer getServer(Cluster cluster, ProcessId id) {
        return (PaxosLogServer) cluster.getProcess(id);
    }
}

