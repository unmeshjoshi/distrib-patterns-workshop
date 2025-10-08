package com.distribpatterns.twophase;

import com.tickloom.ProcessId;
import com.tickloom.future.ListenableFuture;
import com.tickloom.testkit.ClusterTest;
import com.tickloom.testkit.NodeGroup;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static com.distribpatterns.naive.NaiveReplicationServerTest.getServerInstance;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Enhanced Three-Phase Commit recovery test using ClusterTest base class.
 * 
 * This test uses delayForMessageType() to deterministically drop COMMIT messages,
 * ensuring the recovery scenario is reliably tested every time.
 */
public class ThreePhaseServerRecoveryTest extends ClusterTest<ThreePhaseClient, ExecuteResponse, Integer> {
    
    // Replica nodes
    private static final ProcessId ATHENS = ProcessId.of("athens");
    private static final ProcessId BYZANTIUM = ProcessId.of("byzantium");
    private static final ProcessId CYRENE = ProcessId.of("cyrene");
    
    // Clients
    private static final ProcessId ALICE = ProcessId.of("alice");
    private static final ProcessId BOB = ProcessId.of("bob");
    
    public ThreePhaseServerRecoveryTest() throws IOException {
        super(
            List.of(ATHENS, BYZANTIUM, CYRENE),
            ThreePhaseServer::new,
            ThreePhaseClient::new,
            response -> response == null ? null : response.newValue()
        );
    }
    
    @Test
    @DisplayName("Deterministic Recovery Test: Drop COMMIT messages, new coordinator recovers")
    void testDeterministicCoordinatorFailureRecovery() {
        // Step 1: Alice (Client-1) sends increment operation to Athens (Coordinator-1)
        System.out.println("\n=== Step 1: Client-1 (Alice) sends INCREMENT +10 to Coordinator-1 (Athens) ===");
        var aliceClient = clientConnectedTo(ALICE, ATHENS);
        
        // Step 2: Configure network to DROP COMMIT messages from Athens
        // This ensures ACCEPT phase completes but COMMIT never arrives
        System.out.println("\n=== Step 2: Configure network to DROP COMMIT messages from Athens ===");
        System.out.println("        This simulates coordinator crash AFTER ACCEPT but BEFORE COMMIT");
        
        delayForMessageType(
            TwoPhaseMessageTypes.COMMIT_REQUEST,    // Message type to drop
            ATHENS,                                  // Source (Coordinator-1)
            List.of(BYZANTIUM, CYRENE),             // Destinations (Participants)
            Integer.MAX_VALUE                        // Infinite delay = effectively dropped
        );
        
        // Step 3: Send request - QUERY and ACCEPT will complete, but COMMIT will be dropped
        System.out.println("\n=== Step 3: Send request - ACCEPT completes, COMMIT is dropped ===");
        ListenableFuture<ExecuteResponse> aliceFuture = aliceClient.execute(ATHENS, 
            new IncrementCounterOperation("counter_key", 10));
        
        // Let QUERY and ACCEPT phases complete (but COMMIT is blocked by our delay)
        for (int i = 0; i < 100; i++) {
            cluster.tick();
        }
        
        // Step 4: Verify participants have NOT executed yet (waiting for COMMIT)
        System.out.println("\n=== Step 4: Verify participants prepared but NOT executed (COMMIT dropped) ===");
        ThreePhaseServer byzantiumBefore = getServerInstance(cluster, BYZANTIUM);
        ThreePhaseServer cyreneBefore = getServerInstance(cluster, CYRENE);
        
        assertNull(byzantiumBefore.getCounterValue("counter_key"),
                  "Byzantium should NOT have executed (COMMIT dropped)");
        assertNull(cyreneBefore.getCounterValue("counter_key"),
                  "Cyrene should NOT have executed (COMMIT dropped)");
        
        System.out.println("        ✓ Confirmed: Nodes have ACCEPTED but not EXECUTED");
        System.out.println("        ✓ This is the orphaned request scenario!");
        
        // Step 5: Simulate Athens crash by partitioning it away
        System.out.println("\n=== Step 5: Athens (Coordinator-1) crashes (partition) ===");
        partition(
            NodeGroup.of(ATHENS),                    // Crashed coordinator
            NodeGroup.of(BYZANTIUM, CYRENE)         // Still-alive participants
        );
        
        // Step 6: New client (Bob) sends request to new coordinator (Byzantium)
        System.out.println("\n=== Step 6: Client-2 (Bob) sends NEW request to Coordinator-2 (Byzantium) ===");
        var bobClient = clientConnectedTo(BOB, BYZANTIUM);
        
        ListenableFuture<ExecuteResponse> bobFuture = bobClient.execute(BYZANTIUM, 
            new IncrementCounterOperation("counter_key", 20));
        
        // Step 7: Byzantium runs QUERY phase and discovers orphaned request
        System.out.println("\n=== Step 7: Byzantium discovers orphaned request via QUERY ===");
        System.out.println("        Phase 0 (QUERY): Byzantium asks all nodes for pending requests");
        System.out.println("        Response: 'counter_key +10' is pending (accepted but not committed)");
        System.out.println("        Decision: Enter RECOVERY MODE");
        
        // Step 8: Wait for recovery to complete
        System.out.println("\n=== Step 8: Recovery: Re-run ACCEPT → COMMIT for orphaned request ===");
        assertEventually(() -> {
            ThreePhaseServer byzantiumServer = getServerInstance(cluster, BYZANTIUM);
            ThreePhaseServer cyreneServer = getServerInstance(cluster, CYRENE);
            
            // Recovery should complete the FIRST request (counter_key = 10, NOT Bob's 20)
            return byzantiumServer.getCounterValue("counter_key") != null &&
                   cyreneServer.getCounterValue("counter_key") != null;
        });
        
        // Step 9: Verify the RECOVERED operation was executed (10, not Bob's 20)
        System.out.println("\n=== Step 9: Verify recovery completed correctly ===");
        ThreePhaseServer byzantiumServer = getServerInstance(cluster, BYZANTIUM);
        ThreePhaseServer cyreneServer = getServerInstance(cluster, CYRENE);
        
        assertEquals(10, byzantiumServer.getCounterValue("counter_key"), 
                    "Byzantium should have executed RECOVERED operation (+10, NOT Bob's +20)");
        assertEquals(10, cyreneServer.getCounterValue("counter_key"), 
                    "Cyrene should have executed RECOVERED operation (+10, NOT Bob's +20)");
        
        System.out.println("\n=== SUCCESS: Three-Phase Commit recovered from coordinator failure! ===");
        System.out.println("        ✓ Deterministic test using delayForMessageType()");
        System.out.println("        ✓ Orphaned accepted request (+10) was recovered and committed");
        System.out.println("        ✓ New client request (+20) was ignored during recovery");
        System.out.println("        ✓ System is non-blocking (no stuck participants)");
    }
}

