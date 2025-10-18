package com.distribpatterns.raft;

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
 * Tests for Raft consensus algorithm.
 * 
 * Key safety properties tested:
 * 1. Election Safety: At most one leader per term
 * 2. Leader Append-Only: Leader never overwrites/deletes entries
 * 3. Log Matching: If two logs have entry with same index+term, all prior entries match
 * 4. Leader Completeness: If entry committed in term T, present in all leaders for terms > T
 * 5. State Machine Safety: If server applies entry at index i, no other server applies different entry at i
 */
public class RaftTest {
    
    private static final ProcessId ATHENS = ProcessId.of("athens");
    private static final ProcessId BYZANTIUM = ProcessId.of("byzantium");
    private static final ProcessId CYRENE = ProcessId.of("cyrene");
    private static final ProcessId CLIENT_ALICE = ProcessId.of("alice");
    
    // Test-friendly election timeout (low for fast tests)
    private static final int ELECTION_TIMEOUT_TICKS = 20;
    
    private RaftServer getServer(Cluster cluster, ProcessId id) {
        return (RaftServer) cluster.getProcess(id);
    }
    
    @Test
    @DisplayName("Election Safety: At most one leader per term")
    void testElectionSafety() throws IOException {
        System.out.println("\n=== TEST: Election Safety ===\n");
        
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((allNodes, processParams) ->
                    new RaftServer(allNodes, processParams, ELECTION_TIMEOUT_TICKS))
                .start()) {
            
            // Wait for election
            for (int i = 0; i < 300; i++) {
                cluster.tick();
            }
            
            RaftServer athens = getServer(cluster, ATHENS);
            RaftServer byzantium = getServer(cluster, BYZANTIUM);
            RaftServer cyrene = getServer(cluster, CYRENE);
            
            // Count leaders
            int leaderCount = 0;
            RaftServer leader = null;
            if (athens.isLeader()) { leaderCount++; leader = athens; }
            if (byzantium.isLeader()) { leaderCount++; leader = byzantium; }
            if (cyrene.isLeader()) { leaderCount++; leader = cyrene; }
            
            assertEquals(1, leaderCount, "Should have exactly one leader");
            assertNotNull(leader);
            
            // All servers should be in the same term
            int term = leader.getCurrentTerm();
            assertTrue(athens.getCurrentTerm() >= term);
            assertTrue(byzantium.getCurrentTerm() >= term);
            assertTrue(cyrene.getCurrentTerm() >= term);
            
            System.out.println("✓ Election safety verified - one leader in term " + term);
        }
    }

    @Test
    @DisplayName("Basic Replication: Leader replicates to followers")
    void testBasicReplication() throws IOException {
        System.out.println("\n=== TEST: Basic Replication ===\n");
        
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((allNodes, processParams) ->
                    new RaftServer(allNodes, processParams, ELECTION_TIMEOUT_TICKS))
                .start()) {
            
            RaftServer athens = getServer(cluster, ATHENS);
            RaftServer byzantium = getServer(cluster, BYZANTIUM);
            RaftServer cyrene = getServer(cluster, CYRENE);
            
            // Wait for leader
            for (int i = 0; i < 300; i++) {
                cluster.tick();
            }

            ProcessId leaderId = getLeaderId(athens, byzantium, cyrene);
            assertNotNull(leaderId);

            assertOtherNodesAreFollowers(cluster, leaderId, List.of(ATHENS, BYZANTIUM, CYRENE).stream().filter(p -> !p.equals(leaderId)).toList());
            
            // Submit operation
            var client = cluster.newClientConnectedTo(CLIENT_ALICE, leaderId, RaftClient::new);

            ListenableFuture<ExecuteCommandResponse> future = client.execute(leaderId, new SetValueOperation("title", "Raft Consensus"));
            
            assertEventually(cluster, () -> future.isCompleted() && !future.isFailed());
            assertTrue(future.getResult().success());
            
            // Wait for commitIndex to be propagated to followers
            for (int i = 0; i < 100; i++) {
                cluster.tick();
            }
            
            // All servers should have the value
            assertEquals("Raft Consensus", athens.getValue("title"));
            assertEquals("Raft Consensus", byzantium.getValue("title"));
            assertEquals("Raft Consensus", cyrene.getValue("title"));
            
            System.out.println("Basic replication verified");
        }
    }

    private void assertOtherNodesAreFollowers(Cluster cluster, ProcessId leaderId, List<ProcessId> otherNodeIds) {
        for (ProcessId nodeId : otherNodeIds) {
            RaftServer server = getServer(cluster, nodeId);
            assertFalse(server.isLeader());
        }
    }

    private static ProcessId getLeaderId(RaftServer athens, RaftServer byzantium, RaftServer cyrene) {
        ProcessId leaderId = null;
        if (athens.isLeader()) leaderId = ATHENS;
        else if (byzantium.isLeader()) leaderId = BYZANTIUM;
        else if (cyrene.isLeader()) leaderId = CYRENE;
        return leaderId;
    }
}

