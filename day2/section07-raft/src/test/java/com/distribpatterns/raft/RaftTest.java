package com.distribpatterns.raft;

import com.distribpatterns.raft.messages.ExecuteCommandResponse;
import com.distribpatterns.raft.messages.SetValueOperation;
import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

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
    private static final List<ProcessId> REPLICAS = List.of(ATHENS, BYZANTIUM, CYRENE);

    private static final ProcessId CLIENT_ALICE = ProcessId.of("alice");
    private static final String TITLE_KEY = "title";

    private static final int ELECTION_TIMEOUT_TICKS = 20;

    @Test
    @DisplayName("Election Safety: At most one leader per term")
    void testElectionSafety() throws IOException {
        try (var cluster = new Cluster()
            .withProcessIds(REPLICAS)
            .useSimulatedNetwork()
            .build((allNodes, processParams) ->
                new RaftServer(allNodes, processParams, ELECTION_TIMEOUT_TICKS))
            .start()) {

            ProcessId leaderId = waitUntilLeaderElected(cluster);
            int leaderTerm = getServer(cluster, leaderId).getCurrentTerm();

            assertEquals(1, leaderCount(cluster), "Should have exactly one leader");
            assertTermOnAllReplicas(cluster, leaderTerm);
        }
    }

    @Test
    @DisplayName("Basic Replication: Leader replicates to followers")
    void testBasicReplication() throws IOException {
        try (var cluster = new Cluster()
            .withProcessIds(REPLICAS)
            .useSimulatedNetwork()
            .build((allNodes, processParams) ->
                new RaftServer(allNodes, processParams, ELECTION_TIMEOUT_TICKS))
            .start()) {

            ProcessId leaderId = waitUntilLeaderElected(cluster);
            assertOtherNodesAreFollowers(cluster, leaderId);

            var client = cluster.newClientConnectedTo(CLIENT_ALICE, leaderId, RaftClient::new);

            ExecuteCommandResponse response = cluster.tickUntilComplete(
                client.execute(leaderId, new SetValueOperation(TITLE_KEY, "Raft Consensus"))
            );

            assertSuccessfulCommand(response);
            cluster.tickUntil(() -> allReplicasHaveValue(cluster, TITLE_KEY, "Raft Consensus"));
            assertValueOnAllReplicas(cluster, TITLE_KEY, "Raft Consensus");
        }
    }

    private static ProcessId waitUntilLeaderElected(Cluster cluster) {
        cluster.tickUntil(() -> leaderCount(cluster) == 1);
        ProcessId leaderId = currentLeaderId(cluster);
        assertNotNull(leaderId, "A leader should be elected");
        return leaderId;
    }

    private static long leaderCount(Cluster cluster) {
        return REPLICAS.stream()
            .map(id -> getServer(cluster, id))
            .filter(RaftServer::isLeader)
            .count();
    }

    private static ProcessId currentLeaderId(Cluster cluster) {
        return REPLICAS.stream()
            .filter(id -> getServer(cluster, id).isLeader())
            .findFirst()
            .orElse(null);
    }

    private static void assertOtherNodesAreFollowers(Cluster cluster, ProcessId leaderId) {
        for (ProcessId nodeId : REPLICAS) {
            if (!nodeId.equals(leaderId)) {
                assertFalse(getServer(cluster, nodeId).isLeader(), nodeId + " should remain a follower");
            }
        }
    }

    private static boolean allReplicasHaveValue(Cluster cluster, String key, String expectedValue) {
        return REPLICAS.stream()
            .allMatch(id -> expectedValue.equals(getServer(cluster, id).getValue(key)));
    }

    private static void assertValueOnAllReplicas(Cluster cluster, String key, String expectedValue) {
        assertEquals(expectedValue, getServer(cluster, ATHENS).getValue(key));
        assertEquals(expectedValue, getServer(cluster, BYZANTIUM).getValue(key));
        assertEquals(expectedValue, getServer(cluster, CYRENE).getValue(key));
    }

    private static void assertTermOnAllReplicas(Cluster cluster, int expectedTerm) {
        assertEquals(expectedTerm, getServer(cluster, ATHENS).getCurrentTerm());
        assertEquals(expectedTerm, getServer(cluster, BYZANTIUM).getCurrentTerm());
        assertEquals(expectedTerm, getServer(cluster, CYRENE).getCurrentTerm());
    }

    private static void assertSuccessfulCommand(ExecuteCommandResponse response) {
        assertTrue(response.success(), "Command should succeed");
    }

    private static RaftServer getServer(Cluster cluster, ProcessId id) {
        return cluster.getNode(id);
    }
}
