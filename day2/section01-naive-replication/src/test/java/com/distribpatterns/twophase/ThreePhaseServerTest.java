package com.distribpatterns.twophase;

import com.distribpatterns.twophase.messages.ExecuteResponse;
import com.distribpatterns.twophase.messages.TwoPhaseMessageTypes;
import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import com.tickloom.testkit.NodeGroup;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests for Three-Phase Commit with coordinator failure recovery.
 */
class ThreePhaseServerTest {
    // Replica nodes (matching scenario descriptions)
    private static final ProcessId ATHENS = ProcessId.of("athens");
    private static final ProcessId BYZANTIUM = ProcessId.of("byzantium");
    private static final ProcessId CYRENE = ProcessId.of("cyrene");

    // Clients (actors from scenarios)
    private static final ProcessId ALICE = ProcessId.of("alice");
    private static final ProcessId BOB = ProcessId.of("bob");
    private static final String COUNTER_KEY = "counter_key";

    @Test
    @DisplayName("Scenario 1: Three-Phase Commit Happy Path - Normal operation (QUERY→ACCEPT→COMMIT)")
    void testThreePhaseHappyPath() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new ThreePhaseServer(peerIds, processParams))
                .start()) {

            var client = cluster.newClientConnectedTo(ALICE, ATHENS, ThreePhaseClient::new);

            // Alice sends increment operation to coordinator Athens
            // Phase 0 (QUERY): Athens queries all nodes for pending requests → none found
            // Phase 1 (ACCEPT): Athens broadcasts ACCEPT to all nodes → quorum reached
            // Phase 2 (COMMIT): Athens broadcasts COMMIT to all nodes → all execute

            // Drive the simulated cluster until the three-phase workflow completes.
            cluster.tickUntilComplete(client.execute(ATHENS,
                    new IncrementCounterOperation(COUNTER_KEY, 5)));

            // Verify all replicas have the same value
            assertCounterValue(cluster, ATHENS, 5);
            assertCounterValue(cluster, BYZANTIUM, 5);
            assertCounterValue(cluster, CYRENE, 5);
        }
    }

    @Test
    @DisplayName("Scenario 2: Three-Phase Multiple Operations")
    void testThreePhaseMultipleOperations() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new ThreePhaseServer(peerIds, processParams))
                .start()) {

            var client = cluster.newClientConnectedTo(ALICE, ATHENS, ThreePhaseClient::new);

            // These requests are sequential: the second request starts only
            // after the first three-phase workflow has completed.
            // First operation: increment by 3
            cluster.tickUntilComplete(client.execute(ATHENS,
                    new IncrementCounterOperation(COUNTER_KEY, 3)));

            // Second operation: increment by 7
            cluster.tickUntilComplete(client.execute(ATHENS,
                    new IncrementCounterOperation(COUNTER_KEY, 7)));

            // Verify all replicas have the cumulative value (3 + 7 = 10)
            assertCounterValue(cluster, ATHENS, 10);
            assertCounterValue(cluster, BYZANTIUM, 10);
            assertCounterValue(cluster, CYRENE, 10);
        }
    }

    @Test
    @DisplayName("Scenario 3: Three-Phase Recovery - drop Athens COMMIT, new coordinator recovers orphaned request")
    void testThreePhaseCoordinatorFailureRecovery() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new ThreePhaseServer(peerIds, processParams))
                .start()) {

            // Step 1: Alice sends +10 to Athens, the initial coordinator.
            var aliceClient = cluster.newClientConnectedTo(ALICE, ATHENS, ThreePhaseClient::new);

            // Step 2: Drop only Athens' COMMIT messages to the followers.
            // QUERY and ACCEPT still succeed, which leaves an orphaned accepted request.
            cluster.dropMessagesOfType(
                    TwoPhaseMessageTypes.COMMIT_REQUEST,
                    NodeGroup.of(ATHENS),
                    NodeGroup.of(BYZANTIUM, CYRENE)
            );

            // Step 3: Let Athens execute locally while follower COMMIT delivery remains blocked.
            aliceClient.execute(
                    ATHENS,
                    new IncrementCounterOperation(COUNTER_KEY, 10)
            );

            cluster.tickUntil(() -> hasCounterValue(cluster, ATHENS, 10));

            // Step 4: The followers accepted the request, but without COMMIT they still cannot execute it.
            assertCounterMissing(cluster, BYZANTIUM);
            assertCounterMissing(cluster, CYRENE);

            // Step 5: Isolate Athens to model the original coordinator crashing after ACCEPT.
            cluster.partitionNodes(
                    NodeGroup.of(ATHENS),           // Athens isolated (crashed)
                    NodeGroup.of(BYZANTIUM, CYRENE) // Majority still connected
            );

            // Step 6: Bob sends a new +20 request to Byzantium, the new coordinator.
            // Byzantium must recover the orphaned +10 request before it can make progress.
            var bobClient = cluster.newClientConnectedTo(BOB, BYZANTIUM, ThreePhaseClient::new);
            bobClient.execute(
                    BYZANTIUM,
                    new IncrementCounterOperation(COUNTER_KEY, 20)
            );

            // Step 7: Byzantium first recovers the orphaned +10 request.
            // Recovery re-runs ACCEPT -> COMMIT for that prepared request,
            // and Bob's new +20 request is ignored during recovery.
            cluster.tickUntil(() -> hasCounterValue(cluster, BYZANTIUM, 10)
                    && hasCounterValue(cluster, CYRENE, 10));

            // Teaching summary:
            // 1. Only Athens' COMMIT messages to the followers were blocked.
            // 2. Byzantium and Cyrene kept the request in prepared state.
            // 3. Recovery committed the orphaned +10 request.
            // 4. Bob's new +20 request was ignored during recovery.
            // 5. The surviving majority remained non-blocking.
        }
    }

    private static void assertCounterValue(Cluster cluster, ProcessId nodeId, int expectedValue) {
        ThreePhaseServer node = cluster.getNode(nodeId);
        assertEquals(Integer.valueOf(expectedValue), node.getCounterValue(COUNTER_KEY));
    }

    private static void assertCounterMissing(Cluster cluster, ProcessId nodeId) {
        ThreePhaseServer node = cluster.getNode(nodeId);
        assertNull(node.getCounterValue(COUNTER_KEY));
    }

    private static boolean hasCounterValue(Cluster cluster, ProcessId nodeId, int expectedValue) {
        ThreePhaseServer node = cluster.getNode(nodeId);
        return Integer.valueOf(expectedValue).equals(node.getCounterValue(COUNTER_KEY));
    }
}
