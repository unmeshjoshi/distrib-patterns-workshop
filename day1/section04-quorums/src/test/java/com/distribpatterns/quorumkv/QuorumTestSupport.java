package com.distribpatterns.quorumkv;

import com.tickloom.Process;
import com.tickloom.ProcessFactory;
import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.tickloom.testkit.ClusterAssertions.assertEventually;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class QuorumTestSupport {

    static final ProcessId ATHENS = ProcessId.of("athens");
    static final ProcessId BYZANTIUM = ProcessId.of("byzantium");
    static final ProcessId CYRENE = ProcessId.of("cyrene");
    static final ProcessId ALICE = ProcessId.of("alice");

    static final ProcessFactory SYNC_READ_REPAIR_FACTORY =
            (peerIds, processParams) -> new QuorumKVReplica(peerIds, processParams, true, false);

    static final ProcessFactory ASYNC_READ_REPAIR_FACTORY =
            (peerIds, processParams) -> new QuorumKVReplica(peerIds, processParams, true, true);

    private QuorumTestSupport() {
    }

    static QuorumKVClient newClient(Cluster cluster, ProcessId clientId, ProcessId connectedReplica) throws IOException {
        return cluster.newClientConnectedTo(clientId, connectedReplica, QuorumKVClient::new);
    }

    static void assertWriteFails(Cluster cluster, QuorumKVClient client, byte[] key, byte[] value) {
        var future = client.set(key, value);
        assertEventually(cluster, future::isFailed);
    }

    static void assertReadReturns(Cluster cluster, QuorumKVClient client, byte[] key, byte[] expectedValue) {
        var response = cluster.tickUntilComplete(client.get(key));
        assertTrue(response.found(), "Read should find a value");
        assertArrayEquals(expectedValue, response.value(), "Read should return the expected value");
    }

    static void assertReplicaEventuallyHasValue(Cluster cluster, ProcessId node, byte[] key, byte[] expectedValue) {
        cluster.tickUntil(() -> replicaHasValue(cluster, node, key, expectedValue));
    }

    static void assertAllReplicasEventuallyHaveValue(Cluster cluster, byte[] key, byte[] expectedValue) {
        assertReplicasEventuallyHaveValue(cluster, List.of(ATHENS, BYZANTIUM, CYRENE), key, expectedValue);
    }

    static void assertReplicasEventuallyHaveValue(Cluster cluster, List<ProcessId> nodes, byte[] key, byte[] expectedValue) {
        cluster.tickUntil(() -> nodes.stream().allMatch(node -> replicaHasValue(cluster, node, key, expectedValue)));
    }

    static VersionedValue storedValue(Cluster cluster, ProcessId node, byte[] key) {
        QuorumKVReplica process = (QuorumKVReplica) cluster.getProcess(node);
        return process.getDecodedValue(key);
    }

    static void waitForTicks(Cluster cluster, int tickCount) {
        for (int i = 0; i < tickCount; i++) {
            cluster.tick();
        }
    }

    private static boolean replicaHasValue(Cluster cluster, ProcessId node, byte[] key, byte[] expectedValue) {
        VersionedValue storedValue = storedValue(cluster, node, key);
        return storedValue != null && Arrays.equals(expectedValue, storedValue.value());
    }
}
