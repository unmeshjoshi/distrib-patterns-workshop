package com.distribpatterns.quorumkv;

import com.tickloom.ProcessId;
import com.tickloom.testkit.NodeGroup;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static com.distribpatterns.quorumkv.QuorumTestSupport.*;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for read repair scenarios.
 * These tests verify both synchronous and asynchronous read repair functionality.
 */
public class ReadRepairScenariosTest {

    @Test
    @DisplayName("Scenario 3: Read Repair - Coordinator detects stale nodes and synchronizes all replicas ✅")
    void testSyncReadRepair() throws IOException {
        try (var cluster = newCluster(SYNC_READ_REPAIR_FACTORY)) {
            var client = newClient(cluster);

            byte[] key = "user:profile".getBytes();
            byte[] value = "Alice".getBytes();
            byte[] newValue = "Bob".getBytes();

            assertSuccessfulWrite(cluster, client, key, value);
            assertAllReplicasEventuallyHaveValue(cluster, key, value);

            cluster.partitionNodes(
                NodeGroup.of(ATHENS, BYZANTIUM),
                NodeGroup.of(CYRENE)
            );

            assertSuccessfulWrite(cluster, client, key, newValue);

            VersionedValue valueCBefore = storedValue(cluster, CYRENE, key);
            assertArrayEquals(value, valueCBefore.value(), "CYRENE should have stale value");

            cluster.healAllPartitions();

            assertReadReturns(cluster, client, key, newValue);

            assertAllReplicasEventuallyHaveValue(cluster, key, newValue);
        }
    }

    @Test
    @DisplayName("Scenario 4: Async Read Repair - Client returns immediately, async propagation fixes stale replicas")
    void testAsyncReadRepair() throws IOException {
        try (var cluster = newCluster(ASYNC_READ_REPAIR_FACTORY)) {
            var client = newClient(cluster);

            byte[] key = "config:version".getBytes();
            byte[] value1 = "1.0".getBytes();
            byte[] value2 = "2.0".getBytes();

            assertSuccessfulWrite(cluster, client, key, value1);

            cluster.partitionNodes(
                NodeGroup.of(ATHENS, BYZANTIUM),
                NodeGroup.of(CYRENE)
            );

            assertSuccessfulWrite(cluster, client, key, value2);

            cluster.healAllPartitions();

            assertReadReturns(cluster, client, key, value2);
            assertReplicaEventuallyHasValue(cluster, CYRENE, key, value2);
        }
    }

    @Test
    @DisplayName("Scenario 5: Read Repair - Alice updates to new value, latest (time=2) propagated to all ✅")
    void testReadRepairSelectingLatestValueVariant1() throws IOException {
        try (var cluster = newCluster(SYNC_READ_REPAIR_FACTORY)) {
            var client = newClient(cluster);

            byte[] key = "stock:price".getBytes();
            byte[] value1 = "100".getBytes();
            byte[] value2 = "150".getBytes();

            assertSuccessfulWrite(cluster, client, key, value1);

            long timestamp1 = storedValue(cluster, ATHENS, key).timestamp();

            cluster.partitionNodes(
                NodeGroup.of(ATHENS, BYZANTIUM),
                NodeGroup.of(CYRENE)
            );

            waitForTicks(cluster, 10);

            assertSuccessfulWrite(cluster, client, key, value2);

            long timestamp2 = storedValue(cluster, ATHENS, key).timestamp();
            assertTrue(timestamp2 > timestamp1, "Second write should have higher timestamp");

            cluster.healAllPartitions();

            assertReadReturns(cluster, client, key, value2);
            assertReplicaEventuallyHasValue(cluster, CYRENE, key, value2);
        }
    }

    @Test
    @DisplayName("Scenario 6: Read Repair - Alice gets mix of old/new, selects latest and repairs missing replicas")
    void testReadRepairSelectingLatestValueVariant2() throws IOException {
        try (var cluster = newCluster(SYNC_READ_REPAIR_FACTORY)) {
            var client = newClient(cluster);

            byte[] key = "counter:visits".getBytes();
            byte[] value0 = "0".getBytes();
            byte[] value1 = "100".getBytes();
            byte[] value2 = "200".getBytes();

            assertSuccessfulWrite(cluster, client, key, value0);

            cluster.partitionNodes(
                NodeGroup.of(ATHENS),
                NodeGroup.of(BYZANTIUM, CYRENE)
            );

            var write1 = client.set(key, value1);
            cluster.tickUntil(() -> write1.isCompleted() || write1.isFailed());

            cluster.healAllPartitions();
            var clientB = newClient(cluster, ProcessId.of("client-b"), BYZANTIUM);

            cluster.partitionNodes(
                NodeGroup.of(BYZANTIUM, CYRENE),
                NodeGroup.of(ATHENS)
            );

            assertSuccessfulWrite(cluster, clientB, key, value2);

            cluster.healAllPartitions();

            assertReadReturns(cluster, clientB, key, value2);
            assertAllReplicasEventuallyHaveValue(cluster, key, value2);
        }
    }

    @Test
    @DisplayName("Read Repair: Multiple reads repair multiple keys")
    void testReadRepairMultipleKeys() throws IOException {
        try (var cluster = newCluster(SYNC_READ_REPAIR_FACTORY)) {
            var client = newClient(cluster);

            byte[] key1 = "key1".getBytes();
            byte[] key2 = "key2".getBytes();
            byte[] value1 = "value1".getBytes();
            byte[] value2 = "value2".getBytes();
            byte[] newValue1 = "new-value1".getBytes();
            byte[] newValue2 = "new-value2".getBytes();

            assertSuccessfulWrite(cluster, client, key1, value1);
            assertSuccessfulWrite(cluster, client, key2, value2);

            cluster.partitionNodes(
                NodeGroup.of(ATHENS, BYZANTIUM),
                NodeGroup.of(CYRENE)
            );

            assertSuccessfulWrite(cluster, client, key1, newValue1);
            assertSuccessfulWrite(cluster, client, key2, newValue2);

            cluster.healAllPartitions();

            assertReadReturns(cluster, client, key1, newValue1);
            assertReadReturns(cluster, client, key2, newValue2);

            assertReplicaEventuallyHasValue(cluster, CYRENE, key1, newValue1);
            assertReplicaEventuallyHasValue(cluster, CYRENE, key2, newValue2);
        }
    }

    @Test
    @DisplayName("Read Repair: No repair needed when all replicas are consistent")
    void testReadRepairNoRepairNeeded() throws IOException {
        try (var cluster = newCluster(SYNC_READ_REPAIR_FACTORY)) {
            var client = newClient(cluster);

            byte[] key = "consistent-key".getBytes();
            byte[] value = "consistent-value".getBytes();

            assertSuccessfulWrite(cluster, client, key, value);
            assertAllReplicasEventuallyHaveValue(cluster, key, value);

            assertReadReturns(cluster, client, key, value);
            assertAllReplicasEventuallyHaveValue(cluster, key, value);
        }
    }
}
