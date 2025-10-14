package com.distribpatterns.quorumkv;

import com.tickloom.ProcessId;
import com.tickloom.ProcessFactory;
import com.tickloom.storage.VersionedValue;
import com.tickloom.testkit.Cluster;
import com.tickloom.testkit.NodeGroup;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static com.tickloom.testkit.ClusterAssertions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for read repair scenarios.
 * These tests verify both synchronous and asynchronous read repair functionality.
 */
public class ReadRepairScenariosTest {

    // Replica nodes (matching scenario descriptions)
    private static final ProcessId ATHENS = ProcessId.of("athens");
    private static final ProcessId BYZANTIUM = ProcessId.of("byzantium");
    private static final ProcessId CYRENE = ProcessId.of("cyrene");
    
    // Client (actor from scenarios)
    private static final ProcessId ALICE = ProcessId.of("alice");

    /**
     * Factory for creating replicas with read repair enabled (synchronous).
     */
    private static final ProcessFactory SYNC_READ_REPAIR_FACTORY = (peerIds,  processParams) ->
            new QuorumKVReplica(peerIds, processParams, true, false);

    /**
     * Factory for creating replicas with read repair enabled (asynchronous).
     */
    private static final ProcessFactory ASYNC_READ_REPAIR_FACTORY = (peerIds,  processParams) ->
            new QuorumKVReplica(peerIds, processParams, true, true);

    @Test
    @DisplayName("Scenario 3: Read Repair - Coordinator detects stale nodes and synchronizes all replicas ✅")
    void testSyncReadRepair() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build(SYNC_READ_REPAIR_FACTORY)
                .start()) {

            var client = cluster.newClientConnectedTo(ALICE, ATHENS, QuorumKVClient::new);

            byte[] key = "user:profile".getBytes();
            byte[] value = "Alice".getBytes();

            // Write initial value to all nodes
            var write1 = client.set(key, value);
            assertEventually(cluster, write1::isCompleted);
            assertTrue(write1.getResult().success());
            assertAllNodeStoragesContainValue(cluster, key, value);

            // Partition CYRENE away and write new value
            cluster.partitionNodes(
                NodeGroup.of(ATHENS, BYZANTIUM),
                NodeGroup.of(CYRENE)
            );

            byte[] newValue = "Bob".getBytes();
            var write2 = client.set(key, newValue);
            assertEventually(cluster, write2::isCompleted);
            assertTrue(write2.getResult().success());

            // Verify CYRENE still has old value
            VersionedValue valueCBefore = cluster.getStorageValue(CYRENE, key);
            assertArrayEquals(value, valueCBefore.value(), "CYRENE should have stale value");

            // Heal partition
            cluster.healAllPartitions();

            // Perform read - this should trigger read repair
            var read = client.get(key);
            assertEventually(cluster, read::isCompleted);
            assertArrayEquals(newValue, read.getResult().value());

            // Wait for read repair to complete
            assertEventually(cluster, () -> {
                VersionedValue valueC = cluster.getStorageValue(CYRENE, key);
                return valueC != null && java.util.Arrays.equals(newValue, valueC.value());
            });

            // Verify all nodes now have the latest value
            assertAllNodeStoragesContainValue(cluster, key, newValue);
        }
    }

    @Test
    @DisplayName("Scenario 4: Async Read Repair - Client returns immediately, async propagation fixes stale replicas")
    void testAsyncReadRepair() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build(ASYNC_READ_REPAIR_FACTORY)
                .start()) {

            var client = cluster.newClientConnectedTo(ALICE, ATHENS, QuorumKVClient::new);

            byte[] key = "config:version".getBytes();
            byte[] value1 = "1.0".getBytes();
            byte[] value2 = "2.0".getBytes();

            // Write v1.0 to all nodes
            var write1 = client.set(key, value1);
            assertEventually(cluster, write1::isCompleted);
            assertTrue(write1.getResult().success());

            // Partition and write v2.0
            cluster.partitionNodes(
                NodeGroup.of(ATHENS, BYZANTIUM),
                NodeGroup.of(CYRENE)
            );

            var write2 = client.set(key, value2);
            assertEventually(cluster, write2::isCompleted);
            assertTrue(write2.getResult().success());

            // Heal partition
            cluster.healAllPartitions();

            // Read triggers async read repair
            var read = client.get(key);
            assertEventually(cluster, read::isCompleted);
            assertArrayEquals(value2, read.getResult().value(),
                "Read should return latest value immediately");

            // Async repair happens in background
            assertEventually(cluster, () -> {
                VersionedValue valueC = cluster.getStorageValue(CYRENE, key);
                return valueC != null && java.util.Arrays.equals(value2, valueC.value());
            });
        }
    }

    @Test
    @DisplayName("Scenario 5: Read Repair - Alice updates to new value, latest (time=2) propagated to all ✅")
    void testReadRepairSelectingLatestValueVariant1() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build(SYNC_READ_REPAIR_FACTORY)
                .start()) {

            var client = cluster.newClientConnectedTo(ALICE, ATHENS, QuorumKVClient::new);

            byte[] key = "stock:price".getBytes();
            byte[] value1 = "100".getBytes();
            byte[] value2 = "150".getBytes();

            // Write value1
            var write1 = client.set(key, value1);
            assertEventually(cluster, write1::isCompleted);
            assertTrue(write1.getResult().success());

            long timestamp1 = cluster.getStorageValue(ATHENS, key).timestamp();

            // Partition and write value2 (will have higher timestamp)
            cluster.partitionNodes(
                NodeGroup.of(ATHENS, BYZANTIUM),
                NodeGroup.of(CYRENE)
            );

            // Wait a bit to ensure different timestamp
            for (int i = 0; i < 10; i++) {
                cluster.tick();
            }

            var write2 = client.set(key, value2);
            assertEventually(cluster, write2::isCompleted);
            assertTrue(write2.getResult().success());

            long timestamp2 = cluster.getStorageValue(ATHENS, key).timestamp();
            assertTrue(timestamp2 > timestamp1, "Second write should have higher timestamp");

            // Heal and perform read
            cluster.healAllPartitions();

            var read = client.get(key);
            assertEventually(cluster, read::isCompleted);
            
            // Should select value2 as it has higher timestamp
            assertArrayEquals(value2, read.getResult().value(),
                "Read should return value with highest timestamp (value2)");

            // Read repair should update all nodes to value2
            assertEventually(cluster, () -> {
                VersionedValue valueC = cluster.getStorageValue(CYRENE, key);
                return valueC != null && java.util.Arrays.equals(value2, valueC.value());
            });
        }
    }

    @Test
    @DisplayName("Scenario 6: Read Repair - Alice gets mix of old/new, selects latest and repairs missing replicas")
    void testReadRepairSelectingLatestValueVariant2() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build(SYNC_READ_REPAIR_FACTORY)
                .start()) {

            var client = cluster.newClientConnectedTo(ALICE, ATHENS, QuorumKVClient::new);

            byte[] key = "counter:visits".getBytes();
            byte[] value0 = "0".getBytes();
            byte[] value1 = "100".getBytes();
            byte[] value2 = "200".getBytes();

            // Write value0 to all nodes
            var write0 = client.set(key, value0);
            assertEventually(cluster, write0::isCompleted);
            assertTrue(write0.getResult().success());

            // Partition: ATHENS alone vs BYZANTIUM + CYRENE
            cluster.partitionNodes(
                NodeGroup.of(ATHENS),
                NodeGroup.of(BYZANTIUM, CYRENE)
            );

            // Write value1 to ATHENS (will fail due to no quorum, but might persist locally)
            var write1 = client.set(key, value1);
            assertEventually(cluster, () -> write1.isCompleted() || write1.isFailed());

            // Heal and create different partition: BYZANTIUM + CYRENE vs ATHENS
            cluster.healAllPartitions();
            
            // Connect client to BYZANTIUM
            var clientB = cluster.newClientConnectedTo(ProcessId.of("client-b"), BYZANTIUM, QuorumKVClient::new);
            
            cluster.partitionNodes(
                NodeGroup.of(BYZANTIUM, CYRENE),
                NodeGroup.of(ATHENS)
            );

            // Write value2 to majority (BYZANTIUM, CYRENE)
            var write2 = clientB.set(key, value2);
            assertEventually(cluster, write2::isCompleted);
            assertTrue(write2.getResult().success());

            // Heal all partitions
            cluster.healAllPartitions();

            // Read should select latest value (value2) based on timestamp
            var read = clientB.get(key);
            assertEventually(cluster, read::isCompleted);
            assertArrayEquals(value2, read.getResult().value(),
                "Read should return value with latest timestamp");

            // Read repair should update ATHENS to value2
            assertEventually(cluster, () -> {
                VersionedValue valueA = cluster.getStorageValue(ATHENS, key);
                return valueA != null && java.util.Arrays.equals(value2, valueA.value());
            });

            // All nodes should eventually have value2
            assertEventually(cluster, () -> {
                VersionedValue valueA = cluster.getStorageValue(ATHENS, key);
                VersionedValue valueB = cluster.getStorageValue(BYZANTIUM, key);
                VersionedValue valueC = cluster.getStorageValue(CYRENE, key);
                
                return valueA != null && valueB != null && valueC != null &&
                       java.util.Arrays.equals(value2, valueA.value()) &&
                       java.util.Arrays.equals(value2, valueB.value()) &&
                       java.util.Arrays.equals(value2, valueC.value());
            });
        }
    }

    @Test
    @DisplayName("Read Repair: Multiple reads repair multiple keys")
    void testReadRepairMultipleKeys() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build(SYNC_READ_REPAIR_FACTORY)
                .start()) {

            var client = cluster.newClientConnectedTo(ALICE, ATHENS, QuorumKVClient::new);

            byte[] key1 = "key1".getBytes();
            byte[] key2 = "key2".getBytes();
            byte[] value1 = "value1".getBytes();
            byte[] value2 = "value2".getBytes();
            byte[] newValue1 = "new-value1".getBytes();
            byte[] newValue2 = "new-value2".getBytes();

            // Write initial values
            client.set(key1, value1);
            var write2 = client.set(key2, value2);
            assertEventually(cluster, write2::isCompleted);

            // Partition and write new values
            cluster.partitionNodes(
                NodeGroup.of(ATHENS, BYZANTIUM),
                NodeGroup.of(CYRENE)
            );

            client.set(key1, newValue1);
            var write4 = client.set(key2, newValue2);
            assertEventually(cluster, write4::isCompleted);

            // Heal and read both keys
            cluster.healAllPartitions();

            var read1 = client.get(key1);
            var read2 = client.get(key2);
            assertEventually(cluster, () -> read1.isCompleted() && read2.isCompleted());

            // Both reads should trigger repair
            assertEventually(cluster, () -> {
                VersionedValue valueC1 = cluster.getStorageValue(CYRENE, key1);
                VersionedValue valueC2 = cluster.getStorageValue(CYRENE, key2);
                
                return valueC1 != null && valueC2 != null &&
                       java.util.Arrays.equals(newValue1, valueC1.value()) &&
                       java.util.Arrays.equals(newValue2, valueC2.value());
            });
        }
    }

    @Test
    @DisplayName("Read Repair: No repair needed when all replicas are consistent")
    void testReadRepairNoRepairNeeded() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build(SYNC_READ_REPAIR_FACTORY)
                .start()) {

            var client = cluster.newClientConnectedTo(ALICE, ATHENS, QuorumKVClient::new);

            byte[] key = "consistent-key".getBytes();
            byte[] value = "consistent-value".getBytes();

            // Write value to all nodes
            var write = client.set(key, value);
            assertEventually(cluster, write::isCompleted);
            assertTrue(write.getResult().success());

            // All nodes should have the value
            assertAllNodeStoragesContainValue(cluster, key, value);

            // Read should not trigger any repair (all nodes are consistent)
            var read = client.get(key);
            assertEventually(cluster, read::isCompleted);
            assertArrayEquals(value, read.getResult().value());

            // Values should remain unchanged (no unnecessary writes)
            assertAllNodeStoragesContainValue(cluster, key, value);
        }
    }
}

