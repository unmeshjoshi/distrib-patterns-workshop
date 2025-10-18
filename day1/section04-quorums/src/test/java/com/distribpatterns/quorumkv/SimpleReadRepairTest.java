package com.distribpatterns.quorumkv;

import com.tickloom.ProcessFactory;
import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import com.tickloom.testkit.NodeGroup;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static com.tickloom.testkit.ClusterAssertions.assertAllNodeStoragesContainValue;
import static com.tickloom.testkit.ClusterAssertions.assertEventually;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple read repair test to debug the functionality.
 */
public class SimpleReadRepairTest {

    private static final ProcessId ATHENS = ProcessId.of("node-a");
    private static final ProcessId BYZANTIUM = ProcessId.of("node-b");
    private static final ProcessId CYRENE = ProcessId.of("node-c");
    private static final ProcessId ALICE = ProcessId.of("client-1");

    private static final ProcessFactory SYNC_READ_REPAIR_FACTORY = (peerIds, processParams) ->
            new QuorumKVReplica(peerIds, processParams, true, false);

    @Test
    @DisplayName("Simple Read Repair Test")
    void testSimpleReadRepair() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build(SYNC_READ_REPAIR_FACTORY)
                .start()) {

            var client = cluster.newClientConnectedTo(ALICE, ATHENS, QuorumKVClient::new);

            byte[] key = "test".getBytes();
            byte[] value1 = "v1".getBytes();

            // Write initial value
            System.out.println("=== Writing initial value ===");
            var write1 = client.set(key, value1);
            assertEventually(cluster, write1::isCompleted);
            assertTrue(write1.getResult().success());
            
            // Verify all nodes have it
            assertAllNodeStoragesContainValue(cluster, key, value1);
            System.out.println("All nodes have v1");

            // Partition CYRENE
            System.out.println("=== Partitioning CYRENE ===");
            cluster.partitionNodes(
                NodeGroup.of(ATHENS, BYZANTIUM),
                NodeGroup.of(CYRENE)
            );

            // Write new value
            byte[] value2 = "v2".getBytes();
            System.out.println("=== Writing v2 to majority ===");
            var write2 = client.set(key, value2);
            assertEventually(cluster, write2::isCompleted);
            assertTrue(write2.getResult().success());
            
            // Check CYRENE still has v1
            VersionedValue valueCBefore = cluster.getDecodedStoredValue(CYRENE, key, VersionedValue.class);
            System.out.println("CYRENE value before read: " + new String(valueCBefore.value()) + 
                " (timestamp: " + valueCBefore.timestamp() + ")");
            assertArrayEquals(value1, valueCBefore.value());

            // Heal partition
            System.out.println("=== Healing partition ===");
            cluster.healAllPartitions();

            // Do read which should trigger repair
            System.out.println("=== Performing read (should trigger repair) ===");
            var read = client.get(key);
            assertEventually(cluster, read::isCompleted);
            assertArrayEquals(value2, read.getResult().value());
            System.out.println("Read returned v2");

            // Give some time for repair messages to propagate
            System.out.println("=== Waiting for repair to propagate ===");
            for (int i = 0; i < 100; i++) {
                cluster.tick();
                VersionedValue valueC = cluster.getDecodedStoredValue(CYRENE, key, VersionedValue.class);
                if (valueC != null && java.util.Arrays.equals(value2, valueC.value())) {
                    System.out.println("CYRENE repaired after " + i + " ticks!");
                    break;
                }
                if (i % 10 == 0) {
                    System.out.println("Tick " + i + ", CYRENE still has: " + 
                        (valueC != null ? new String(valueC.value()) : "null"));
                }
            }

            // Check if CYRENE was repaired
            VersionedValue valueCAfter = cluster.getDecodedStoredValue(CYRENE, key, VersionedValue.class);
            System.out.println("CYRENE value after read: " + 
                (valueCAfter != null ? new String(valueCAfter.value()) + " (ts: " + valueCAfter.timestamp() + ")" : "null"));
            
            if (!java.util.Arrays.equals(value2, valueCAfter.value())) {
                fail("Read repair did not update CYRENE. Expected v2 but got: " + new String(valueCAfter.value()));
            }
        }
    }
}

