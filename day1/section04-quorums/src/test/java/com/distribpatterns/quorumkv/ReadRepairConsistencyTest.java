package com.distribpatterns.quorumkv;

import com.tickloom.ConsistencyChecker;
import com.tickloom.ConsistencyChecker.ConsistencyProperty;
import com.tickloom.ConsistencyChecker.DataModel;
import com.tickloom.ProcessId;
import com.tickloom.ProcessFactory;
import com.tickloom.history.History;
import com.tickloom.history.Op;
import com.tickloom.testkit.Cluster;
import com.tickloom.testkit.NodeGroup;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static com.tickloom.testkit.ClusterAssertions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Demonstrates how to use Jepsen history tracking to verify consistency properties.
 * 
 * This test shows:
 * 1. How to track operations using History<String>
 * 2. How to export history to EDN format
 * 3. How to check linearizability and sequential consistency using Jepsen's Knossos checker
 */
public class ReadRepairConsistencyTest {

    private static final ProcessId ATHENS = ProcessId.of("athens");
    private static final ProcessId BYZANTIUM = ProcessId.of("byzantium");
    private static final ProcessId CYRENE = ProcessId.of("cyrene");
    
    private static final ProcessId ALICE = ProcessId.of("alice");

    /**
     * Factory for creating replicas with read repair enabled (synchronous).
     */
    private static final ProcessFactory SYNC_READ_REPAIR_FACTORY = (peerIds, processParams) ->
            new QuorumKVReplica(peerIds, processParams, true, false);

    @Test
    @DisplayName("Read Repair with Sync: Demonstrates history tracking and consistency checking")
    void testSyncReadRepairConsistency() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build(SYNC_READ_REPAIR_FACTORY)
                .start()) {

            var alice = cluster.newClientConnectedTo(ALICE, ATHENS, QuorumKVClient::new);

            String key = "title";
            String v0 = "Microservices";
            String v1 = "Distributed Systems";

            // Create a History tracker to record all operations
            History<String> history = new History<>();

            // Step 1: Write initial value to all nodes
            // Pattern: invoke() -> perform operation -> ok() or fail()
            history.invoke(ALICE, Op.WRITE, v0);
            var write1 = alice.set(key.getBytes(), v0.getBytes());
            assertEventually(cluster, write1::isCompleted);
            assertTrue(write1.getResult().success());
            history.ok(ALICE, Op.WRITE, v0);

            // Step 2: Partition CYRENE away
            cluster.partitionNodes(
                NodeGroup.of(ATHENS, BYZANTIUM),
                NodeGroup.of(CYRENE)
            );

            // Step 3: Write new value to majority (ATHENS, BYZANTIUM)
            history.invoke(ALICE, Op.WRITE, v1);
            var write2 = alice.set(key.getBytes(), v1.getBytes());
            assertEventually(cluster, write2::isCompleted);
            assertTrue(write2.getResult().success());
            history.ok(ALICE, Op.WRITE, v1);

            // Step 4: Heal partition
            cluster.healAllPartitions();

            // Step 5: Read with sync read repair - should return v1 and repair CYRENE
            history.invoke(ALICE, Op.READ, null);
            var read = alice.get(key.getBytes());
            assertEventually(cluster, read::isCompleted);
            assertTrue(read.getResult().found());
            assertArrayEquals(v1.getBytes(), read.getResult().value());
            history.ok(ALICE, Op.READ, new String(read.getResult().value()));

            // Step 6: Wait for sync read repair to complete
            assertEventually(cluster, () -> {
                var valueC = cluster.getStorageValue(CYRENE, key.getBytes());
                return valueC != null && java.util.Arrays.equals(v1.getBytes(), valueC.value());
            });

            // Step 7: Another read should still see v1
            history.invoke(ALICE, Op.READ, null);
            var read2 = alice.get(key.getBytes());
            assertEventually(cluster, read2::isCompleted);
            assertArrayEquals(v1.getBytes(), read2.getResult().value());
            history.ok(ALICE, Op.READ, new String(read2.getResult().value()));

            // Step 8: Export history to EDN format and check consistency
            String edn = history.toEdn();
            System.out.println("=== History EDN ===");
            System.out.println(edn);
            System.out.println("==================");

            // Step 9: Check consistency properties using Jepsen's Knossos checker
            boolean linearizable = ConsistencyChecker.check(edn, ConsistencyProperty.LINEARIZABILITY, DataModel.REGISTER);
            boolean sequential = ConsistencyChecker.check(edn, ConsistencyProperty.SEQUENTIAL_CONSISTENCY, DataModel.REGISTER);
            
            System.out.println("Linearizable: " + linearizable);
            System.out.println("Sequential: " + sequential);

            // With sync read repair and a single client, the system should be linearizable
            assertTrue(linearizable, "Sync read repair with single client should be linearizable");
            assertTrue(sequential, "Should also be sequentially consistent");
        }
    }
}
