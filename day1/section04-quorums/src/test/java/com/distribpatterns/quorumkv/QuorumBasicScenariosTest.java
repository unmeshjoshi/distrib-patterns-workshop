package com.distribpatterns.quorumkv;

import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import com.tickloom.testkit.NodeGroup;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static com.tickloom.testkit.ClusterAssertions.*;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for basic quorum scenarios without read repair.
 * These tests verify fundamental quorum behavior including writes, reads,
 * incomplete writes, and inconsistent reads.
 */
public class QuorumBasicScenariosTest {

    // Replica nodes (matching scenario descriptions)
    private static final ProcessId ATHENS = ProcessId.of("athens");
    private static final ProcessId BYZANTIUM = ProcessId.of("byzantium");
    private static final ProcessId CYRENE = ProcessId.of("cyrene");
    
    // Clients (actors from scenarios)
    private static final ProcessId ALICE = ProcessId.of("alice");

    @Test
    @DisplayName("Scenario 1: Quorum Write - Alice sets title='Microservices', write succeeds when quorum is met")
    void testQuorumWrite() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new QuorumKVReplica(peerIds, processParams))
                .start()) {

            var client = cluster.newClientConnectedTo(ALICE, ATHENS, QuorumKVClient::new);

            byte[] key = "title".getBytes();
            byte[] value = "Microservices".getBytes();

            // Perform write - should succeed with quorum (2 out of 3)
            var writeResult = client.set(key, value);
            assertEventually(cluster, writeResult::isCompleted);
            
            assertTrue(writeResult.getResult().success(), 
                "Write should succeed after majority acknowledgment");

            // Verify all replicas eventually have the value
            assertEventually(cluster, () -> {
                VersionedValue valueAthens = cluster.getDecodedStoredValue(ATHENS, key, VersionedValue.class);
                VersionedValue valueByzantium = cluster.getDecodedStoredValue(BYZANTIUM, key, VersionedValue.class);
                VersionedValue valueCyrene = cluster.getDecodedStoredValue(CYRENE, key, VersionedValue.class);
                
                return valueAthens != null && valueByzantium != null && valueCyrene != null;
            });

            // Verify the written value is stored correctly
            assertAllNodeStoragesContainValue(cluster, key, value);
        }
    }

    @Test
    @DisplayName("Scenario 2: Quorum Read - Alice reads from all replicas, returns 'Microservices'")
    void testQuorumRead() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new QuorumKVReplica(peerIds, processParams))
                .start()) {

            var client = cluster.newClientConnectedTo(ALICE, ATHENS, QuorumKVClient::new);

            byte[] key = "title".getBytes();
            byte[] value = "Microservices".getBytes();

            cluster.partitionNodes(NodeGroup.of(ATHENS,BYZANTIUM), NodeGroup.of(CYRENE));
            // Write value
            var writeResult = client.set(key, value);
            assertEventually(cluster, writeResult::isCompleted);
            assertTrue(writeResult.getResult().success());

            cluster.healAllPartitions();
            cluster.partitionNodes(NodeGroup.of(ATHENS,CYRENE), NodeGroup.of(BYZANTIUM));
            // Read value - should query majority and return latest
            var readResult = client.get(key);
            assertEventually(cluster, readResult::isCompleted);
            
            assertTrue(readResult.getResult().found(), "Read should find the value");
            assertArrayEquals(value, readResult.getResult().value(),
                "Read should return the value written");
        }
    }

    @Test
    @DisplayName("Scenario 3: Consistency - Which values do two sequential reads get?")
    void testConsistencySequentialReads() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new QuorumKVReplica(peerIds, processParams))
                .start()) {

            var alice = cluster.newClientConnectedTo(ALICE, ATHENS, QuorumKVClient::new);

            byte[] key = "title".getBytes();
            byte[] value1 = "Microservices".getBytes();
            byte[] value2 = "Distributed Systems".getBytes();

            cluster.partitionNodes(NodeGroup.of(ATHENS,BYZANTIUM), NodeGroup.of(CYRENE));
            // First write
            var write1 = alice.set(key, value1);
            assertEventually(cluster, write1::isCompleted);
            assertTrue(write1.getResult().success());

            cluster.healAllPartitions();
            cluster.partitionNodes(NodeGroup.of(ATHENS,CYRENE), NodeGroup.of(BYZANTIUM));
            // First read - should return value1
            var read1 = alice.get(key);
            assertEventually(cluster, read1::isCompleted);
            assertArrayEquals(value1, read1.getResult().value());

            // Second write
            var write2 = alice.set(key, value2);
            assertEventually(cluster, write2::isCompleted);
            assertTrue(write2.getResult().success());

            // Second read - should return value2 (latest)
            cluster.healAllPartitions();
            cluster.partitionNodes(NodeGroup.of(ATHENS,BYZANTIUM), NodeGroup.of(CYRENE));


            var read2 = alice.get(key);
            assertEventually(cluster, read2::isCompleted);
            assertArrayEquals(value2, read2.getResult().value(),
                "Sequential read should return latest written value");
        }
    }

    @Test
    @DisplayName("Scenario 4: Incomplete Writes - Network failure prevents propagation, some replicas outdated")
    void testIncompleteWrites() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new QuorumKVReplica(peerIds, processParams))
                .start()) {

            var alice = cluster.newClientConnectedTo(ALICE, ATHENS, QuorumKVClient::new);

            byte[] key = "title".getBytes();
            byte[] value = "Microservices".getBytes();

            // Initial successful write
            var initialWrite = alice.set(key, value);
            assertEventually(cluster, initialWrite::isCompleted);
            assertTrue(initialWrite.getResult().success());
            assertAllNodeStoragesContainValue(cluster, key, value);

            // Partition: cyrene away from athens and byzantium (network failure)
            cluster.partitionNodes(
                NodeGroup.of(ATHENS, BYZANTIUM),
                NodeGroup.of(CYRENE)
            );

            byte[] newValue = "Distributed Systems".getBytes();
            
            // Write with cyrene disconnected - should still succeed with 2 nodes
            var partialWrite = alice.set(key, newValue);
            assertEventually(cluster, partialWrite::isCompleted);
            assertTrue(partialWrite.getResult().success(),
                "Write should succeed with 2/3 nodes (quorum)");

            // Verify only athens and byzantium have the new value
            assertNodesContainValue(cluster, List.of(ATHENS, BYZANTIUM), key, newValue);
            // cyrene should still have old value (some replicas remain outdated)
            assertNodesContainValue(cluster, List.of(CYRENE), key, value);
        }
    }

    @Test
    @DisplayName("Scenario 5: Write fails when majority unavailable")
    void testWriteFailsWithoutQuorum() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new QuorumKVReplica(peerIds, processParams))
                .start()) {

            var alice = cluster.newClientConnectedTo(ALICE, ATHENS, QuorumKVClient::new);

            byte[] key = "title".getBytes();
            byte[] value = "Microservices".getBytes();

            // Partition athens alone (minority) from byzantium and cyrene (majority)
            cluster.partitionNodes(
                NodeGroup.of(ATHENS),
                NodeGroup.of(BYZANTIUM, CYRENE)
            );

            // Write should fail - athens cannot achieve quorum alone
            var write = alice.set(key, value);
            assertEventually(cluster, write::isFailed);
        }
    }
}

