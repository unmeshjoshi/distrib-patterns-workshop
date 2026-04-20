package com.distribpatterns.quorumkv;

import com.tickloom.testkit.Cluster;
import com.tickloom.testkit.NodeGroup;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static com.tickloom.testkit.ClusterAssertions.assertNodesContainValue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static com.distribpatterns.quorumkv.QuorumTestSupport.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for basic quorum scenarios without read repair.
 * These tests verify fundamental quorum behavior including writes, reads,
 * incomplete writes, and inconsistent reads.
 */
public class QuorumBasicScenariosTest {

    @Test
    @DisplayName("Scenario 1: Quorum Write - Alice sets title='Microservices', write succeeds when quorum is met")
    void testQuorumWrite() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new QuorumKVReplica(peerIds, processParams))
                .start()) {
            var client = newClient(cluster, ALICE, ATHENS);

            byte[] key = "title".getBytes();
            byte[] value = "Microservices".getBytes();

            var response = cluster.tickUntilComplete(client.set(key, value));
            assertTrue(response.success(), "Write should succeed");
            assertAllReplicasEventuallyHaveValue(cluster, key, value);
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
            var client = newClient(cluster, ALICE, ATHENS);

            byte[] key = "title".getBytes();
            byte[] value = "Microservices".getBytes();

            cluster.partitionNodes(NodeGroup.of(ATHENS,BYZANTIUM), NodeGroup.of(CYRENE));
            var response = cluster.tickUntilComplete(client.set(key, value));
            assertTrue(response.success(), "Write should succeed");

            cluster.healAllPartitions();
            cluster.partitionNodes(NodeGroup.of(ATHENS, CYRENE), NodeGroup.of(BYZANTIUM));
            assertReadReturns(cluster, client, key, value);
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
            var alice = newClient(cluster, ALICE, ATHENS);

            byte[] key = "title".getBytes();
            byte[] value1 = "Microservices".getBytes();
            byte[] value2 = "Distributed Systems".getBytes();

            cluster.partitionNodes(NodeGroup.of(ATHENS,BYZANTIUM), NodeGroup.of(CYRENE));
            var response1 = cluster.tickUntilComplete(alice.set(key, value1));
            assertTrue(response1.success(), "Write should succeed");

            cluster.healAllPartitions();
            cluster.partitionNodes(NodeGroup.of(ATHENS,CYRENE), NodeGroup.of(BYZANTIUM));
            assertReadReturns(cluster, alice, key, value1);

            var response = cluster.tickUntilComplete(alice.set(key, value2));
            assertTrue(response.success(), "Write should succeed");

            cluster.healAllPartitions();
            cluster.partitionNodes(NodeGroup.of(ATHENS, BYZANTIUM), NodeGroup.of(CYRENE));
            assertReadReturns(cluster, alice, key, value2);
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
            var alice = newClient(cluster, ALICE, ATHENS);

            byte[] key = "title".getBytes();
            byte[] value = "Microservices".getBytes();

            var response1 = cluster.tickUntilComplete(alice.set(key, value));
            assertTrue(response1.success(), "Write should succeed");
            assertAllReplicasEventuallyHaveValue(cluster, key, value);

            cluster.partitionNodes(
                NodeGroup.of(ATHENS, BYZANTIUM),
                NodeGroup.of(CYRENE)
            );

            byte[] newValue = "Distributed Systems".getBytes();
            var response = cluster.tickUntilComplete(alice.set(key, newValue));
            assertTrue(response.success(), "Write should succeed");

            assertNodesContainValue(cluster, List.of(ATHENS, BYZANTIUM), key, newValue);
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
            var alice = newClient(cluster, ALICE, ATHENS);

            byte[] key = "title".getBytes();
            byte[] value = "Microservices".getBytes();

            cluster.partitionNodes(
                NodeGroup.of(ATHENS),
                NodeGroup.of(BYZANTIUM, CYRENE)
            );

            assertWriteFails(cluster, alice, key, value);
        }
    }
}
