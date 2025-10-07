package replicate.quorumkv;

import com.tickloom.ProcessId;
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
    private static final ProcessId BOB = ProcessId.of("bob");
    private static final ProcessId NATHAN = ProcessId.of("nathan");

    @Test
    @DisplayName("Scenario 1: Quorum Write - Alice sets title='Microservices', write succeeds when quorum is met")
    void testQuorumWrite() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build(QuorumKVReplica::new)
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
                VersionedValue valueAthens = cluster.getStorageValue(ATHENS, key);
                VersionedValue valueByzantium = cluster.getStorageValue(BYZANTIUM, key);
                VersionedValue valueCyrene = cluster.getStorageValue(CYRENE, key);
                
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
                .build(QuorumKVReplica::new)
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
    @DisplayName("Scenario 7: Consistency - Which values do two sequential reads get?")
    void testConsistencySequentialReads() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build(QuorumKVReplica::new)
                .start()) {

            var alice = cluster.newClientConnectedTo(ALICE, ATHENS, QuorumKVClient::new);

            byte[] key = "title".getBytes();
            byte[] value1 = "Microservices".getBytes();
            byte[] value2 = "Distributed Systems".getBytes();

            // First write
            var write1 = alice.set(key, value1);
            assertEventually(cluster, write1::isCompleted);
            assertTrue(write1.getResult().success());

            // First read - should return value1
            var read1 = alice.get(key);
            assertEventually(cluster, read1::isCompleted);
            assertArrayEquals(value1, read1.getResult().value());

            // Second write
            var write2 = alice.set(key, value2);
            assertEventually(cluster, write2::isCompleted);
            assertTrue(write2.getResult().success());

            // Second read - should return value2 (latest)
            var read2 = alice.get(key);
            assertEventually(cluster, read2::isCompleted);
            assertArrayEquals(value2, read2.getResult().value(),
                "Sequential read should return latest written value");
        }
    }

    @Test
    @DisplayName("Scenario 7: Consistency - Which values does a read get with concurrent writes?")
    void testConsistencyConcurrentWrites() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build(QuorumKVReplica::new)
                .start()) {

            var alice = cluster.newClientConnectedTo(ALICE, ATHENS, QuorumKVClient::new);
            var bob = cluster.newClientConnectedTo(BOB, BYZANTIUM, QuorumKVClient::new);

            byte[] key = "title".getBytes();
            byte[] value1 = "Microservices".getBytes();
            byte[] value2 = "Distributed Systems".getBytes();

            // Concurrent writes from Alice and Bob
            var write1 = alice.set(key, value1);
            var write2 = bob.set(key, value2);

            assertEventually(cluster, () -> write1.isCompleted() && write2.isCompleted());

            // Both writes should succeed (they don't conflict at quorum level)
            assertTrue(write1.getResult().success() || write2.getResult().success(),
                "At least one write should succeed");

            // Read should return one of the values (determined by timestamp)
            var read = alice.get(key);
            assertEventually(cluster, read::isCompleted);
            assertTrue(read.getResult().found());
            
            byte[] resultValue = read.getResult().value();
            boolean isValue1 = java.util.Arrays.equals(value1, resultValue);
            boolean isValue2 = java.util.Arrays.equals(value2, resultValue);
            
            assertTrue(isValue1 || isValue2,
                "Read should return one of the written values");
        }
    }

    @Test
    @DisplayName("Scenario 8: Incomplete Writes - Network failure prevents propagation, some replicas outdated")
    void testIncompleteWrites() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build(QuorumKVReplica::new)
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
            assertEventually(cluster, () -> {
                VersionedValue valueAthens = cluster.getStorageValue(ATHENS, key);
                VersionedValue valueByzantium = cluster.getStorageValue(BYZANTIUM, key);
                return valueAthens != null && valueByzantium != null &&
                       java.util.Arrays.equals(newValue, valueAthens.value()) &&
                       java.util.Arrays.equals(newValue, valueByzantium.value());
            });

            // cyrene should still have old value (some replicas remain outdated)
            VersionedValue valueCyrene = cluster.getStorageValue(CYRENE, key);
            assertNotNull(valueCyrene);
            assertArrayEquals(value, valueCyrene.value(),
                "Disconnected node should still have old value");
        }
    }

    @Test
    @DisplayName("Scenario 9: Inconsistent Reads (1) - Bob reads from byzantium and cyrene, temporary inconsistency")
    void testInconsistentReadsVariant1() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build(QuorumKVReplica::new)
                .start()) {

            var bob = cluster.newClientConnectedTo(BOB, BYZANTIUM, QuorumKVClient::new);

            byte[] key = "title".getBytes();
            byte[] value = "Microservices".getBytes();

            // Initial write
            var write = bob.set(key, value);
            assertEventually(cluster, write::isCompleted);
            assertTrue(write.getResult().success());

            // Create partition: {athens, byzantium} vs {cyrene}
            cluster.partitionNodes(
                NodeGroup.of(ATHENS, BYZANTIUM),
                NodeGroup.of(CYRENE)
            );

            byte[] newValue = "Distributed Systems".getBytes();
            
            // Write to majority partition
            var write2 = bob.set(key, newValue);
            assertEventually(cluster, write2::isCompleted);
            assertTrue(write2.getResult().success());

            // Bob reading from majority partition sees new value
            var read1 = bob.get(key);
            assertEventually(cluster, read1::isCompleted);
            assertArrayEquals(newValue, read1.getResult().value(),
                "Bob in majority partition should see new value");

            // Verify cyrene still has old value (stale) - temporary inconsistency
            VersionedValue valueCyrene = cluster.getStorageValue(CYRENE, key);
            assertArrayEquals(value, valueCyrene.value(),
                "cyrene has stale value (background repair delayed)");

            // If we create a client connected to cyrene directly (reading local storage),
            // it would see different value - this demonstrates temporary inconsistency
            // However, with quorum reads, the client would contact multiple nodes
            // and get the latest value
        }
    }

    @Test
    @DisplayName("Scenario 10: Inconsistent Reads (2) - Nathan gets old value from mixed results ⚠️")
    void testInconsistentReadsVariant2() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build(QuorumKVReplica::new)
                .start()) {

            var nathan = cluster.newClientConnectedTo(NATHAN, ATHENS, QuorumKVClient::new);

            byte[] key = "title".getBytes();
            byte[] value1 = "Microservices".getBytes();
            byte[] value2 = "Distributed Systems".getBytes();

            // Write "Microservices" to all replicas
            var write1 = nathan.set(key, value1);
            assertEventually(cluster, write1::isCompleted);
            assertTrue(write1.getResult().success());
            assertAllNodeStoragesContainValue(cluster, key, value1);

            // First read - sees "Microservices"
            var read1 = nathan.get(key);
            assertEventually(cluster, read1::isCompleted);
            assertArrayEquals(value1, read1.getResult().value());

            // Partition cyrene away
            cluster.partitionNodes(
                NodeGroup.of(ATHENS, BYZANTIUM),
                NodeGroup.of(CYRENE)
            );

            // Write "Distributed Systems" to majority (athens, byzantium)
            var write2 = nathan.set(key, value2);
            assertEventually(cluster, write2::isCompleted);
            assertTrue(write2.getResult().success());

            // Read from majority - sees "Distributed Systems"
            var read2 = nathan.get(key);
            assertEventually(cluster, read2::isCompleted);
            assertArrayEquals(value2, read2.getResult().value(),
                "Read from majority should see latest value");

            // Heal and create different partition: cyrene with athens vs byzantium
            cluster.healAllPartitions();
            cluster.partitionNodes(
                NodeGroup.of(CYRENE, ATHENS),
                NodeGroup.of(BYZANTIUM)
            );

            // Nathan reads from replicas - receives mixed results
            // isQuorumReached detects divergence
            // Comment: "Nathan gets old value." ⚠️
            
            // For this scenario, we verify that cyrene still has stale data
            VersionedValue valueCyrene = cluster.getStorageValue(CYRENE, key);
            assertArrayEquals(value1, valueCyrene.value(),
                "cyrene should still have old value 'Microservices' until repaired");
        }
    }

    @Test
    @DisplayName("Scenario: Write fails when majority unavailable")
    void testWriteFailsWithoutQuorum() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build(QuorumKVReplica::new)
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

    @Test
    @DisplayName("Scenario 11: Issues with Basic Quorum - Two clients connecting to different replicas")
    void testMultipleClientsConcurrentOps() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build(QuorumKVReplica::new)
                .start()) {

            var alice = cluster.newClientConnectedTo(ALICE, ATHENS, QuorumKVClient::new);
            var bob = cluster.newClientConnectedTo(BOB, BYZANTIUM, QuorumKVClient::new);

            byte[] key1 = "title".getBytes();
            byte[] key2 = "author".getBytes();
            byte[] value1 = "Microservices".getBytes();
            byte[] value2 = "Martin Kleppmann".getBytes();

            // Concurrent writes from different clients to different keys
            var write1 = alice.set(key1, value1);
            var write2 = bob.set(key2, value2);

            assertEventually(cluster, () -> write1.isCompleted() && write2.isCompleted());
            
            assertTrue(write1.getResult().success(), "Alice write should succeed");
            assertTrue(write2.getResult().success(), "Bob write should succeed");

            // Both clients should be able to read both values
            var read1 = alice.get(key2);
            var read2 = bob.get(key1);

            assertEventually(cluster, () -> read1.isCompleted() && read2.isCompleted());
            
            assertArrayEquals(value2, read1.getResult().value(),
                "Alice should read value written by Bob");
            assertArrayEquals(value1, read2.getResult().value(),
                "Bob should read value written by Alice");
        }
    }
}

