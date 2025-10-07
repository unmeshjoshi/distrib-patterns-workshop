package com.distribpatterns.quorumkv;

import com.tickloom.ConsistencyChecker;
import com.tickloom.ConsistencyChecker.ConsistencyProperty;
import com.tickloom.ConsistencyChecker.DataModel;
import com.tickloom.ProcessId;
import com.tickloom.future.ListenableFuture;
import com.tickloom.history.History;
import com.tickloom.history.Op;
import com.tickloom.testkit.Cluster;
import com.tickloom.testkit.NodeGroup;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.function.BooleanSupplier;

import static com.tickloom.testkit.ClusterAssertions.*;
import static org.junit.jupiter.api.Assertions.*;


public class NetworkPartitionTest {

    // Common ids reused across tests
    private static final ProcessId ATHENS = ProcessId.of("athens");
    private static final ProcessId BYZANTIUM = ProcessId.of("byzantium");
    private static final ProcessId CYRENE = ProcessId.of("cyrene");
    private static final ProcessId DELPHI = ProcessId.of("delphi");
    private static final ProcessId SPARTA = ProcessId.of("sparta");

    private static final ProcessId MINORITY_CLIENT = ProcessId.of("minority_client");
    private static final ProcessId MAJORITY_CLIENT = ProcessId.of("majority_client");

    private static final int SKEW_TICKS = 10;

    @Test
    @DisplayName("Split-brain prevention: majority value persists after heal")
    void shouldPreventSplitBrainDuringNetworkPartition() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE, DELPHI, SPARTA))
                .useSimulatedNetwork()
                .build(QuorumKVReplica::new)
                .start()) {

            // clients
            var minorityClient = cluster.newClientConnectedTo(MINORITY_CLIENT, ATHENS, QuorumKVClient::new);
            var majorityClient = cluster.newClientConnectedTo(MAJORITY_CLIENT, CYRENE, QuorumKVClient::new);

            // data
            byte[] key = "distributed_ledger".getBytes();
            byte[] initialValue = "genesis_block".getBytes();
            byte[] minorityValue = "minority_attempt".getBytes();
            byte[] majorityValue = "majority_success".getBytes();

            // phase 1 — initial write via majority, cluster converges
            var initialSet = majorityClient.set(key, initialValue);
            assertEventually(cluster, initialSet::isCompleted);
            assertTrue(initialSet.getResult().success(), "Initial write should succeed");
            assertAllNodeStoragesContainValue(cluster, key, initialValue);

            // phase 2 — partition 2 vs 3
            var minority = NodeGroup.of(ATHENS, BYZANTIUM);
            var majority = NodeGroup.of(CYRENE, DELPHI, SPARTA);
            cluster.partitionNodes(minority, majority);

            // phase 3 — minority write fails for client (no quorum) but persists locally
            var minorityWrite = minorityClient.set(key, minorityValue);
            assertEventually(cluster, minorityWrite::isFailed);
            assertNodesContainValue(cluster, List.of(ATHENS, BYZANTIUM), key, minorityValue);

            // phase 4 — majority write succeeds in its partition
            var majorityWrite = majorityClient.set(key, majorityValue);
            assertEventually(cluster, completesSuccessfully(majorityWrite));
            assertNodesContainValue(cluster, List.of(CYRENE, DELPHI, SPARTA), key, majorityValue);

            // phase 5 — heal and verify final value (majority value should win without skew)
            cluster.healAllPartitions();

            var healedRead = majorityClient.get(key);
            assertEventually(cluster, healedRead::isCompleted);
            assertTrue(healedRead.getResult().found(), "Data should be retrievable after healing");
            assertArrayEquals(majorityValue, healedRead.getResult().value(), "Majority value should persist after heal");
        }
    }

    @Test
    @DisplayName("Local stale read after quorum write: linearizable=false, sequential=true")
    void localReadAfterQuorumWrite_breaksLin_passesSeq() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE, DELPHI, SPARTA))
                .useSimulatedNetwork()
                .build(QuorumKVReplica::new)
                .start()) {

            var clientWriter = cluster.newClientConnectedTo(ProcessId.of("client1"), CYRENE, QuorumKVClient::new);

            String key = "kv";
            String v0 = "v0";
            String v1 = "v1";

            History<String> history = new History();

            // Step 1: initialize value via quorum so all nodes converge to v0
            history.invoke(ProcessId.of("client1"), Op.WRITE, v0);
            var init = clientWriter.set(key.getBytes(), v0.getBytes());
            assertEventually(cluster, init::isCompleted);
            assertTrue(init.getResult().success());
            history.ok(ProcessId.of("client1"), Op.WRITE, v0);

            // Step 2: partition nodes so writer's majority excludes ATHENS
            var writerSide = NodeGroup.of(CYRENE, DELPHI, SPARTA);
            var otherSide = NodeGroup.of(ATHENS, BYZANTIUM);
            cluster.partitionNodes(writerSide, otherSide);

            // Step 3: quorum write v1 on writer side
            history.invoke(ProcessId.of("client1"), Op.WRITE, v1);
            var w1 = clientWriter.set(key.getBytes(), v1.getBytes());
            assertEventually(cluster, completesSuccessfully(w1));
            history.ok(ProcessId.of("client1"), Op.WRITE, v1);

            // Step 4: different client performs local read on ATHENS (single-node read), sees stale v0
            history.invoke(ProcessId.of("client2"), Op.READ, null);
            var vv = cluster.getStorageValue(ATHENS, key.getBytes());
            assertNotNull(vv);
            assertArrayEquals(v0.getBytes(), vv.value());
            history.ok(ProcessId.of("client2"), Op.READ, new String(vv.value()));

            // Step 5: analyze: linearizable should fail; sequential should pass (different client)
            String edn = history.toEdn();
            System.out.println("edn = " + edn);
            boolean lin = ConsistencyChecker.check(edn, ConsistencyProperty.LINEARIZABILITY, DataModel.REGISTER);
            boolean seq = ConsistencyChecker.check(edn, ConsistencyProperty.SEQUENTIAL_CONSISTENCY, DataModel.REGISTER);
            assertFalse(lin, "Stale read after successful write should not be linearizable");
            assertTrue(seq, "Across clients, stale read can be sequential by reordering");
        }
    }

    @Test
    @DisplayName("Clock skew: minority (higher timestamp) wins after heal")
    /**
     * <-- MINORITY PARTITION (2) --> x <-- MAJORITY PARTITION (3) -->
     *   Athens | Byzantium          x      Cyrene | Delphi | Sparta
     *      |   |                    x         |   |   |
     * (Step 1: Initial write from Majority Client; all nodes converge on v0)
     *      |   |<---- set(k,v0) -- quorum -- set(k,v0) ---->|   |
     *      |   |                    x         |   |   |
     * (Step 2: Network is partitioned)
     * ------------------------------x---------------------------------
     *      |   |                    x         |   |   |
     * (Step 3: Minority write fails at client but persists on A,B with high ts1)
     * [mCli]--set(k,v1)-->|   |     x         |   |   |
     *      |<--| (2/5 no quorum)    x         |   |   |
     * [mCli]<--[TIMEOUT]            x         |   |   |
     *      (A,B have v1, ts1)       x         (C,D,S have v0)
     *      |   |                    x         |   |   |
     * (Step 4: Majority clock skewed back; write succeeds with low ts0)
     *      |   |                    x      (Clock on C set to ts1-SKEW)
     *      |   |                    x         |   |   |
     *      |   |                    x         |<--set(k,v2)--[MCli]
     *      |   |                    x      (3/3 quorum) |   |
     *      |   |                    x         |----[OK]---->[MCli]
     *      (A,B have v1, ts1)     x      (C,D,S have v2, ts0)
     *      |   |                    x         |   |   |
     * (Step 5: Partitions are healed)
     * -----------------------------------------------------------------
     *      |   |                               |   |   |
     * (Step 6: Read from any node triggers resolution. Higher timestamp wins)
     * [mCli]-->| get(k)                          |   |   |
     *      |---|----->| (Read repair starts)    |<--|---|
     *      |   |      Sees (v1,ts1) from A/B    |   |   |
     *      |   |      Sees (v2,ts0) from C/D/S  |   |   |
     *      |   |                               |   |   |
     *      |   |      (ts1 > ts0, so v1 wins)   |   |   |
     *      |   |<----(Propagate v1)------------|---|--->|
     *      |   |                               |   |   |
     * [mCli]<--|- - - - (returns v1) - - - - - -|   |   |
     *      |   |                               |   |   |
     *      (All nodes eventually converge on v1: the "old minority" value)
     */
    void clockSkewOverwritesMajorityValue() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE, DELPHI, SPARTA))
                .useSimulatedNetwork()
                .build(QuorumKVReplica::new)
                .start()) {

            var minorityClient = cluster.newClientConnectedTo(MINORITY_CLIENT, ATHENS, QuorumKVClient::new);
            var majorityClient = cluster.newClientConnectedTo(MAJORITY_CLIENT, CYRENE, QuorumKVClient::new);

            String key = "distributed_ledger";
            String initialValue = "genesis_block";
            String minorityValue = "minority_attempt";
            String majorityValue = "majority_success";

            // Step 0: start recording a client-observed history for Jepsen analysis
            History<String> history = new History<>();

            // Step 1: majority-side write of initialValue; cluster converges on v0
            history.invoke(ProcessId.of("majority_client"), Op.WRITE, initialValue);
            var initialSet = majorityClient.set(key.getBytes(), initialValue.getBytes());
            assertEventually(cluster, completesSuccessfully(initialSet));
            assertAllNodeStoragesContainValue(cluster, key.getBytes(), initialValue.getBytes());
            history.ok(ProcessId.of("majority_client"), Op.WRITE, initialValue);

            // Step 2: partition cluster into minority (2) and majority (3)
            cluster.partitionNodes(NodeGroup.of(ATHENS, BYZANTIUM), NodeGroup.of(CYRENE, DELPHI, SPARTA));

            // Step 3: minority write times out at client but persists locally in its partition
            history.invoke(ProcessId.of("minority_client"), Op.WRITE, minorityValue);
            var minorityWrite = minorityClient.set(key.getBytes(), minorityValue.getBytes());
            assertEventually(cluster, minorityWrite::isFailed);
            assertNodesContainValue(cluster, List.of(ATHENS, BYZANTIUM), key.getBytes(), minorityValue.getBytes());
            history.fail(ProcessId.of("minority_client"), Op.WRITE, minorityValue);

            // Step 4: skew majority clock behind minority; majority write now has lower timestamp
            var athensTs = cluster.getStorageValue(ATHENS, key.getBytes()).timestamp();
            cluster.setTimeForProcess(CYRENE, athensTs - SKEW_TICKS);

            history.invoke(ProcessId.of("majority_client"), Op.WRITE, majorityValue);
            var majorityWrite = majorityClient.set(key.getBytes(), majorityValue.getBytes());
            assertEventually(cluster, completesSuccessfully(majorityWrite));
            history.ok(ProcessId.of("majority_client"), Op.WRITE, majorityValue);

            // Step 5: heal partitions; higher timestamp (minority) should prevail cluster-wide
            cluster.healAllPartitions();

            history.invoke(ProcessId.of("minority_client"), Op.READ, null);
            var healedRead = minorityClient.get(key.getBytes());
            assertEventually(cluster, healedRead::isCompleted);
            assertTrue(healedRead.getResult().found(), "Data should be retrievable after healing");
            assertArrayEquals(minorityValue.getBytes(), healedRead.getResult().value(),
                    "Minority value (higher timestamp) should win after heal with clock skew");
            history.ok(ProcessId.of("minority_client"), Op.READ, new String(healedRead.getResult().value()));

            // Step 6: prove not linearizable (real-time precedence) and not sequential (no serial order preserves results)
            // - Linearizability fails: after healing, the read observes the minority value written in a different
            //   partition while a successful majority write also occurred. There is no placement respecting real-time precedence.
            // - Sequential consistency fails: even ignoring real-time order, no single serial order yields the observed read.
            String edn = history.toEdn();
            System.out.println("edn = " + edn);
            boolean linearizable = ConsistencyChecker.check(edn, ConsistencyProperty.LINEARIZABILITY, DataModel.REGISTER);
            assertFalse(linearizable, "History should be non-linearizable: failed write took effect");

            boolean okSeq = ConsistencyChecker.check(edn, ConsistencyProperty.SEQUENTIAL_CONSISTENCY, DataModel.REGISTER);
            assertFalse(okSeq);
        }
    }

    private static BooleanSupplier completesSuccessfully(ListenableFuture<SetResponse> w2) {
        return () -> w2.isCompleted() && w2.getResult().success();
    }

}

