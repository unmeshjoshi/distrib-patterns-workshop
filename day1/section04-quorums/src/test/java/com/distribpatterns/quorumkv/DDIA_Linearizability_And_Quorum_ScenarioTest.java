package com.distribpatterns.quorumkv;

import com.tickloom.ProcessId;
import com.tickloom.future.ListenableFuture;
import com.tickloom.storage.VersionedValue;
import com.tickloom.testkit.ClusterTest;
import com.tickloom.testkit.NodeGroup;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class DDIA_Linearizability_And_Quorum_ScenarioTest
        extends ClusterTest<QuorumKVClient, GetResponse, String> {

    // 3 replicas
    private static final ProcessId ATHENS = ProcessId.of("athens");
    private static final ProcessId BYZANTIUM = ProcessId.of("byzantium");
    private static final ProcessId CYRENE = ProcessId.of("cyrene");

    // clients
    private static final ProcessId ALICE = ProcessId.of("alice");
    private static final ProcessId BOB = ProcessId.of("bob");
    private static final ProcessId WRITER = ProcessId.of("writer");

    // delay window (~10 ticks) to hold back propagation from ATHENS to others
    private static final int PROP_DELAY_TICKS = 100;

    public DDIA_Linearizability_And_Quorum_ScenarioTest() throws IOException {
        super(
                List.of(ATHENS, BYZANTIUM, CYRENE),
                QuorumKVReplica::new,
                QuorumKVClient::new,
                r -> r == null || r.value() == null ? null : new String(r.value(), StandardCharsets.UTF_8)
        );
    }

    @Test
    @DisplayName("DDIA §10.6 via per-link delay: Alice sees new, later Bob sees old (LIN ❌, SC ✅)")
    void ddia106_link_delay_race()  {
        //Updates the order status..
        var orderStatusUpdater = clientConnectedTo(WRITER, ATHENS);

        var alice = clientConnectedTo(ALICE, BYZANTIUM);    // Alice reads via ATHENS
        var bob = clientConnectedTo(BOB, BYZANTIUM); // Bob reads via BYZANTIUM

        // Realistic key/values
        byte[] key = "order:1001".getBytes(StandardCharsets.UTF_8);
        byte[] vOld = "x=0".getBytes(StandardCharsets.UTF_8);
        byte[] vNew = "x=1".getBytes(StandardCharsets.UTF_8);

        // Seed initial state across the cluster (no delays yet) so everyone agrees on "PLACED"
        write(withWriter(orderStatusUpdater, key, vOld));
        assertNodesContainValue(List.of(ATHENS, BYZANTIUM, CYRENE), key, vOld);

        // Introduce *per-link delay* from ATHENS -> (BYZANTIUM, CYRENE)
        // This holds back propagation of the upgrade write for ~PROP_DELAY_TICKS.
        delayForMessageType(QuorumKVMessageTypes.INTERNAL_SET_REQUEST, ATHENS, List.of(BYZANTIUM, CYRENE), PROP_DELAY_TICKS);

        // Writer upgrades to SHIPPED via ATHENS. ATHENS applies promptly; others are delayed.
        // We need to wait until the value is available on ATHENS.
        writeAndWaitUntil(withWriter(orderStatusUpdater, key, vNew), () -> {
            VersionedValue storageValue = cluster.getStorageValue(ATHENS, key);
            if (null == storageValue) return false;
            return Arrays.equals(vNew, storageValue.value());
        });

        // Sanity: immediately after the write, ATHENS has vNew while others still have vOld
        assertNodesContainValue(List.of(ATHENS), key, vNew);
        assertNodesContainValue(List.of(BYZANTIUM, CYRENE), key, vOld);


        //make sure byzatium is partitioned away from CYRENE, so it gets the values from itself and ATHENS
        //We have not implemented read-repair.. so no repair happens on BYZANTIUM.
        partition(NodeGroup.of(BYZANTIUM), NodeGroup.of(CYRENE));

        // --- A L I C E ---
        // Alice performs a read via ATHENS. Quorum includes a fresh replica (ATHENS),
        // so Alice returns the NEW value.
        read(withReader(alice, key)); // → vNew

        assertNodesContainValue(List.of(BYZANTIUM, CYRENE), key, vOld); //BYZANTIUM and CYRENE still have vOld

        //make sure byzatium is partitioned away from athens, so it can only form majority with cyrene
        cluster.reconnectProcess(BYZANTIUM);
        partition(NodeGroup.of(BYZANTIUM), NodeGroup.of(ATHENS));

        assertNodesContainValue(List.of(BYZANTIUM, CYRENE), key, vOld); //BYZANTIUM and CYRENE still have vOld

        // --- B O B ---
        // Bob reads via BYZANTIUM, whose quorum (e.g., {BYZANTIUM, CYRENE}) is still OLD at this instant,
        // so Bob returns the OLD value—even though Alice already saw NEW.
        read(withReader(bob, key)); // → vOld

        // Let delayed messages arrive; replicas converge.
        assertEventually(() -> {
           return List.of(ATHENS, BYZANTIUM, CYRENE).stream().allMatch(node -> {
                return getNodeValue(node, key).isPresent() && Arrays.equals(vNew, getNodeValue(node, key).get());
            });
        });

        assertEquals("[{:process 0, :process-name \"writer\", :type :invoke, :f :write, :value \"x=0\"} " +
                        "{:process 0, :process-name \"writer\", :type :ok, :f :write, :value \"x=0\"} " +
                        "{:process 0, :process-name \"writer\", :type :invoke, :f :write, :value \"x=1\"} " +
                        "{:process 1, :process-name \"alice\", :type :invoke, :f :read, :value nil} " +
                        "{:process 1, :process-name \"alice\", :type :ok, :f :read, :value \"x=1\"} " +
                        "{:process 2, :process-name \"bob\", :type :invoke, :f :read, :value nil} " +
                        "{:process 2, :process-name \"bob\", :type :ok, :f :read, :value \"x=0\"} " +
                        "{:process 0, :process-name \"writer\", :type :ok, :f :write, :value \"x=1\"}]",
                getHistory().toEdn());
        // Consistency verdicts:
        // Later Bob saw older → not linearizable.
        assertLinearizability(false);

        // Different clients (Alice vs Bob), no same-client regression → SC can be satisfied.
        assertSequentialConsistency(true);
    }

    private static ClusterTest.Reader<QuorumKVClient, GetResponse, String> withReader(QuorumKVClient reconnectedClient2, byte[] key) {
        return new ClusterTest.Reader<>(reconnectedClient2) {
            @Override
            public Supplier<ListenableFuture<GetResponse>> getSupplier() {
                return () -> reconnectedClient2.get(key);
            }
        };
    }

    private ClusterTest.Writer<QuorumKVClient, String> withWriter(QuorumKVClient client2, byte[] key, byte[] value) {
        return new ClusterTest.Writer<>(client2, new String(key), new String(value)) {

            @Override
            public String attemptedValue() {
                return new String(value);
            }

            @Override
            public Supplier<ListenableFuture<?>> getSupplier() {
                return () -> client2.set(key, value);
            }
        };
    }
}

