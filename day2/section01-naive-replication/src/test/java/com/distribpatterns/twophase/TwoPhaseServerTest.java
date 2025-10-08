package com.distribpatterns.twophase;

import com.tickloom.ProcessId;
import com.tickloom.future.ListenableFuture;
import com.tickloom.testkit.Cluster;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static com.distribpatterns.naive.NaiveReplicationServerTest.getServerInstance;
import static com.tickloom.testkit.ClusterAssertions.assertEventually;
import static org.junit.Assert.assertEquals;

class TwoPhaseServerTest {
    // Replica nodes (matching scenario descriptions)
    private static final ProcessId ATHENS = ProcessId.of("athens");
    private static final ProcessId BYZANTIUM = ProcessId.of("byzantium");
    private static final ProcessId CYRENE = ProcessId.of("cyrene");

    // Clients (actors from scenarios)
    private static final ProcessId ALICE = ProcessId.of("alice");

    @Test
    @DisplayName("Scenario 1: Naive Replication - Athens increments counter, replicated to followers")
    void testQuorumWrite() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build(TwoPhaseServer::new)
                .start()) {

            var client = cluster.newClientConnectedTo(ALICE, ATHENS, TwoPhaseClient::new);
            ListenableFuture<ExecuteResponse> future = client.execute(ATHENS, new IncrementCounterOperation("counter_key", 2));
            assertEventually(cluster, ()-> future.isCompleted() && !future.isFailed());

            TwoPhaseServer athensServer = getServerInstance(cluster, ATHENS);
            assertEquals(Integer.valueOf(2), athensServer.getCounterValue("counter_key"));

        }
    }
}