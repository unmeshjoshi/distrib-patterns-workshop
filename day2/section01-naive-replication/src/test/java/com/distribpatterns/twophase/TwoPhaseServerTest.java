package com.distribpatterns.twophase;

import com.distribpatterns.twophase.messages.ExecuteResponse;
import com.tickloom.ProcessId;
import com.tickloom.future.ListenableFuture;
import com.tickloom.testkit.Cluster;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static com.tickloom.testkit.ClusterAssertions.assertEventually;
import static org.junit.Assert.assertEquals;

class TwoPhaseServerTest {
    // Replica nodes
    private static final ProcessId ATHENS = ProcessId.of("athens");
    private static final ProcessId BYZANTIUM = ProcessId.of("byzantium");
    private static final ProcessId CYRENE = ProcessId.of("cyrene");

    // Clients
    private static final ProcessId ALICE = ProcessId.of("alice");

    @Test
    @DisplayName("Scenario 1: Two-Phase Commit - client sees success after commit executes on coordinator")
    void testClientSeesSuccessAfterCommitExecutesOnCoordinator() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new TwoPhaseServer(peerIds, processParams))
                .start()) {

            var client = cluster.newClientConnectedTo(ALICE, ATHENS, TwoPhaseClient::new);
            ListenableFuture<ExecuteResponse> executeResponse =
                client.execute(ATHENS, new IncrementCounterOperation("counter_key", 2));
            assertEventually(cluster, () -> executeResponse.isCompleted() && !executeResponse.isFailed());

            TwoPhaseServer athensServer = cluster.getNode(ATHENS);
            assertEquals(Integer.valueOf(2), athensServer.getCounterValue("counter_key"));

        }
    }
}
