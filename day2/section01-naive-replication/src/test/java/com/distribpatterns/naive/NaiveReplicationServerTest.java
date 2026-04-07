package com.distribpatterns.naive;

import com.distribpatterns.naive.messages.IncrementCounterResponse;
import com.distribpatterns.naive.messages.MessageTypes;
import com.tickloom.ProcessId;
import com.tickloom.future.ListenableFuture;
import com.tickloom.testkit.Cluster;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.function.BooleanSupplier;
import static com.tickloom.testkit.ClusterAssertions.assertEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class NaiveReplicationServerTest {
    // Replica nodes
    private static final ProcessId ATHENS = ProcessId.of("athens");
    private static final ProcessId BYZANTIUM = ProcessId.of("byzantium");
    private static final ProcessId CYRENE = ProcessId.of("cyrene");

    // Clients
    private static final ProcessId ALICE = ProcessId.of("alice");
    // The point of this test is the unsafe success window:
    // the client sees success before followers apply replication.
    //
    // Note: delayed messages may still be delivered after Athens fails, so this
    // test demonstrates replication lag, not deterministic write loss.
    // We can try network partition and crashing the servers to demo the same effect.
    @Test
    @DisplayName("Scenario 1: Naive Replication - client sees success before followers apply replication")
    void testClientSeesSuccessBeforeFollowersApplyReplication() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(List.of(ATHENS, BYZANTIUM, CYRENE))
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new NaiveReplicationServer(peerIds, processParams))
                .start()) {

            var client = cluster.newClientConnectedTo(ALICE, ATHENS, CounterClient::new);

            int delay = cluster.delayForClusterTicks(2);
            cluster.delayForMessageType(MessageTypes.REPLICATE_OP, ATHENS, List.of(BYZANTIUM, CYRENE), delay);


            String COUNTER_KEY = "counterKey";
            ListenableFuture<IncrementCounterResponse> incrementResponse = client.increment(ATHENS, COUNTER_KEY, 2);
            //assertEventually ticks only until response is received.
            // Because athens immediately sends the response and then sends the replication message to followers.
            // So it will require one more tick for followers to process the messages.
            assertEventually(cluster, completesSuccessfully(incrementResponse));

            NaiveReplicationServer athensServer = cluster.getNode(ATHENS);
            assertEquals(Integer.valueOf(2), athensServer.getContainerValue(COUNTER_KEY));

            NaiveReplicationServer byzantiumServer = cluster.getNode(BYZANTIUM);
            assertNull(byzantiumServer.getContainerValue(COUNTER_KEY));

            NaiveReplicationServer cyreneServer = cluster.getNode(CYRENE);
            assertNull(cyreneServer.getContainerValue(COUNTER_KEY));

        }
    }

    private static BooleanSupplier completesSuccessfully(ListenableFuture<IncrementCounterResponse> future) {
        return () -> future.isCompleted() && future.getResult().success();
    }
}
