package com.distribpatterns.naive;

import com.tickloom.Process;
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

class NaiveReplicationServerTest {
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
                .build(NaiveReplicationServer::new)
                .start()) {

            var client = cluster.newClientConnectedTo(ALICE, ATHENS, CounterClient::new);
            //Add tick delay for replication messages from athens to byzantium and cyrene
            int delay = getDelay(cluster, 2, 3, 1);
            cluster.delayForMessageType(MessageTypes.REPLICATE_OP, ATHENS, List.of(BYZANTIUM, CYRENE), delay);

            String COUNTER_KEY = "counterKey";
            ListenableFuture<IncrementCounterResponse> counterKey = client.increment(ATHENS, COUNTER_KEY, 2);
            //assertEventually ticks only until response is received.
            // Because athens immediately sends the response and then sends the replication message to followers.
            // So it will require one more tick for followers to process the messages.
            assertEventually(cluster, completesSuccessfully(counterKey));

            NaiveReplicationServer athensServer = getServerInstance(cluster, ATHENS);
            assertEquals(Integer.valueOf(2), athensServer.getContainerValue(COUNTER_KEY));

            NaiveReplicationServer byzantiumServer = getServerInstance(cluster, BYZANTIUM);
            assertNull(byzantiumServer.getContainerValue(COUNTER_KEY));

            NaiveReplicationServer cyreneServer = getServerInstance(cluster, CYRENE);
            assertNull(cyreneServer.getContainerValue(COUNTER_KEY));

        }
    }

    //TODO: Add this method to cluster class
    private int getDelay(Cluster cluster, int desiredDelayTicks, int numServerNodes, int numClientNodes) {
        // IMPORTANT: Network tick multiplier calculation
//
// In tickloom's simulated network, the shared network instance is ticked by EACH node
// during cluster.tick(). This means:
//   - cluster.tick() → advances time by 1ms for all processes
//   - But network.tick() is called N times per cluster tick (once per node)
//   - Where N = numServerNodes + numClientNodes
//
// In this test:
//   - 3 server nodes (ATHENS, BYZANTIUM, CYRENE)
//   - 1 client node (ALICE)
//   - Total: 4 nodes ticking the shared network
//
// Therefore: 1 cluster.tick() = 4 network.tick() calls
//
// To delay replication messages by X "logical/cluster ticks", we need to set:
//   networkDelayTicks = (numNodes + numClients) * X
//
// Example: To keep replication messages delayed for 2 cluster ticks:
//   delayTicks = (3 + 1) * 2 = 8 network ticks
//
// This demonstrates NAIVE REPLICATION's key flaw:
//   - Client receives SUCCESS response immediately
//   - But followers haven't received/processed the replication yet
//   - Creates a window where client thinks write succeeded but data isn't replicated
//   - If leader crashes in this window, data is LOST despite client receiving success!
        // server nodes
        // client nodes
        int delayTicks = (numServerNodes + numClientNodes) * desiredDelayTicks;
        return delayTicks;
    }

    //TODO. Move following methods to Cluster class.
    private  static <T extends Process>  T getServerInstance(Cluster cluster, ProcessId processId) {
        return (T) cluster.getProcess(processId);
    }

    private static BooleanSupplier completesSuccessfully(ListenableFuture<IncrementCounterResponse> future) {
        return () -> future.isCompleted() && future.getResult().success();
    }
}