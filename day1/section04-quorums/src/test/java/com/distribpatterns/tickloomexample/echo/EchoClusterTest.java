package com.distribpatterns.tickloomexample.echo;

import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.tickloom.testkit.ClusterAssertions.assertEventually;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class EchoClusterTest {

    private Cluster cluster;

    @BeforeEach
    void setup() throws Exception {
        cluster = new Cluster()
                .withNumProcesses(1)
                .useSimulatedNetwork()
                .build(EchoServer::new)
                .start();
    }

    @AfterEach
    void teardown() {
        if (cluster != null) cluster.close();
    }

    @Test
    void echo_roundtrip() throws Exception {
        ProcessId serverId = ProcessId.of("process-1");

        EchoClient client = cluster.newClient(ProcessId.of("client-1"), (clientId, endpoints, bus, codec, clock, timeoutTicks) ->
                new EchoClient(clientId, java.util.List.of(serverId), bus, codec, clock, timeoutTicks));

        var future = client.echo(serverId, "hello");
        assertEventually(cluster, () -> future.isCompleted());

        assertEquals("hello", future.getResult().text());
    }
}


