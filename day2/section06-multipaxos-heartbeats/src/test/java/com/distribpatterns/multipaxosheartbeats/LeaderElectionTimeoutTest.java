package com.distribpatterns.multipaxosheartbeats;

import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Leader election driven by election timeouts the test chooses. The node with the shortest timeout is the first to
 * notice there is no leader, so the test decides who leads.
 */
public class LeaderElectionTimeoutTest {

    private static final ProcessId ATHENS = ProcessId.of("athens");
    private static final ProcessId BYZANTIUM = ProcessId.of("byzantium");
    private static final ProcessId CYRENE = ProcessId.of("cyrene");
    private static final List<ProcessId> ALL = List.of(ATHENS, BYZANTIUM, CYRENE);

    // Far enough apart that one node's election finishes before the next node's timeout fires.
    private static final Map<ProcessId, Long> ELECTION_TIMEOUTS = Map.of(
            ATHENS, 100L,
            BYZANTIUM, 200L,
            CYRENE, 300L);

    private static Cluster newCluster() throws IOException {
        return new Cluster()
                .withProcessIds(ALL)
                .useSimulatedNetwork()
                .build((peerIds, processParams) -> new MultiPaxosWithHeartbeatsServer(
                        peerIds, processParams, ELECTION_TIMEOUTS.get(processParams.id())))
                .start();
    }

    private static MultiPaxosWithHeartbeatsServer server(Cluster cluster, ProcessId id) {
        return (MultiPaxosWithHeartbeatsServer) cluster.getProcess(id);
    }

    private static void tick(Cluster cluster, long ticks) {
        for (int i = 0; i < ticks; i++) {
            cluster.tick();
        }
    }

    @Test
    @DisplayName("The node with the shortest election timeout becomes the first leader")
    void shortestTimeoutBecomesFirstLeader() throws IOException {
        try (var cluster = newCluster()) {
            cluster.tickUntil(() -> ALL.stream().allMatch(id -> server(cluster, id).isInitialised()));
            var athens = server(cluster, ATHENS);
            var byzantium = server(cluster, BYZANTIUM);
            var cyrene = server(cluster, CYRENE);

            // Nobody stands for election before its timeout fires.
            tick(cluster, ELECTION_TIMEOUTS.get(ATHENS) - 1);
            assertTrue(ALL.stream().noneMatch(id -> server(cluster, id).isLeader()));
            assertEquals(0, athens.getPromisedGeneration());

            // Athens times out first and wins generation 1.
            cluster.tickUntil(athens::isLeader);
            assertEquals(1, athens.getPromisedGeneration());

            // Its heartbeats keep the others following, well past their own timeouts.
            tick(cluster, ELECTION_TIMEOUTS.get(CYRENE) * 2);
            assertTrue(athens.isLeader());
            assertTrue(byzantium.isFollower());
            assertTrue(cyrene.isFollower());
            assertEquals(1, byzantium.getPromisedGeneration());
            assertEquals(1, cyrene.getPromisedGeneration());
        }
    }

    @Test
    @DisplayName("When the leader crashes, the follower with the next shortest timeout takes over")
    void nextShortestTimeoutTakesOverWhenLeaderCrashes() throws IOException {
        try (var cluster = newCluster()) {
            var athens = server(cluster, ATHENS);
            cluster.tickUntil(athens::isLeader);
            var byzantium = server(cluster, BYZANTIUM);
            var cyrene = server(cluster, CYRENE);
            cluster.tickUntil(() -> byzantium.isFollower() && byzantium.getPromisedGeneration() == 1
                    && cyrene.isFollower() && cyrene.getPromisedGeneration() == 1);

            cluster.crashNode(ATHENS);

            // The followers wait out their timeouts before suspecting the leader. The margin allows for the last
            // heartbeat still being in flight when athens crashed.
            tick(cluster, ELECTION_TIMEOUTS.get(BYZANTIUM) - 10);
            assertTrue(byzantium.isFollower());
            assertTrue(cyrene.isFollower());

            // Byzantium times out before cyrene and wins generation 2 with cyrene's promise.
            cluster.tickUntil(byzantium::isLeader);
            assertEquals(2, byzantium.getPromisedGeneration());

            tick(cluster, ELECTION_TIMEOUTS.get(CYRENE) * 2);
            assertTrue(byzantium.isLeader());
            assertTrue(cyrene.isFollower());
            assertEquals(2, cyrene.getPromisedGeneration());
        }
    }
}
