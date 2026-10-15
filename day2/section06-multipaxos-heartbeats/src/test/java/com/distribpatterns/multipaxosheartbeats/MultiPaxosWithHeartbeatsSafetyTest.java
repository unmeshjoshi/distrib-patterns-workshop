package com.distribpatterns.multipaxosheartbeats;

import com.distribpatterns.multipaxosheartbeats.messages.ExecuteCommandResponse;
import com.distribpatterns.multipaxosheartbeats.messages.Operation;
import com.distribpatterns.multipaxosheartbeats.messages.SetValueOperation;
import com.tickloom.ProcessId;
import com.tickloom.future.TickCompletableFuture;
import com.tickloom.testkit.Cluster;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Randomised safety test: client writes, crashes, restarts and partitions, driven by a seed.
 *
 * The property checked is the one Paxos promises: once a value is committed at an index, no node ever has a
 * different committed value there. The checker remembers the first committed value seen at each index for the whole
 * run, because replicas that have all overwritten an entry agree with each other; only a value that changes over time
 * shows the bug.
 */
public class MultiPaxosWithHeartbeatsSafetyTest {

    private static final ProcessId ATHENS = ProcessId.of("athens");
    private static final ProcessId BYZANTIUM = ProcessId.of("byzantium");
    private static final ProcessId CYRENE = ProcessId.of("cyrene");
    private static final List<ProcessId> REPLICAS = List.of(ATHENS, BYZANTIUM, CYRENE);
    private static final ProcessId CLIENT = ProcessId.of("client");

    private static final int SEEDS = 50;
    private static final int TICKS_PER_RUN = 3000;

    @Test
    @DisplayName("Safety: a committed entry never changes under crashes, restarts and partitions")
    void committedEntriesNeverChange() throws IOException {
        for (long seed = 1; seed <= SEEDS; seed++) {
            runWithSeed(seed);
        }
    }

    private void runWithSeed(long seed) throws IOException {
        Random random = new Random(seed);
        Map<Integer, Operation> firstCommitted = new HashMap<>();
        Set<Integer> leaderGenerations = new HashSet<>();

        try (var cluster = new Cluster()
            .withProcessIds(REPLICAS)
            .withSeed(seed)
            .useSimulatedNetwork()
            .build((peerIds, processParams) -> new MultiPaxosWithHeartbeatsServer(peerIds, processParams))
            .start()) {

            cluster.tickUntil(cluster::areAllNodesInitialized);
            var client = cluster.newClientConnectedTo(CLIENT, ATHENS, MultiPaxosWithHeartbeatsClient::new);

            ProcessId down = null;       // at most one node is crashed or cut off at a time, so a quorum remains
            boolean downIsCrashed = false;
            int writes = 0;

            for (int tick = 0; tick < TICKS_PER_RUN; tick++) {
                if (random.nextInt(5) == 0) {
                    ProcessId target = REPLICAS.get(random.nextInt(REPLICAS.size()));
                    client.execute(target, new SetValueOperation("key-" + seed + "-" + writes, "value-" + writes));
                    writes++;
                }

                if (down == null && random.nextInt(150) == 0) {
                    // Most often the leader, because a leader change is where entries can be lost.
                    ProcessId leader = currentLeader(cluster);
                    down = leader != null && random.nextBoolean() ? leader : REPLICAS.get(random.nextInt(REPLICAS.size()));
                    downIsCrashed = random.nextBoolean();
                    if (downIsCrashed) {
                        cluster.crashNode(down);
                    } else {
                        cluster.isolateProcess(down);
                    }
                } else if (down != null && random.nextInt(100) == 0) {
                    if (downIsCrashed) {
                        cluster.restartNode(down);
                    } else {
                        cluster.reconnectProcess(down);
                    }
                    down = null;
                }

                cluster.tick();
                REPLICAS.stream()
                    .filter(id -> isRunningLeader(cluster, id))
                    .forEach(id -> leaderGenerations.add(cluster.<MultiPaxosWithHeartbeatsServer>getNode(id).getPromisedGeneration()));
                checkCommittedEntriesNeverChange(cluster, firstCommitted, seed, tick);
            }

            if (down != null) {
                if (downIsCrashed) {
                    cluster.restartNode(down);
                } else {
                    cluster.reconnectProcess(down);
                }
            }
            assertTrue(writeEventuallySucceeds(cluster, client, firstCommitted, seed),
                "seed " + seed + ": a write should succeed once all faults are healed");

            assertTrue(leaderGenerations.size() > 1, "seed " + seed + ": the run should include a leader change");
            assertTrue(firstCommitted.size() >= 10,
                "seed " + seed + ": the run should commit some entries, got " + firstCommitted.size());
        }
    }

    private static boolean writeEventuallySucceeds(Cluster cluster,
                                                   MultiPaxosWithHeartbeatsClient client,
                                                   Map<Integer, Operation> firstCommitted,
                                                   long seed) {
        for (int attempt = 0; attempt < 20; attempt++) {
            // A new election repairs a log the old leader left with a gap, for example after its PROPOSE replies
            // were lost; without one, the leader keeps heartbeating while its execution waits on that gap forever.
            cluster.tickUntil(cluster::areAllNodesInitialized);
            MultiPaxosWithHeartbeatsServer candidate = cluster.getNode(REPLICAS.get(attempt % REPLICAS.size()));
            candidate.startElection();
            for (int tick = 0; tick < 200 && !candidate.isLeader(); tick++) {
                cluster.tick();
            }
            if (!candidate.isLeader()) {
                continue;
            }
            TickCompletableFuture<ExecuteCommandResponse> write =
                client.execute(candidate.id, new SetValueOperation("final-" + seed, "done"));
            for (int tick = 0; tick < 500 && !write.isCompleted(); tick++) {
                cluster.tick();
                checkCommittedEntriesNeverChange(cluster, firstCommitted, seed, -1);
            }
            if (write.isCompleted() && write.getResult() != null && write.getResult().success()) {
                return true;
            }
        }
        return false;
    }

    private static void checkCommittedEntriesNeverChange(Cluster cluster,
                                                         Map<Integer, Operation> firstCommitted,
                                                         long seed,
                                                         int tick) {
        for (ProcessId id : REPLICAS) {
            MultiPaxosWithHeartbeatsServer server = cluster.getNode(id);
            if (server.isStopped() || !server.isInitialised()) {
                continue;
            }
            server.getPaxosLog().forEach((index, state) -> state.committedValue().ifPresent(value -> {
                Operation first = firstCommitted.putIfAbsent(index, value);
                assertTrue(first == null || first.equals(value),
                    "seed " + seed + ", tick " + tick + ": " + id + " has " + value + " committed at index " + index +
                        ", but " + first + " was committed there before");
            }));
        }
    }

    private static ProcessId currentLeader(Cluster cluster) {
        return REPLICAS.stream().filter(id -> isRunningLeader(cluster, id)).findFirst().orElse(null);
    }

    private static boolean isRunningLeader(Cluster cluster, ProcessId id) {
        MultiPaxosWithHeartbeatsServer server = cluster.getNode(id);
        return !server.isStopped() && server.isLeader();
    }
}
