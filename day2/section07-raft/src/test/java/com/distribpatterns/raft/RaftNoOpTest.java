package com.distribpatterns.raft;

import com.distribpatterns.raft.messages.NoOpOperation;
import com.distribpatterns.raft.messages.RaftMessageTypes;
import com.distribpatterns.raft.messages.SetValueOperation;
import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import com.tickloom.testkit.NodeGroup;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A leader commits only entries of its own term (§5.4.2). Entries left over from an earlier term are committed only
 * along with a later entry of the leader's own term, even when they are already on a majority. The new leader
 * appends a no-op for that, so it does not have to wait for a client write.
 */
public class RaftNoOpTest {
    private static final ProcessId ATHENS = ProcessId.of("athens");
    private static final ProcessId BYZANTIUM = ProcessId.of("byzantium");
    private static final ProcessId CYRENE = ProcessId.of("cyrene");
    private static final ProcessId DELPHI = ProcessId.of("delphi");
    private static final ProcessId EPHESUS = ProcessId.of("ephesus");
    private static final List<ProcessId> ALL = List.of(ATHENS, BYZANTIUM, CYRENE, DELPHI, EPHESUS);
    private static final List<ProcessId> SURVIVORS = List.of(BYZANTIUM, CYRENE, DELPHI, EPHESUS);

    private static final ProcessId ALICE = ProcessId.of("alice");

    // Athens times out first and leads term 1. When it crashes, byzantium times out before the others and leads
    // term 2. (RaftServer adds up to the same again as jitter, so the ranges do not overlap.)
    private static final Map<ProcessId, Integer> ELECTION_TIMEOUTS = Map.of(
            ATHENS, 100,
            BYZANTIUM, 300,
            CYRENE, 1000,
            DELPHI, 1000,
            EPHESUS, 1000);

    // Log index 0 is the sentinel, 1 is athens's no-op for term 1, and the client writes go at 2, 3 and 4.
    private static final int TERM_1_NO_OP = 1;
    private static final int FIRST_WRITE = 2;
    private static final int LAST_WRITE = 4;
    private static final int TERM_2_NO_OP = 5;

    @Test
    @DisplayName("Entries from an earlier term on a majority are committed by the new leader's no-op")
    void entriesFromAnEarlierTermAreCommittedByTheNewLeadersNoOp() throws IOException {
        try (var cluster = new Cluster()
                .withProcessIds(ALL)
                .useSimulatedNetwork()
                .build((peerIds, processParams) ->
                        new RaftServer(peerIds, processParams, ELECTION_TIMEOUTS.get(processParams.id())))
                .start()) {

            // 1. Five servers. Athens is elected for term 1 and everyone commits its no-op.
            cluster.tickUntil(() -> server(cluster, ATHENS).isLeader());
            cluster.tickUntil(() -> ALL.stream().allMatch(id -> server(cluster, id).getCommitIndex() == TERM_1_NO_OP));

            // 2. A partition cuts athens off from delphi and ephesus, and athens never hears that byzantium and
            //    cyrene accepted the writes. So the writes reach three servers and athens cannot commit them.
            cluster.partitionNodes(NodeGroup.of(ATHENS), NodeGroup.of(DELPHI, EPHESUS));
            cluster.dropMessagesOfType(RaftMessageTypes.APPEND_ENTRIES_RESPONSE,
                    NodeGroup.of(BYZANTIUM, CYRENE), NodeGroup.of(ATHENS));

            var client = cluster.newClientConnectedTo(ALICE, ATHENS, RaftClient::new);
            client.execute(ATHENS, new SetValueOperation("title", "Microservices"));
            client.execute(ATHENS, new SetValueOperation("author", "Martin"));
            client.execute(ATHENS, new SetValueOperation("year", "2014"));

            cluster.tickUntil(() -> List.of(ATHENS, BYZANTIUM, CYRENE).stream()
                    .allMatch(id -> lastLogIndex(cluster, id) == LAST_WRITE));
            assertEquals(TERM_1_NO_OP, lastLogIndex(cluster, DELPHI));
            assertEquals(TERM_1_NO_OP, lastLogIndex(cluster, EPHESUS));
            assertEquals(TERM_1_NO_OP, server(cluster, ATHENS).getCommitIndex(), "athens cannot commit the writes");

            // 3. Athens fails before it commits them.
            cluster.crashNode(ATHENS);

            // 4. Byzantium is elected for term 2. Only byzantium or cyrene could be: delphi and ephesus miss the
            //    writes, so their logs are not up to date (§5.4.1).
            cluster.tickUntil(() -> server(cluster, BYZANTIUM).isLeader());
            RaftServer leader = server(cluster, BYZANTIUM);
            assertEquals(2, leader.getCurrentTerm());

            // The writes are term 1 entries on three of five servers, a majority, yet they are not committed:
            // the new leader cannot commit them by counting replicas, because they are not from its own term.
            for (int index = FIRST_WRITE; index <= LAST_WRITE; index++) {
                for (ProcessId id : List.of(BYZANTIUM, CYRENE)) {
                    assertEquals(1, server(cluster, id).getLogEntry(index).term());
                }
            }
            assertEquals(TERM_1_NO_OP, leader.getCommitIndex());

            // 5. The no-op of the leader's own term is already in its log. Becoming leader and appending it are one
            //    step, with no tick between: a Raft leader writes to its own log locally, not by sending itself a
            //    message as a Paxos proposer that broadcasts to every node including itself does. The followers get
            //    the no-op only through AppendEntries.
            LogEntry noOp = leader.getLogEntry(TERM_2_NO_OP);
            assertEquals(2, noOp.term());
            assertInstanceOf(NoOpOperation.class, noOp.operation());

            // Once the no-op is on a majority it is committed, and every entry before it with it.
            cluster.tickUntil(() -> leader.getCommitIndex() == TERM_2_NO_OP);
            cluster.tickUntil(() -> SURVIVORS.stream().allMatch(id ->
                    "Microservices".equals(server(cluster, id).getValue("title"))
                            && "Martin".equals(server(cluster, id).getValue("author"))
                            && "2014".equals(server(cluster, id).getValue("year"))));
        }
    }

    private static RaftServer server(Cluster cluster, ProcessId id) {
        return cluster.getNode(id);
    }

    private static int lastLogIndex(Cluster cluster, ProcessId id) {
        return server(cluster, id).getLogSize() - 1;
    }
}
