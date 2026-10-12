package com.distribpatterns.raft;

import com.distribpatterns.raft.messages.AppendEntriesRequest;
import com.distribpatterns.raft.messages.AppendEntriesResponse;
import com.distribpatterns.raft.messages.RaftMessageTypes;
import com.distribpatterns.raft.messages.SetValueOperation;
import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.algorithms.replication.ClusterClient;
import com.tickloom.future.TickCompletableFuture;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageType;
import com.tickloom.testkit.Cluster;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A leader commits only entries of its own term, so an entry left over from an earlier term is committed only along
 * with a later entry of the leader's own term. The new leader appends a no-op for that, so it does not have to wait
 * for a client write.
 */
public class RaftNoOpTest {
    private static final ProcessId ATHENS = ProcessId.of("athens");
    private static final ProcessId BYZANTIUM = ProcessId.of("byzantium");
    private static final ProcessId CYRENE = ProcessId.of("cyrene");
    private static final List<ProcessId> ALL = List.of(ATHENS, BYZANTIUM, CYRENE);

    /** Plays a leader of an earlier term that has replicated an entry but not committed it. */
    static class OldLeaderClient extends ClusterClient {
        OldLeaderClient(List<ProcessId> replicaEndpoints, ProcessParams processParams) {
            super(replicaEndpoints, processParams);
        }

        TickCompletableFuture<AppendEntriesResponse> replicate(ProcessId follower, int term, LogEntry entry) {
            return sendRequest(new AppendEntriesRequest(term, ProcessId.of("old-leader"), 0, 0, List.of(entry), 0),
                    follower, RaftMessageTypes.APPEND_ENTRIES_REQUEST);
        }

        @Override
        protected Map<MessageType, Handler> initialiseHandlers() {
            return Map.of(RaftMessageTypes.APPEND_ENTRIES_RESPONSE, this::handleResponse);
        }

        private void handleResponse(Message message) {
            handleResponse(message.correlationId(), deserialize(message.payload(), AppendEntriesResponse.class), message.source());
        }
    }

    @Test
    @DisplayName("A new leader commits an entry left over from an earlier term without any client write")
    void anEntryFromAnEarlierTermCommitsWithoutAClientWrite() throws IOException {
        // Long enough for the test to place the old entry on every server before any of them starts an election.
        try (var cluster = Cluster.createSimulated(ALL,
                (peerIds, processParams) -> new RaftServer(peerIds, processParams, 300))) {
            var oldLeader = cluster.newClient(ProcessId.of("old-leader"), OldLeaderClient::new);
            var leftOver = new LogEntry(1, 2, new SetValueOperation("k", "left-over"));
            for (ProcessId server : ALL) {
                assertTrue(cluster.tickUntilComplete(oldLeader.replicate(server, 2, leftOver)).success());
            }
            ALL.forEach(id -> assertEquals(0, ((RaftServer) cluster.getNode(id)).getCommitIndex()));

            cluster.tickUntil(() -> ALL.stream().allMatch(
                    id -> "left-over".equals(((RaftServer) cluster.getNode(id)).getValue("k"))));

            ProcessId leader = ALL.stream().filter(id -> ((RaftServer) cluster.getNode(id)).isLeader())
                    .findFirst().orElseThrow();
            RaftServer leaderServer = cluster.getNode(leader);
            assertEquals(3, leaderServer.getCurrentTerm());
            assertEquals(3, leaderServer.getLogEntry(2).term(), "the no-op is in the leader's own term");
            assertTrue(leaderServer.getCommitIndex() >= 2, "the no-op is committed, and the old entry with it");
        }
    }
}
