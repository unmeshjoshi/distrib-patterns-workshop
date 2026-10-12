package com.distribpatterns.raft;

import com.distribpatterns.raft.messages.AppendEntriesRequest;
import com.distribpatterns.raft.messages.AppendEntriesResponse;
import com.distribpatterns.raft.messages.RaftMessageTypes;
import com.distribpatterns.raft.messages.RequestVoteRequest;
import com.distribpatterns.raft.messages.RequestVoteResponse;
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
 * The current term, the vote and the log go into one write-ahead log, and a server replies only once the records
 * behind the reply are durable. A restart brings back what the server has told anyone, and nothing it has not.
 *
 * TODO: a leader counts its own log towards a majority only as far as it is durable. Test that a leader whose own
 *       entry is not yet durable does not commit it, even when both followers have already acknowledged it.
 * TODO: a follower that truncates a conflicting suffix and appends the leader's entries gets exactly that log back
 *       after a restart (an ENTRY record at index i discards everything from i on when replayed).
 */
public class RaftDurabilityTest {
    private static final ProcessId ATHENS = ProcessId.of("athens");
    private static final ProcessId BYZANTIUM = ProcessId.of("byzantium");
    private static final ProcessId CYRENE = ProcessId.of("cyrene");
    private static final ProcessId ALICE = ProcessId.of("alice");
    private static final List<ProcessId> ALL = List.of(ATHENS, BYZANTIUM, CYRENE);

    private static final ProcessId CANDIDATE_X = ProcessId.of("candidate-x");
    private static final ProcessId CANDIDATE_Y = ProcessId.of("candidate-y");

    private static final int ELECTION_TIMEOUT_TICKS = 20;
    // Long enough that no server starts an election while a test sends votes by hand.
    private static final int NO_ELECTIONS = 1_000_000;

    /** Sends an AppendEntries straight to one server, so a test plays the leader. */
    static class LeaderClient extends ClusterClient {
        LeaderClient(List<ProcessId> replicaEndpoints, ProcessParams processParams) {
            super(replicaEndpoints, processParams);
        }

        TickCompletableFuture<AppendEntriesResponse> appendEntries(ProcessId follower, int term, int prevLogIndex,
                                                                    int prevLogTerm, List<LogEntry> entries) {
            return sendRequest(new AppendEntriesRequest(term, CANDIDATE_X, prevLogIndex, prevLogTerm, entries, 0),
                    follower, RaftMessageTypes.APPEND_ENTRIES_REQUEST);
        }

        @Override
        protected Map<MessageType, Handler> initialiseHandlers() {
            return Map.of(RaftMessageTypes.APPEND_ENTRIES_RESPONSE, this::handleAppendEntriesResponse);
        }

        private void handleAppendEntriesResponse(Message message) {
            handleResponse(message.correlationId(), deserialize(message.payload(), AppendEntriesResponse.class), message.source());
        }
    }

    /** Sends a RequestVote straight to one server, so a test controls the candidate and the term. */
    static class CandidateClient extends ClusterClient {
        CandidateClient(List<ProcessId> replicaEndpoints, ProcessParams processParams) {
            super(replicaEndpoints, processParams);
        }

        TickCompletableFuture<RequestVoteResponse> requestVote(ProcessId voter, ProcessId candidate, int term) {
            return sendRequest(new RequestVoteRequest(term, candidate, 0, 0), voter, RaftMessageTypes.REQUEST_VOTE_REQUEST);
        }

        @Override
        protected Map<MessageType, Handler> initialiseHandlers() {
            return Map.of(RaftMessageTypes.REQUEST_VOTE_RESPONSE, this::handleVoteResponse);
        }

        private void handleVoteResponse(Message message) {
            handleResponse(message.correlationId(), deserialize(message.payload(), RequestVoteResponse.class), message.source());
        }
    }

    private static Cluster newCluster(int electionTimeoutTicks) throws IOException {
        return Cluster.createSimulated(ALL,
                (peerIds, processParams) -> new RaftServer(peerIds, processParams, electionTimeoutTicks));
    }

    private static RaftServer server(Cluster cluster, ProcessId id) {
        return cluster.getNode(id);
    }

    private static void crashAndRestart(Cluster cluster, ProcessId id) {
        cluster.crashNode(id);
        cluster.restartNode(id);
        cluster.tickUntil(() -> server(cluster, id).isInitialised());
    }

    @Test
    @DisplayName("A follower gets its term, vote and log back after a restart, and its state machine from the leader")
    void aFollowerRecoversTermVoteAndLog() throws IOException {
        try (var cluster = newCluster(ELECTION_TIMEOUT_TICKS)) {
            cluster.tickUntil(() -> ALL.stream().filter(id -> server(cluster, id).isLeader()).count() == 1);
            ProcessId leader = ALL.stream().filter(id -> server(cluster, id).isLeader()).findFirst().orElseThrow();
            var client = cluster.newClientConnectedTo(ALICE, leader, RaftClient::new);
            assertTrue(cluster.tickUntilComplete(client.execute(leader, new SetValueOperation("title", "Raft"))).success());
            ProcessId follower = ALL.stream().filter(id -> !id.equals(leader)).findFirst().orElseThrow();
            cluster.tickUntil(() -> "Raft".equals(server(cluster, follower).getValue("title"))
                    && !server(cluster, follower).hasRecordsNotYetDurable());
            int termBefore = server(cluster, follower).getCurrentTerm();
            ProcessId voteBefore = server(cluster, follower).getVotedFor();
            int logSizeBefore = server(cluster, follower).getLogSize();
            assertEquals(2, logSizeBefore, "the placeholder at index 0 and the one entry");

            crashAndRestart(cluster, follower);

            RaftServer restarted = server(cluster, follower);
            assertEquals(termBefore, restarted.getCurrentTerm());
            assertEquals(voteBefore, restarted.getVotedFor());
            assertEquals(logSizeBefore, restarted.getLogSize());
            assertNull(restarted.getValue("title"), "the commit index is not logged, so nothing is applied yet");
            cluster.tickUntil(() -> "Raft".equals(restarted.getValue("title")));
        }
    }

    @Test
    @DisplayName("A follower reports a match only as far as the request checked, not up to its whole log")
    void aFollowerDoesNotReportAMatchBeyondWhatTheRequestChecked() throws IOException {
        try (var cluster = newCluster(NO_ELECTIONS)) {
            var leader = cluster.newClient(ProcessId.of("leader"), LeaderClient::new);
            // An old leader of term 2 put entries 1 and 2 on the follower.
            var stale = List.of(new LogEntry(1, 2, new SetValueOperation("k", "a")),
                    new LogEntry(2, 2, new SetValueOperation("k", "b")));
            AppendEntriesResponse first = cluster.tickUntilComplete(leader.appendEntries(BYZANTIUM, 2, 0, 0, stale));
            assertTrue(first.success());
            assertEquals(2, first.matchIndex());

            // The leader of term 3 has not sent anything beyond index 1, so it only knows index 1 matches.
            AppendEntriesResponse heartbeat = cluster.tickUntilComplete(
                    leader.appendEntries(BYZANTIUM, 3, 1, 2, List.of()));

            assertTrue(heartbeat.success());
            assertEquals(1, heartbeat.matchIndex(), "entry 2 is a stale suffix the leader has never compared");
        }
    }

    @Test
    @DisplayName("A vote that was granted is kept after a restart, so the server cannot vote twice in a term")
    void aVoteSurvivesACrash() throws IOException {
        try (var cluster = newCluster(NO_ELECTIONS)) {
            var candidates = cluster.newClient(ProcessId.of("candidates"), CandidateClient::new);
            assertTrue(cluster.tickUntilComplete(candidates.requestVote(BYZANTIUM, CANDIDATE_X, 5)).voteGranted());

            crashAndRestart(cluster, BYZANTIUM);

            assertEquals(5, server(cluster, BYZANTIUM).getCurrentTerm());
            assertEquals(CANDIDATE_X, server(cluster, BYZANTIUM).getVotedFor());
            assertFalse(cluster.tickUntilComplete(candidates.requestVote(BYZANTIUM, CANDIDATE_Y, 5)).voteGranted(),
                    "the vote in term 5 is already given");
            assertTrue(cluster.tickUntilComplete(candidates.requestVote(BYZANTIUM, CANDIDATE_X, 5)).voteGranted(),
                    "asking again for the same candidate is fine");
            assertTrue(cluster.tickUntilComplete(candidates.requestVote(BYZANTIUM, CANDIDATE_Y, 6)).voteGranted(),
                    "a later term is a new vote");
        }
    }

    @Test
    @DisplayName("No vote is answered before it is durable")
    void noVoteIsAnsweredBeforeItIsDurable() throws IOException {
        try (var cluster = newCluster(NO_ELECTIONS)) {
            var candidates = cluster.newClient(ProcessId.of("candidates"), CandidateClient::new);
            TickCompletableFuture<RequestVoteResponse> vote = candidates.requestVote(BYZANTIUM, CANDIDATE_X, 5);

            cluster.tickUntil(() -> server(cluster, BYZANTIUM).hasRecordsNotYetDurable());
            assertTrue(vote.isPending(), "no reply while the vote can still be lost");

            crashAndRestart(cluster, BYZANTIUM);
            for (int i = 0; i < 100; i++) cluster.tick();

            assertTrue(vote.isPending(), "the vote was never answered");
            assertEquals(0, server(cluster, BYZANTIUM).getCurrentTerm(), "and nobody was told about it");
            assertNull(server(cluster, BYZANTIUM).getVotedFor());
        }
    }
}
