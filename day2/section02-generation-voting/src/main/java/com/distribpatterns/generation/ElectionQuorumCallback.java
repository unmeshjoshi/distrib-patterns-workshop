package com.distribpatterns.generation;

import com.distribpatterns.generation.messages.PrepareResponse;
import com.tickloom.ProcessId;
import com.tickloom.future.ListenableFuture;
import com.tickloom.messaging.RequestCallback;

import java.util.HashMap;
import java.util.Map;

/**
 * Quorum callback for election voting that detects quorum impossibility early.
 *
 * Unlike a generic quorum callback that waits for all responses (or a timeout),
 * this callback fails as soon as enough rejections arrive to make quorum impossible.
 * This avoids waiting for partitioned/slow nodes to time out before retrying.
 */
class ElectionQuorumCallback implements RequestCallback<PrepareResponse> {

    private final int totalNodes;
    private final int quorum;
    private final long proposedGeneration;
    private final Map<ProcessId, PrepareResponse> responses = new HashMap<>();
    private final ListenableFuture<Map<ProcessId, PrepareResponse>> quorumFuture = new ListenableFuture<>();

    private int promises = 0;
    private int rejections = 0;
    private boolean completed = false;

    ElectionQuorumCallback(int totalNodes, long proposedGeneration) {
        this.totalNodes = totalNodes;
        this.quorum = totalNodes / 2 + 1;
        this.proposedGeneration = proposedGeneration;
    }

    @Override
    public void onResponse(PrepareResponse response, ProcessId fromNode) {
        if (completed) return;

        responses.put(fromNode, response);

        if (response.promised() && response.currentGeneration() == proposedGeneration) {
            promises++;
        } else {
            rejections++;
        }

        if (promises >= quorum) {
            completed = true;
            quorumFuture.complete(responses);
        } else if (rejections > totalNodes - quorum) {
            // Too many rejections — quorum is impossible
            completed = true;
            quorumFuture.fail(new RuntimeException(
                    "Quorum impossible: " + rejections + " rejections, need " + quorum + " promises"));
        }
    }

    @Override
    public void onError(Exception error) {
        if (completed) return;
        // Treat errors (e.g. timeouts) as rejections
        rejections++;
        if (rejections > totalNodes - quorum) {
            completed = true;
            quorumFuture.fail(error);
        }
    }

    public ListenableFuture<Map<ProcessId, PrepareResponse>> getQuorumFuture() {
        return quorumFuture;
    }
}
