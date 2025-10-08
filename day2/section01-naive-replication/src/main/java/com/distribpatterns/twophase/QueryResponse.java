package com.distribpatterns.twophase;

/**
 * Phase 0 (Query/CanCommit) response.
 * Participant responds with any pending accepted request that hasn't been committed yet.
 * 
 * If pendingRequestId is null, this node has no pending requests.
 * If pendingRequestId is non-null, this node has an accepted-but-uncommitted request.
 */
public record QueryResponse(String queryId, String pendingRequestId, Operation pendingOperation) {
    
    /**
     * Factory method for "no pending request" response
     */
    public static QueryResponse noPendingRequest(String queryId) {
        return new QueryResponse(queryId, null, null);
    }
    
    /**
     * Factory method for "has pending request" response
     */
    public static QueryResponse withPendingRequest(String queryId, String requestId, Operation operation) {
        return new QueryResponse(queryId, requestId, operation);
    }
    
    public boolean hasPendingRequest() {
        return pendingRequestId != null;
    }
}

