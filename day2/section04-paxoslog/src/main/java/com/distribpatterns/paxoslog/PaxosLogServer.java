package com.distribpatterns.paxoslog;

import com.distribpatterns.paxoslog.messages.*;
import com.distribpatterns.wal.LogStore;
import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.Replica;
import com.tickloom.future.ListenableFuture;
import com.tickloom.messaging.AsyncQuorumCallback;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageType;
import com.tickloom.messaging.RequestCallback;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * PaxosLog: Replicated log where each entry is decided via Paxos consensus.
 * 
 * Key features:
 * - Each log index runs independent Paxos instance
 * - Sequential execution: entries must be committed in order (no gaps)
 * - Automatic conflict resolution: retry at next index if value rejected
 * - State machine: applies commands (SetValue, CompareAndSwap) to KV store
 */
public class PaxosLogServer extends Replica {
    // LogStore for persistent storage of Paxos log entries
    private static final String PAXOS_LOG_ID = "paxos";
    private LogStore logStore;
    
    // In-memory cache: one PaxosState per log index (for fast access)
    // This is rebuilt from LogStore on startup
    private final Map<Integer, PaxosState> paxosLogCache = new TreeMap<>();
    private final Map<Integer, ListenableFuture<Boolean>> pendingPersistsByIndex = new HashMap<>();
    private final Map<Integer, ExecuteCommandResponse> executedResponsesByIndex = new HashMap<>();
    private final Set<Integer> durablyCommittedIndices = new HashSet<>();
    private final Map<Integer, ClientCommitContext> pendingClientCommitsByIndex = new HashMap<>();

    // State machine: key-value store
    private final Map<String, String> kvStore = new HashMap<>();
    
    // Track highest committed index (high water mark)
    private int highWaterMark = -1;
    
    // Generation counter for Paxos rounds
    private final AtomicInteger generationCounter = new AtomicInteger(0);
    
    // Server ID for breaking ties (if needed)
    private final int serverId;
    
    // No-op operation for reads
    private static final NoOpOperation NO_OP = new NoOpOperation();
    
    private record ClientCommitContext(String clientKey, Operation agreedValue, Operation initialOperation) {}
    
    public PaxosLogServer(List<ProcessId> allNodes, ProcessParams processParams) {
        super(allNodes, processParams);
        // Simple server ID: hash of process ID
        this.serverId = id.toString().hashCode();
        // LogStore will be initialized in onInit() after storage is available
    }
    
    @Override
    protected ListenableFuture<?> onInit() {
        // Initialize LogStore now that storage is available
        if (logStore == null) {
            logStore = new LogStore(List.of(PAXOS_LOG_ID), storage);
        }
        
        // LogStore initialization is async - it queries storage for last indices
        // We'll attempt to load entries, but if LogStore isn't ready yet, 
        // we'll load them lazily when first accessed
        ListenableFuture<Void> initFuture = new ListenableFuture<>();
        
        // Try to load entries if LogStore is already initialized
        // Otherwise, entries will be loaded on-demand
        if (logStore.isInitialised()) {
            loadLogEntriesFromStorage(initFuture);
        } else {
            // LogStore is still initializing - allow the process to come up and
            // load entries lazily on first access.
            // Entries will be loaded on first access
            initFuture.complete(null);
        }
        
        return initFuture;
    }
    
    private void loadLogEntriesFromStorage(ListenableFuture<Void> initFuture) {
        if (logStore == null) {
            completeInitialization(initFuture);
            return;
        }
        
        ListenableFuture<byte[]> lastKeyFuture = storage.lowerKey(
            logStore.createLogKey(PAXOS_LOG_ID, Long.MAX_VALUE));
        
        lastKeyFuture.andThen((lastKey, error) -> {
            if (error != null) {
                completeInitializationWithError(initFuture, "Error finding last log index", error);
                return;
            }
            
            if (lastKey == null) {
                System.out.println(id + ": No existing log entries found");
                completeInitialization(initFuture);
                return;
            }
            
            loadLogRange(initFuture, lastKey);
        });
    }

    private void loadLogRange(ListenableFuture<Void> initFuture, byte[] lastKey) {
        long lastIndex = logStore.getIndex(lastKey);
        System.out.println(id + ": Loading log entries from storage, last index: " + lastIndex);

        byte[] startKey = logStore.createLogKey(PAXOS_LOG_ID, 0L);
        byte[] endKey = logStore.createLogKey(PAXOS_LOG_ID, lastIndex + 1L);

        ListenableFuture<Map<byte[], byte[]>> rangeFuture = storage.readRange(startKey, endKey);
        rangeFuture.andThen((entries, rangeError) -> {
            if (rangeError != null) {
                completeInitializationWithError(initFuture, "Error reading log entries", rangeError);
                return;
            }

            cacheLoadedEntries(entries);
            System.out.println(id + ": Loaded " + entries.size() + " log entries, highWaterMark: " + highWaterMark);
            replayCommittedEntries();
            completeInitialization(initFuture);
        });
    }

    private void cacheLoadedEntries(Map<byte[], byte[]> entries) {
        for (Map.Entry<byte[], byte[]> entry : entries.entrySet()) {
            long index = logStore.getIndex(entry.getKey());
            PaxosState state = deserializePaxosState(entry.getValue());
            paxosLogCache.put((int) index, state);

            if (state.committedValue().isPresent()) {
                highWaterMark = Math.max(highWaterMark, (int) index);
            }
        }
    }

    private void completeInitializationWithError(ListenableFuture<Void> initFuture, String context, Throwable error) {
        System.err.println(id + ": " + context + ": " + error.getMessage());
        completeInitialization(initFuture);
    }

    private void completeInitialization(ListenableFuture<Void> initFuture) {
        if (initFuture != null) {
            initFuture.complete(null);
        }
    }
    
    private void replayCommittedEntries() {
        // Execute all committed entries in order
        for (int index = 0; index <= highWaterMark; index++) {
            PaxosState state = paxosLogCache.get(index);
            if (state != null && state.committedValue().isPresent()) {
                executeLogEntry(index, state.committedValue().get());
            }
        }
    }
    
    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
            // Client handlers
            PaxosLogMessageTypes.CLIENT_EXECUTE_REQUEST, this::handleClientExecuteRequest,
            PaxosLogMessageTypes.CLIENT_GET_REQUEST, this::handleClientGetRequest,
            
            // Paxos Phase 1: Prepare/Promise
            PaxosLogMessageTypes.PREPARE_REQUEST, this::handlePrepareRequest,
            PaxosLogMessageTypes.PREPARE_RESPONSE, this::handlePrepareResponse,
            
            // Paxos Phase 2: Propose/Accept
            PaxosLogMessageTypes.PROPOSE_REQUEST, this::handleProposeRequest,
            PaxosLogMessageTypes.PROPOSE_RESPONSE, this::handleProposeResponse,
            
            // Paxos Phase 3: Commit/Learn
            PaxosLogMessageTypes.COMMIT_REQUEST, this::handleCommitRequest,
            PaxosLogMessageTypes.COMMIT_RESPONSE, this::handleCommitResponse
        );
    }
    
    // ========== CLIENT REQUEST HANDLING ==========
    
    private void handleClientExecuteRequest(Message clientMessage) {
        ExecuteCommandRequest request = deserializePayload(clientMessage.payload(), ExecuteCommandRequest.class);
        String clientKey = clientKeyFor(clientMessage);
        
        System.out.println(id + ": Received execute request for command: " + request.operation());

        registerExecuteResponseCallback(clientMessage, clientKey);
        appendToLog(0, request.operation(), clientKey);
    }
    
    private void handleClientGetRequest(Message clientMessage) {
        GetValueRequest request = deserializePayload(clientMessage.payload(), GetValueRequest.class);
        String clientKey = clientKeyFor(clientMessage);
        
        System.out.println(id + ": Received get request for key: " + request.key());

        registerGetResponseCallback(clientMessage, request.key(), clientKey);
        appendToLog(0, NO_OP, clientKey);
    }

    private String clientKeyFor(Message clientMessage) {
        return "client_" + clientMessage.correlationId();
    }

    private void registerExecuteResponseCallback(Message clientMessage, String clientKey) {
        waitingList.add(clientKey, new RequestCallback<>() {
            @Override
            public void onResponse(Object response, ProcessId fromNode) {
                send(createClientResponse(
                        clientMessage,
                        response,
                        PaxosLogMessageTypes.CLIENT_EXECUTE_RESPONSE
                ));
            }

            @Override
            public void onError(Exception error) {
                send(createClientResponse(
                        clientMessage,
                        new ExecuteCommandResponse(false, null),
                        PaxosLogMessageTypes.CLIENT_EXECUTE_RESPONSE
                ));
            }
        });
    }

    private void registerGetResponseCallback(Message clientMessage, String key, String clientKey) {
        waitingList.add(clientKey, new RequestCallback<>() {
            @Override
            public void onResponse(Object response, ProcessId fromNode) {
                String value = kvStore.get(key);
                send(createClientResponse(
                        clientMessage,
                        new GetValueResponse(value),
                        PaxosLogMessageTypes.CLIENT_GET_RESPONSE
                ));
            }

            @Override
            public void onError(Exception error) {
                send(createClientResponse(
                        clientMessage,
                        new GetValueResponse(null),
                        PaxosLogMessageTypes.CLIENT_GET_RESPONSE
                ));
            }
        });
    }

    private Message createClientResponse(Message clientMessage, Object payload, MessageType messageType) {
        return createMessage(clientMessage.source(), clientMessage.correlationId(), payload, messageType);
    }
    
    // ========== LOG APPEND LOGIC ==========
    
    /**
     * Append operation to log at given index.
     * If consensus fails (value rejected), retry at next index.
     */
    private void appendToLog(int startIndex, Operation operation, String clientKey) {
        int generation = generationCounter.incrementAndGet();
        System.out.println(id + ": Attempting to append " + operation + " at index " + startIndex + 
                         " with generation " + generation);
        
        runPaxosForIndex(startIndex, generation, operation, clientKey, 0);
    }
    
    private void runPaxosForIndex(int logIndex, int generation, Operation operation, String clientKey, int attempt) {
        if (attempt >= 3) {
            System.out.println(id + ": Max attempts reached for " + operation);
            waitingList.handleResponse(clientKey, new ExecuteCommandResponse(false, null), id);
            return;
        }
        
        startPreparePhase(logIndex, generation, operation, clientKey, attempt);
    }
    
    // ========== PHASE 1: PREPARE/PROMISE ==========
    
    private void startPreparePhase(int logIndex, int generation, Operation operation, String clientKey, int attempt) {
        System.out.println(id + ": Phase 1 - PREPARE for index " + logIndex + ", generation " + generation);
        
        var quorumCallback = new AsyncQuorumCallback<PrepareResponse>(
            getAllNodes().size(),
            response -> response != null && response.promised()
        );
        
        quorumCallback
            .onSuccess(responses -> {
                System.out.println(id + ": PREPARE quorum reached for index " + logIndex);
                startProposePhase(logIndex, generation, operation, responses, clientKey, attempt);
            })
            .onFailure(error -> {
                System.out.println(id + ": PREPARE quorum failed for index " + logIndex + ": " + error.getMessage());
                // Retry with higher generation
                int newGeneration = generationCounter.incrementAndGet();
                runPaxosForIndex(logIndex, newGeneration, operation, clientKey, attempt + 1);
            });
        
        PrepareRequest prepareReq = new PrepareRequest(logIndex, generation);
        broadcastToAllReplicas(quorumCallback, (node, correlationId) ->
            createMessage(node, correlationId, prepareReq, PaxosLogMessageTypes.PREPARE_REQUEST)
        );
    }
    
    private void handlePrepareRequest(Message message) {
        PrepareRequest request = deserializePayload(message.payload(), PrepareRequest.class);
        int logIndex = request.logIndex();
        int generation = request.generation();
        
        var paxosState = getOrCreatePaxosState(logIndex);
        
        if (paxosState.canPromise(generation)) {
            // Promise this generation
            var newState = paxosState.promise(generation);
            persistPaxosState(logIndex, newState);
            
            PrepareResponse response = new PrepareResponse(
                true,
                newState.acceptedGeneration().orElse(null),
                newState.acceptedValue().orElse(null)
            );
            
            System.out.println(id + ": PROMISED generation " + generation + " for index " + logIndex);
            send(createMessage(message.source(), message.correlationId(), response,
                PaxosLogMessageTypes.PREPARE_RESPONSE));
        } else {
            // Reject
            PrepareResponse response = new PrepareResponse(
                false,
                paxosState.acceptedGeneration().orElse(null),
                paxosState.acceptedValue().orElse(null)
            );
            
            System.out.println(id + ": REJECTED generation " + generation + " for index " + logIndex);
            send(createMessage(message.source(), message.correlationId(), response,
                PaxosLogMessageTypes.PREPARE_RESPONSE));
        }
    }
    
    private void handlePrepareResponse(Message message) {
        PrepareResponse response = deserializePayload(message.payload(), PrepareResponse.class);
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }
    
    // ========== PHASE 2: PROPOSE/ACCEPT ==========
    
    private void startProposePhase(int logIndex, int generation, Operation initialOperation,
                                   Map<ProcessId, PrepareResponse> promises, String clientKey, int attempt) {
        // Select value to propose: highest accepted or initial command
        Operation valueToPropose = selectValueToPropose(initialOperation, promises);
        
        System.out.println(id + ": Phase 2 - PROPOSE " + valueToPropose + " for index " + logIndex);
        
        var quorumCallback = new AsyncQuorumCallback<ProposeResponse>(
            getAllNodes().size(),
            response -> response != null && response.accepted()
        );
        
        quorumCallback
            .onSuccess(responses -> {
                System.out.println(id + ": PROPOSE quorum reached for index " + logIndex);
                startCommitPhase(logIndex, generation, valueToPropose, initialOperation, clientKey);
            })
            .onFailure(error -> {
                System.out.println(id + ": PROPOSE quorum failed for index " + logIndex);
                // Retry with higher generation
                int newGeneration = generationCounter.incrementAndGet();
                runPaxosForIndex(logIndex, newGeneration, initialOperation, clientKey, attempt + 1);
            });
        
        ProposeRequest proposeReq = new ProposeRequest(logIndex, generation, valueToPropose);
        broadcastToAllReplicas(quorumCallback, (node, correlationId) ->
            createMessage(node, correlationId, proposeReq, PaxosLogMessageTypes.PROPOSE_REQUEST)
        );
    }
    
    private Operation selectValueToPropose(Operation initialOperation, Map<ProcessId, PrepareResponse> promises) {
        Optional<PrepareResponse> highestAccepted = promises.values().stream()
            .filter(p -> p.acceptedGeneration() != null)
            .max((p1, p2) -> Integer.compare(p1.acceptedGeneration(), p2.acceptedGeneration()));
        
        if (highestAccepted.isPresent() && highestAccepted.get().acceptedValue() != null) {
            Operation recoveredValue = highestAccepted.get().acceptedValue();
            System.out.println(id + ": Found previously accepted value: " + recoveredValue);
            return recoveredValue;
        } else {
            return initialOperation;
        }
    }
    
    private void handleProposeRequest(Message message) {
        ProposeRequest request = deserializePayload(message.payload(), ProposeRequest.class);
        int logIndex = request.logIndex();
        int generation = request.generation();
        Operation value = request.value();
        
        var paxosState = getOrCreatePaxosState(logIndex);
        
        if (paxosState.canAccept(generation)) {
            // Accept the proposal
            var newState = paxosState.accept(generation, value);
            persistPaxosState(logIndex, newState);
            
            System.out.println(id + ": ACCEPTED proposal for index " + logIndex);
            send(createMessage(message.source(), message.correlationId(), 
                new ProposeResponse(logIndex, true), PaxosLogMessageTypes.PROPOSE_RESPONSE));
        } else {
            System.out.println(id + ": REJECTED proposal for index " + logIndex);
            send(createMessage(message.source(), message.correlationId(), 
                new ProposeResponse(logIndex, false), PaxosLogMessageTypes.PROPOSE_RESPONSE));
        }
    }
    
    private void handleProposeResponse(Message message) {
        ProposeResponse response = deserializePayload(message.payload(), ProposeResponse.class);
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }
    
    // ========== PHASE 3: COMMIT/LEARN ==========
    
    private void startCommitPhase(int logIndex, int generation, Operation agreedValue, 
                                  Operation initialOperation, String clientKey) {
        System.out.println(id + ": Phase 3 - COMMIT " + agreedValue + " for index " + logIndex);

        rememberClientWaitingForCommittedIndex(logIndex, agreedValue, initialOperation, clientKey);

        var quorumCallback = commitQuorumCallbackFor(logIndex, agreedValue, initialOperation, clientKey);
        broadcastCommitFor(logIndex, generation, agreedValue, quorumCallback);
    }
    
    private void handleCommitRequest(Message message) {
        CommitRequest request = deserializePayload(message.payload(), CommitRequest.class);
        int logIndex = request.logIndex();
        int generation = request.generation();
        Operation value = request.value();
        
        System.out.println(id + ": Received COMMIT for index " + logIndex + ", value " + value);
        
        var paxosState = getOrCreatePaxosState(logIndex);
        
        if (paxosState.canAccept(generation)) {
            // Commit the value
            var newState = paxosState.commit(generation, value);
            persistPaxosState(logIndex, newState).handle((success, error) -> {
                if (error != null || !Boolean.TRUE.equals(success)) {
                    send(createMessage(message.source(), message.correlationId(),
                        new CommitResponse(false), PaxosLogMessageTypes.COMMIT_RESPONSE));
                    return;
                }

                System.out.println(id + ": COMMITTED index " + logIndex);

                // Try to execute this entry and any subsequent committed entries
                tryExecuteLogEntries();

                send(createMessage(message.source(), message.correlationId(),
                    new CommitResponse(true), PaxosLogMessageTypes.COMMIT_RESPONSE));
            });
        } else {
            System.out.println(id + ": REJECTED commit for index " + logIndex);
            send(createMessage(message.source(), message.correlationId(),
                new CommitResponse(false), PaxosLogMessageTypes.COMMIT_RESPONSE));
        }
    }
    
    private void handleCommitResponse(Message message) {
        CommitResponse response = deserializePayload(message.payload(), CommitResponse.class);
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }

    private AsyncQuorumCallback<CommitResponse> commitQuorumCallbackFor(int logIndex,
                                                                        Operation agreedValue,
                                                                        Operation initialOperation,
                                                                        String clientKey) {
        var quorumCallback = new AsyncQuorumCallback<CommitResponse>(
            getAllNodes().size(),
            response -> response != null && response.success()
        );

        quorumCallback
            .onSuccess(responses -> handleCommitQuorumReached(logIndex, agreedValue, initialOperation, clientKey))
            .onFailure(error -> handleCommitQuorumFailed(logIndex, clientKey));

        return quorumCallback;
    }

    private void handleCommitQuorumReached(int logIndex,
                                           Operation agreedValue,
                                           Operation initialOperation,
                                           String clientKey) {
        System.out.println(id + ": COMMIT quorum reached for index " + logIndex);
        durablyCommittedIndices.add(logIndex);

        if (agreedValue.equals(initialOperation)) {
            respondToClientIfCommitIsDurable(logIndex);
            return;
        }

        clearClientCommitTracking(logIndex);
        System.out.println(id + ": Value mismatch at index " + logIndex + ", retrying at next index");
        appendToLog(logIndex + 1, initialOperation, clientKey);
    }

    private void handleCommitQuorumFailed(int logIndex, String clientKey) {
        System.out.println(id + ": COMMIT quorum failed for index " + logIndex);
        clearClientCommitTracking(logIndex);
        waitingList.handleResponse(clientKey, new ExecuteCommandResponse(false, null), id);
    }

    private void rememberClientWaitingForCommittedIndex(int logIndex,
                                                        Operation agreedValue,
                                                        Operation initialOperation,
                                                        String clientKey) {
        pendingClientCommitsByIndex.put(logIndex, new ClientCommitContext(clientKey, agreedValue, initialOperation));
    }

    private void broadcastCommitFor(int logIndex,
                                    int generation,
                                    Operation agreedValue,
                                    AsyncQuorumCallback<CommitResponse> quorumCallback) {
        CommitRequest commitReq = new CommitRequest(logIndex, generation, agreedValue);
        broadcastToAllReplicas(quorumCallback, (node, correlationId) ->
            createMessage(node, correlationId, commitReq, PaxosLogMessageTypes.COMMIT_REQUEST)
        );
    }

    private void respondToClientIfCommitIsDurable(int logIndex) {
        ClientCommitContext clientCommitContext = pendingClientCommitsByIndex.get(logIndex);
        ExecuteCommandResponse executedResponse = executedResponsesByIndex.get(logIndex);

        if (clientCommitContext == null || executedResponse == null || !durablyCommittedIndices.contains(logIndex)) {
            return;
        }

        waitingList.handleResponse(clientCommitContext.clientKey(), executedResponse, id);
        clearClientCommitTracking(logIndex);
    }

    private void clearClientCommitTracking(int logIndex) {
        pendingClientCommitsByIndex.remove(logIndex);
        durablyCommittedIndices.remove(logIndex);
        executedResponsesByIndex.remove(logIndex);
    }
    
    // ========== LOG EXECUTION (STATE MACHINE) ==========
    
    /**
     * Try to execute committed log entries in order (no gaps).
     */
    private void tryExecuteLogEntries() {
        // Execute all consecutive committed entries starting from highWaterMark + 1
        for (int index = highWaterMark + 1; ; index++) {
            PaxosState state = paxosLogCache.get(index);
            if (state == null || state.committedValue().isEmpty()) {
                // Gap found or no more entries
                break;
            }
            
            // Execute this entry
            Operation operation = state.committedValue().get();
            executeLogEntry(index, operation);
            highWaterMark = index;
        }
    }
    
    private void executeLogEntry(int logIndex, Operation operation) {
        System.out.println(id + ": Executing log entry " + logIndex + ": " + operation);
        
        String result = null;
        boolean success = true;
        
        if (operation instanceof SetValueOperation set) {
            kvStore.put(set.key(), set.value());
            result = set.value();
            System.out.println(id + ": Executed SET " + set.key() + "=" + set.value());
            
        } else if (operation instanceof CompareAndSwapOperation cas) {
            String existingValue = kvStore.get(cas.key());
            boolean matches = Objects.equals(existingValue, cas.expectedValue());
            
            if (matches) {
                kvStore.put(cas.key(), cas.newValue());
                success = true;
            } else {
                success = false;
            }
            result = existingValue;
            
            System.out.println(id + ": Executed CAS " + cas.key() + 
                             " (expected=" + cas.expectedValue() + ", actual=" + existingValue + 
                             ", success=" + success + ")");
            
        } else if (operation instanceof NoOpOperation) {
            // No-op: just ensure we see committed state
            System.out.println(id + ": Executed NO-OP");
        }
        
        executedResponsesByIndex.put(logIndex, new ExecuteCommandResponse(success, result));
        respondToClientIfCommitIsDurable(logIndex);
    }
    
    // ========== UTILITY METHODS ==========
    
    private PaxosState getOrCreatePaxosState(int logIndex) {
        // Check cache first
        PaxosState cached = paxosLogCache.get(logIndex);
        if (cached != null) {
            return cached;
        }
        
        // Try to load from storage if not in cache
        if (isInitialised() && logStore != null) {
            loadPaxosStateFromStorage(logIndex);
        }
        
        // Return new state if not found
        return new PaxosState();
    }
    
    private void loadPaxosStateFromStorage(int logIndex) {
        if (logStore == null || !isInitialised()) {
            return;
        }
        byte[] key = logStore.createLogKey(PAXOS_LOG_ID, logIndex);
        ListenableFuture<byte[]> future = storage.get(key);
        future.andThen((data, error) -> {
            if (error == null && data != null) {
                PaxosState state = deserializePaxosState(data);
                paxosLogCache.put(logIndex, state);
            }
        });
    }
    
    private ListenableFuture<Boolean> persistPaxosState(int logIndex, PaxosState state) {
        // Update cache
        paxosLogCache.put(logIndex, state);
        
        // Persist to storage using the log key (not append, since we update at specific index)
        if (!isInitialised() || logStore == null) {
            // Don't persist until initialized
            ListenableFuture<Boolean> future = new ListenableFuture<>();
            future.complete(true);
            return future;
        }
        
        byte[] stateBytes = serializePaxosState(state);
        byte[] logKey = logStore.createLogKey(PAXOS_LOG_ID, logIndex);
        ListenableFuture<Boolean> persistFuture = new ListenableFuture<>();
        ListenableFuture<Boolean> previousPersist = pendingPersistsByIndex.put(logIndex, persistFuture);

        if (previousPersist == null) {
            startPersist(logIndex, logKey, stateBytes, persistFuture);
            return persistFuture;
        }

        previousPersist.handle((ignored, error) ->
            startPersist(logIndex, logKey, stateBytes, persistFuture));
        return persistFuture;
    }

    private void startPersist(int logIndex, byte[] logKey, byte[] stateBytes, ListenableFuture<Boolean> persistFuture) {
        storage.put(logKey, stateBytes, com.tickloom.storage.Storage.WriteOptions.SYNC)
            .handle((success, error) -> {
                if (error != null || !Boolean.TRUE.equals(success)) {
                    System.err.println(id + ": Failed to persist PaxosState for index " + logIndex +
                        (error != null ? ": " + error.getMessage() : ""));
                    persistFuture.fail(error != null ? error : new IllegalStateException("Persist returned false"));
                } else {
                    persistFuture.complete(true);
                }

                if (pendingPersistsByIndex.get(logIndex) == persistFuture) {
                    pendingPersistsByIndex.remove(logIndex);
                }
            });
    }
    
    private byte[] serializePaxosState(PaxosState state) {
        // Use messageCodec to serialize PaxosState
        return messageCodec.encode(state);
    }
    
    private PaxosState deserializePaxosState(byte[] data) {
        // Use messageCodec to deserialize PaxosState
        return messageCodec.decode(data, PaxosState.class);
    }
    
    // ========== PUBLIC ACCESSORS FOR TESTING ==========

    public ListenableFuture<PaxosState> getPersistedLogEntry(int logIndex) {
        ListenableFuture<PaxosState> future = new ListenableFuture<>();

        if (!isInitialised() || logStore == null) {
            future.complete(null);
            return future;
        }

        byte[] key = logStore.createLogKey(PAXOS_LOG_ID, logIndex);
        storage.get(key).handle((data, error) -> {
            if (error != null) {
                future.fail(error);
                return;
            }

            if (data == null) {
                future.complete(null);
                return;
            }

            future.complete(deserializePaxosState(data));
        });

        return future;
    }

    public boolean hasPendingPersistFor(int logIndex) {
        ListenableFuture<Boolean> persistFuture = pendingPersistsByIndex.get(logIndex);
        return persistFuture != null && persistFuture.isPending();
    }
    
    public Map<Integer, PaxosState> getPaxosLog() {
        return paxosLogCache;
    }
    
    public String getValue(String key) {
        return kvStore.get(key);
    }
    
    public int getHighWaterMark() {
        return highWaterMark;
    }
}
