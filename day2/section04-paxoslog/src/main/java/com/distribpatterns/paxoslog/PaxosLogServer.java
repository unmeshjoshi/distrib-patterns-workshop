package com.distribpatterns.paxoslog;

import com.distribpatterns.paxoslog.messages.*;
import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.Replica;
import com.tickloom.fs.OpenMode;
import com.tickloom.future.TickCompletableFuture;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageType;
import com.tickloom.messaging.RequestCallback;
import com.tickloom.wal.BatchedLog;
import com.tickloom.wal.Wal;

import java.util.*;

/**
 * PaxosLog: Replicated log where each entry is decided via Paxos consensus.
 * 
 * Key features:
 * - Each log index runs independent Paxos instance
 * - Sequential execution: entries must be committed in order (no gaps)
 * - Automatic conflict resolution: retry at next index if value rejected
 * - State machine: applies commands (SetValue, CompareAndSwap) to KV store
 * - Durability: every promise, accept and commit, for every index, goes into one write-ahead log. A reply goes out
 *   only once its record is durable, and a restart rebuilds the log by replaying it.
 */
public class PaxosLogServer extends Replica {
    private static final String DECISION_LOG_FILE = "paxoslog.wal";
    private static final int GROUP_WAIT_TICKS = 2;

    // In-memory cache: one PaxosState per log index. It is always the result of applying, in order, every
    // decision in the write-ahead log and the ones still being written.
    private final Map<Integer, PaxosState> paxosLogCache = new TreeMap<>();

    // Indexes whose commit record is durable. Only these are executed, so the state machine never runs ahead of the log.
    private final Set<Integer> durablyCommitted = new HashSet<>();

    private BatchedLog decisionLog;
    private int decisionsNotYetDurable;

    // State machine: key-value store
    private final Map<String, String> kvStore = new HashMap<>();
    
    // Track highest committed index (high water mark)
    private int highWaterMark = -1;
    
    // Generation counter for Paxos rounds
    private int generationCounter = 0;
    
    // No-op operation for reads
    private static final NoOpOperation NO_OP = new NoOpOperation();

    public PaxosLogServer(List<ProcessId> allNodes, ProcessParams processParams) {
        super(allNodes, processParams);
    }

    /** The server is not running until its log is rebuilt from the write-ahead log and the committed entries executed. */
    @Override
    protected TickCompletableFuture<?> onStart() {
        if (decisionLog != null) {
            decisionLog.close();   // a restart on the same instance: let go of the old handle
        }
        paxosLogCache.clear();
        durablyCommitted.clear();
        kvStore.clear();
        highWaterMark = -1;
        decisionLog = null;
        decisionsNotYetDurable = 0;
        return fileSystem.open(DECISION_LOG_FILE, OpenMode.OPEN_OR_CREATE)
                // A new file's name is durable only after the directory is synced.
                .thenCompose(opened -> fileSystem.syncDirectory().thenApply(ignored -> opened))
                .thenCompose(opened -> {
                    Wal wal = new Wal(opened);
                    return wal.recover().thenApply(entries -> {
                        entries.forEach(entry ->
                                deserializePayload(entry.payload(), PaxosLogDecision.class).applyTo(paxosLogCache));
                        paxosLogCache.forEach((index, state) -> {
                            if (state.committedValue().isPresent()) {
                                durablyCommitted.add(index);
                            }
                        });
                        // The state machine is not logged, so the committed entries are executed again.
                        tryExecuteLogEntries();
                        // Generations this node proposes must beat every promise it has already made.
                        paxosLogCache.values().forEach(state ->
                                generationCounter = Math.max(generationCounter, state.promisedGeneration()));
                        decisionLog = new BatchedLog(wal, GROUP_WAIT_TICKS);
                        return null;
                    });
                });
    }

    @Override
    protected void onTick() {
        if (decisionLog != null) {
            decisionLog.tick();
        }
    }

    /**
     * Appends the decision to the log and runs {@code onDurable} once it is on disk. The in-memory state has already
     * been updated by the caller, so the next message sees it; only the reply waits. Records become durable in the
     * order they were appended, whatever their index. If the append fails the log is unusable, so no reply is sent.
     */
    private void recordDecision(PaxosLogDecision decision, Runnable onDurable) {
        decisionsNotYetDurable++;
        decisionLog.append(serializePayload(decision)).whenComplete((offset, error) -> {
            decisionsNotYetDurable--;
            if (error != null) {
                System.err.println(id + ": Failed to record " + decision + ": " + error.getMessage());
                return;
            }
            onDurable.run();
        });
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
        int generation = ++generationCounter;
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
    
    // ========== PROPOSER ROLE ==========
    // ========== PHASE 1: PREPARE ==========
    
    private void startPreparePhase(int logIndex, int generation, Operation operation, String clientKey, int attempt) {
        System.out.println(id + ": Phase 1 - PREPARE for index " + logIndex + ", generation " + generation);

        PrepareRequest prepareReq = new PrepareRequest(logIndex, generation);
        this.<PrepareResponse>quorumRequest(PaxosLogMessageTypes.PREPARE_REQUEST, prepareReq)
                .countResponseIf(response -> response != null && response.promised())
                .send()
                .whenComplete((responses, error) -> {
                    if (error != null) {
                        retryPrepareAtHigherGeneration(logIndex, operation, clientKey, attempt, error);
                    } else {
                        System.out.println(id + ": PREPARE quorum reached for index " + logIndex);
                        startProposePhase(logIndex, generation, operation, responses, clientKey, attempt);
                    }
                });
    }

    private void retryPrepareAtHigherGeneration(int logIndex,
                                                Operation operation,
                                                String clientKey,
                                                int attempt,
                                                Throwable error) {
        System.out.println(id + ": PREPARE quorum failed for index " + logIndex + ": " + error.getMessage());
        int newGeneration = ++generationCounter;
        runPaxosForIndex(logIndex, newGeneration, operation, clientKey, attempt + 1);
    }
    
    // ========== PHASE 2: PROPOSE ==========
    
    private void startProposePhase(int logIndex, int generation, Operation initialOperation,
                                   Map<ProcessId, PrepareResponse> promises, String clientKey, int attempt) {
        Operation valueToPropose = selectValueToPropose(initialOperation, promises);

        System.out.println(id + ": Phase 2 - PROPOSE " + valueToPropose + " for index " + logIndex);

        ProposeRequest proposeReq = new ProposeRequest(logIndex, generation, valueToPropose);
        this.<ProposeResponse>quorumRequest(PaxosLogMessageTypes.PROPOSE_REQUEST, proposeReq)
                .countResponseIf(response -> response != null && response.accepted())
                .send()
                .whenComplete((responses, error) -> {
                    if (error != null) {
                        retryProposalAtHigherGeneration(logIndex, initialOperation, clientKey, attempt);
                    } else {
                        System.out.println(id + ": PROPOSE quorum reached for index " + logIndex);
                        startCommitPhase(logIndex, generation, valueToPropose, initialOperation, clientKey);
                    }
                });
    }

    private void retryProposalAtHigherGeneration(int logIndex,
                                                 Operation initialOperation,
                                                 String clientKey,
                                                 int attempt) {
        System.out.println(id + ": PROPOSE quorum failed for index " + logIndex);
        int newGeneration = ++generationCounter;
        runPaxosForIndex(logIndex, newGeneration, initialOperation, clientKey, attempt + 1);
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
    
    // ========== PHASE 3: COMMIT ==========
    
    private void startCommitPhase(int logIndex, int generation, Operation agreedValue, 
                                  Operation initialOperation, String clientKey) {
        System.out.println(id + ": Phase 3 - COMMIT " + agreedValue + " for index " + logIndex);

        if (agreedValue.equals(initialOperation)) {
            registerClientForCommittedIndex(logIndex, clientKey);
        } else {
            System.out.println(id + ": Value mismatch at index " + logIndex + ", retrying at next index");
            appendToLog(logIndex + 1, initialOperation, clientKey);
        }

        CommitRequest commitReq = new CommitRequest(logIndex, generation, agreedValue);
        this.<CommitResponse>quorumRequest(PaxosLogMessageTypes.COMMIT_REQUEST, commitReq)
                .countResponseIf(response -> response != null && response.success())
                .send()
                .whenComplete((responses, error) -> {
                    if (error != null) {
                        System.out.println(id + ": COMMIT quorum failed for index " + logIndex);
                    } else {
                        System.out.println(id + ": COMMIT quorum reached for index " + logIndex);
                    }
                });
    }

    private void commitValue(Message message,
                             int logIndex,
                             int generation,
                             Operation value,
                             PaxosState paxosState) {
        paxosLogCache.put(logIndex, paxosState.commit(generation, value));
        recordDecision(PaxosLogDecision.commit(logIndex, generation, value), () -> {
            System.out.println(id + ": COMMITTED index " + logIndex);
            durablyCommitted.add(logIndex);
            tryExecuteLogEntries();
            sendCommitResponse(message, true);
        });
    }

    private void rejectCommit(Message message, int logIndex) {
        System.out.println(id + ": REJECTED commit for index " + logIndex);
        sendCommitResponse(message, false);
    }

    private void sendCommitResponse(Message requestMessage, boolean success) {
        send(createMessage(
            requestMessage.source(),
            requestMessage.correlationId(),
            new CommitResponse(success),
            PaxosLogMessageTypes.COMMIT_RESPONSE
        ));
    }

    private void registerClientForCommittedIndex(int logIndex, String clientKey) {
        waitingList.add(executionKeyFor(logIndex), new RequestCallback<>() {
            @Override
            public void onResponse(Object response, ProcessId fromNode) {
                waitingList.handleResponse(clientKey, response, fromNode);
            }

            @Override
            public void onError(Exception error) {
                waitingList.handleResponse(clientKey, new ExecuteCommandResponse(false, null), id);
            }
        });
    }

    // The waiting-list key under which a client's callback waits for its log index to be executed.
    private String executionKeyFor(int logIndex) {
        return "execution_" + logIndex;
    }

    // ========== ACCEPTOR / LEARNER ROLE ==========
    // ========== PREPARE / PROMISE ==========

    private void handlePrepareRequest(Message message) {
        PrepareRequest request = deserializePayload(message.payload(), PrepareRequest.class);
        int logIndex = request.logIndex();
        int generation = request.generation();

        var paxosState = getOrCreatePaxosState(logIndex);

        if (paxosState.canPromise(generation)) {
            promiseGeneration(message, logIndex, generation, paxosState);
            return;
        }

        rejectPrepareRequest(message, logIndex, generation, paxosState);
    }

    private void promiseGeneration(Message message, int logIndex, int generation, PaxosState paxosState) {
        var newState = paxosState.promise(generation);
        paxosLogCache.put(logIndex, newState);
        recordDecision(PaxosLogDecision.promise(logIndex, generation), () -> {
            System.out.println(id + ": PROMISED generation " + generation + " for index " + logIndex);
            sendPrepareResponse(message, prepareResponse(true, newState));
        });
    }

    private void rejectPrepareRequest(Message message, int logIndex, int generation, PaxosState paxosState) {
        System.out.println(id + ": REJECTED generation " + generation + " for index " + logIndex);
        sendPrepareResponse(message, prepareResponse(false, paxosState));
    }

    private PrepareResponse prepareResponse(boolean promised, PaxosState paxosState) {
        return new PrepareResponse(
            promised,
            paxosState.acceptedGeneration().orElse(null),
            paxosState.acceptedValue().orElse(null)
        );
    }

    private void sendPrepareResponse(Message requestMessage, PrepareResponse response) {
        send(createMessage(
            requestMessage.source(),
            requestMessage.correlationId(),
            response,
            PaxosLogMessageTypes.PREPARE_RESPONSE
        ));
    }

    private void handlePrepareResponse(Message message) {
        PrepareResponse response = deserializePayload(message.payload(), PrepareResponse.class);
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }

    // ========== PROPOSE / ACCEPT ==========

    private void handleProposeRequest(Message message) {
        ProposeRequest request = deserializePayload(message.payload(), ProposeRequest.class);
        int logIndex = request.logIndex();
        int generation = request.generation();
        Operation value = request.value();

        var paxosState = getOrCreatePaxosState(logIndex);

        if (paxosState.canAccept(generation)) {
            acceptProposal(message, logIndex, generation, value, paxosState);
            return;
        }

        rejectProposal(message, logIndex);
    }

    private void acceptProposal(Message message,
                                int logIndex,
                                int generation,
                                Operation value,
                                PaxosState paxosState) {
        paxosLogCache.put(logIndex, paxosState.accept(generation, value));
        recordDecision(PaxosLogDecision.accept(logIndex, generation, value), () -> {
            System.out.println(id + ": ACCEPTED proposal for index " + logIndex);
            sendProposeResponse(message, new ProposeResponse(logIndex, true));
        });
    }

    private void rejectProposal(Message message, int logIndex) {
        System.out.println(id + ": REJECTED proposal for index " + logIndex);
        sendProposeResponse(message, new ProposeResponse(logIndex, false));
    }

    private void sendProposeResponse(Message requestMessage, ProposeResponse response) {
        send(createMessage(
            requestMessage.source(),
            requestMessage.correlationId(),
            response,
            PaxosLogMessageTypes.PROPOSE_RESPONSE
        ));
    }

    private void handleProposeResponse(Message message) {
        ProposeResponse response = deserializePayload(message.payload(), ProposeResponse.class);
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }

    // ========== COMMIT / LEARN ==========

    private void handleCommitRequest(Message message) {
        CommitRequest request = deserializePayload(message.payload(), CommitRequest.class);
        int logIndex = request.logIndex();
        int generation = request.generation();
        Operation value = request.value();

        System.out.println(id + ": Received COMMIT for index " + logIndex + ", value " + value);

        var paxosState = getOrCreatePaxosState(logIndex);

        if (paxosState.canAccept(generation)) {
            commitValue(message, logIndex, generation, value, paxosState);
            return;
        }

        rejectCommit(message, logIndex);
    }

    private void handleCommitResponse(Message message) {
        CommitResponse response = deserializePayload(message.payload(), CommitResponse.class);
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }
    
    // ========== LOG EXECUTION (STATE MACHINE) ==========
    
    /**
     * Try to execute committed log entries in order (no gaps).
     */
    private void tryExecuteLogEntries() {
        // Execute all consecutive committed entries starting from highWaterMark + 1
        for (int index = highWaterMark + 1; ; index++) {
            PaxosState state = paxosLogCache.get(index);
            if (state == null || state.committedValue().isEmpty() || !durablyCommitted.contains(index)) {
                // Gap found, no more entries, or the commit is not durable yet
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
        
        waitingList.handleResponse(executionKeyFor(logIndex), new ExecuteCommandResponse(success, result), id);
    }

    private PaxosState getOrCreatePaxosState(int logIndex) {
        return paxosLogCache.getOrDefault(logIndex, new PaxosState());
    }
    
    // ========== PUBLIC ACCESSORS FOR TESTING ==========

    public PaxosState getLogEntry(int logIndex) {
        return paxosLogCache.get(logIndex);
    }
    
    public Map<Integer, PaxosState> getPaxosLog() {
        return paxosLogCache;
    }
    
    public String getValue(String key) {
        return kvStore.get(key);
    }
    
    /** True while a decision has been applied to the log but is not yet durable, so no reply for it has gone out. */
    public boolean hasDecisionsNotYetDurable() {
        return decisionsNotYetDurable > 0;
    }

    public int getHighWaterMark() {
        return highWaterMark;
    }
}
