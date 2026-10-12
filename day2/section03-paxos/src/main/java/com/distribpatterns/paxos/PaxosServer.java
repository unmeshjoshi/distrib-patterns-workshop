package com.distribpatterns.paxos;

import com.distribpatterns.paxos.messages.*;
import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.fs.FileIO;
import com.tickloom.fs.OpenMode;
import com.tickloom.Replica;
import com.tickloom.future.TickCompletableFuture;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageType;
import com.tickloom.messaging.RequestCallback;
import com.tickloom.wal.BatchedLog;
import com.tickloom.wal.Wal;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Single Value Paxos implementation following the established structure
 * from TwoPhaseServer, ThreePhaseServer, and GenerationVotingServer.
 * <p>
 * Paxos Protocol:
 * 1. PREPARE phase: Proposer gets promises from quorum
 * 2. PROPOSE phase: Proposer proposes value (highest accepted or client's)
 * 3. COMMIT phase: Coordinator commits the agreed value
 */

public class PaxosServer extends Replica {
    private static final String DECISION_LOG_FILE = "paxos.wal";
    private static final int GROUP_WAIT_TICKS = 2;
    
    // State machine
    private final Map<String, Integer> counters = new HashMap<>();
    private final Map<String, String> kvStore = new HashMap<>();
    
    // Paxos state (immutable, recreated on each update). It is always the result of applying, in order, every
    // decision in the log and the ones still being written.
    private PaxosState state = new PaxosState();

    // Every promise, accept and commit is appended here, and the reply goes out only once it is durable.
    private FileIO decisionFile;
    private BatchedLog decisionLog;
    private int decisionsNotYetDurable;
    
    // For generation number
    private final AtomicInteger generationCounter = new AtomicInteger(0);
    
    // Retry mechanism
    private static final int MAX_RETRIES = 5;

    public PaxosServer(List<ProcessId> allNodes, ProcessParams processParams) {
        super(allNodes, processParams);
        // Initialization is handled by Process.initialise()
    }

    /** The server is not running until its state is rebuilt from the decision log. */
    @Override
    protected TickCompletableFuture<?> onStart() {
        if (decisionFile != null) {
            decisionFile.close();   // a restart on the same instance: let go of the old handle
        }
        state = new PaxosState();
        counters.clear();
        kvStore.clear();
        decisionLog = null;
        decisionsNotYetDurable = 0;
        return fileSystem.open(DECISION_LOG_FILE, OpenMode.OPEN_OR_CREATE)
                // A new file's name is durable only after the directory is synced.
                .thenCompose(opened -> fileSystem.syncDirectory().thenApply(ignored -> opened))
                .thenCompose(opened -> {
                    decisionFile = opened;
                    Wal wal = new Wal(opened);
                    return wal.recover().thenApply(entries -> {
                        entries.forEach(entry -> replay(deserializePayload(entry.payload(), PaxosDecision.class)));
                        // The state machine is not logged, so the one committed value is executed again.
                        state.committedValue().ifPresent(this::executeOperation);
                        decisionLog = new BatchedLog(wal, GROUP_WAIT_TICKS);
                        return (Void) null;
                    });
                });
    }

    private void replay(PaxosDecision decision) {
        state = decision.applyTo(state);
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
     * order they were appended, so a reply never goes out before an earlier decision it depends on is durable.
     * If the append fails the log is unusable, so no reply is sent.
     */
    private void recordDecision(PaxosDecision decision, Runnable onDurable) {
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
                PaxosMessageTypes.CLIENT_EXECUTE_REQUEST, this::handleClientExecuteRequest,
                PaxosMessageTypes.PREPARE_REQUEST, this::handlePrepareRequest,
                PaxosMessageTypes.PREPARE_RESPONSE, this::handlePrepareResponse,
                PaxosMessageTypes.PROPOSE_REQUEST, this::handleProposeRequest,
                PaxosMessageTypes.PROPOSE_RESPONSE, this::handleProposeResponse,
                PaxosMessageTypes.COMMIT_REQUEST, this::handleCommitRequest,
                PaxosMessageTypes.COMMIT_RESPONSE, this::handleCommitResponse
        );
    }

    // ========== PROPOSER ROLE ==========

    private void handleClientExecuteRequest(Message clientMessage) {
        ExecuteRequest request = deserializePayload(clientMessage.payload(), ExecuteRequest.class);
        String clientKey = clientKeyFor(clientMessage);

        System.out.println(id + ": Received client request for operation: " + request.operation());

        registerClientCompletionCallback(clientMessage, clientKey);

        //clientMessage.. we can carry the original client request operation through to the commit request,
        //so that we respond to client with success only if the client request is the one we executed and committed
        //Otherwise we send a failure response back with the final operation that is executed.
        // Start Paxos protocol with retry

        startPaxosWithRetry(new ClientMessage(clientKey, request.operation()), 0);
    }

    //Starts paxos protocol
    private void startPaxosWithRetry(ClientMessage clientMessage, int attempt) {
        if (attemptsExhausted(attempt)) {
            System.out.println(id + ": Max retries reached, waiting for another proposer to complete the value");
            return;
        }

        // Generate new generation number (higher than any seen)
        int generation = generationCounter.incrementAndGet();
        // Phase 1: Prepare

        System.out.println(id + ": Starting Paxos attempt " + (attempt + 1) + " with generation " + generation);
        startPreparePhase(generation, clientMessage, attempt);
    }
    private void startPreparePhase(int generation, ClientMessage clientMessage, int attempt) {
        System.out.println(id + ": Phase 1 - PREPARE with generation " + generation);

        PrepareRequest prepareReq = new PrepareRequest(generation);
        this.<PrepareResponse>quorumRequest(PaxosMessageTypes.PREPARE_REQUEST, prepareReq)
                .countResponseIf(response -> isPromiseFor(response, generation))
                .send()
                .whenComplete((responses, error) -> {
                    if (error != null) {
                        System.out.println(id + ": PREPARE quorum failed: " + error.getMessage() + ", retrying...");
                        startPaxosWithRetry(clientMessage, attempt + 1);
                    } else {
                        System.out.println(id + ": PREPARE quorum reached (" + responses.size() + " promises), moving to Phase 2");
                        startProposePhase(generation, clientMessage, responses, attempt);
                    }
                });

        System.out.println(id + ": Broadcast PREPARE to " + getAllNodes().size() + " nodes");
    }
    private void startProposePhase(long generation, ClientMessage clientMessage,
                                   Map<ProcessId, PrepareResponse> promises, int attempt) {
        Operation valueToPropose = selectRequestOperationToPropose(clientMessage.operation(), promises);

        System.out.println(id + ": Phase 2 - PROPOSE value " + valueToPropose + " with generation " + generation);

        ProposeRequest proposeReq = new ProposeRequest(generation, valueToPropose);
        this.<ProposeResponse>quorumRequest(PaxosMessageTypes.PROPOSE_REQUEST, proposeReq)
                .countResponseIf(response -> isAcceptedFor(response, generation))
                .send()
                .whenComplete((responses, error) -> {
                    if (error != null) {
                        System.out.println(id + ": PROPOSE quorum failed: " + error.getMessage() + ", retrying...");
                        startPaxosWithRetry(clientMessage, attempt + 1);
                    } else {
                        System.out.println(id + ": PROPOSE quorum reached (" + responses.size() + " accepts), moving to Phase 3");
                        startCommitPhase(generation, clientMessage, valueToPropose);
                    }
                });

        System.out.println(id + ": Broadcast PROPOSE to " + getAllNodes().size() + " nodes");
    }

    private void startCommitPhase(long generation, ClientMessage clientMessage, Operation committedOperation) {
        System.out.println(id + ": Phase 3 - COMMIT value " + committedOperation);

        CommitRequest commitReq = new CommitRequest(generation, clientMessage, committedOperation);
        this.<CommitResponse>quorumRequest(PaxosMessageTypes.COMMIT_REQUEST, commitReq)
                .countResponseIf(response -> response != null && response.committed())
                .send()
                .whenComplete((acks, err) -> {
                    if (err != null) {
                        System.out.println(id + ": COMMIT quorum failed: " + err.getMessage());
                    } else {
                        System.out.println(id + ": COMMIT quorum reached (" + acks.size() + ")");
                    }
                });

        System.out.println(id + ": Broadcast COMMIT to " + getAllNodes().size() + " nodes");
    }

    private String clientKeyFor(Message clientMessage) {
        return "client_" + clientMessage.correlationId();
    }

    private void registerClientCompletionCallback(Message clientMessage, String clientKey) {
        waitingList.add(clientKey, new RequestCallback<Object>() {
            @Override
            public void onResponse(Object response, ProcessId fromNode) {
                var responseMsg = createMessage(clientMessage.source(), clientMessage.correlationId(),
                        response, PaxosMessageTypes.CLIENT_EXECUTE_RESPONSE);
                System.out.println(id + ": Sending response to client: " + response);
                send(responseMsg);
            }

            @Override
            public void onError(Exception error) {
                System.out.println(id + ": Error processing client request: " + error.getMessage());
                send(createMessage(clientMessage.source(), clientMessage.correlationId(),
                        new ExecuteResponse(false, null), PaxosMessageTypes.CLIENT_EXECUTE_RESPONSE));
            }
        });
    }

    private static boolean attemptsExhausted(int attempt) {
        return attempt >= MAX_RETRIES;
    }

    private static boolean isPromiseFor(PrepareResponse response, long generation) {
        return response != null
                && response.promised()
                && response.promisedGeneration() == generation;
    }

    private void handlePrepareResponse(Message message) {
        PrepareResponse response = deserializePayload(message.payload(), PrepareResponse.class);
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }

    // ========== PHASE 2: PROPOSE/ACCEPT ==========
    private Operation selectRequestOperationToPropose(Operation clientOperation,
                                                      Map<ProcessId, PrepareResponse> promises) {
        // Find the promise with the highest accepted generation
        Optional<PrepareResponse> highestAccepted = promises.values().stream()
                .filter(p -> p.acceptedGeneration() != null)
                .max(Comparator.comparingLong(PrepareResponse::acceptedGeneration));

        if (highestAccepted.isPresent() && highestAccepted.get().acceptedValue() != null) {
            Operation recoveredValue = highestAccepted.get().acceptedValue();
            System.out.println(id + ": Found previously accepted value, using it: " + recoveredValue);
            return recoveredValue;
        } else {
            System.out.println(id + ": No previously accepted value, using client's: " + clientOperation);
            return clientOperation;
        }
    }

    private static boolean isAcceptedFor(ProposeResponse response, long generation) {
        return response != null
                && response.accepted()
                && response.generation() == generation;
    }

    private void handleProposeResponse(Message message) {
        ProposeResponse response = deserializePayload(message.payload(), ProposeResponse.class);
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }

    // ========== PHASE 3: COMMIT/LEARN ==========


    private void handleCommitResponse(Message message) {
        CommitResponse response = deserializePayload(message.payload(), CommitResponse.class);
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }

    // ========== ACCEPTOR / LEARNER ROLE ==========

    private void handlePrepareRequest(Message message) {
        PrepareRequest request = deserializePayload(message.payload(), PrepareRequest.class);
        int generation = request.generation();

        System.out.println(id + ": Received PREPARE for generation " + generation + " (current promise: " + state.promisedGeneration() + ")");

        if (state.canPromise(generation)) {
            promiseGeneration(message, generation);
            return;
        }

        rejectPrepareRequest(message, generation);
    }

    private void promiseGeneration(Message message, int generation) {
        state = state.promise(generation);
        recordDecision(PaxosDecision.promise(generation), () -> {
            PrepareResponse response = promiseResponse();
            System.out.println(id + ": PROMISED generation " + generation
                    + acceptedValueDescription());
            sendPrepareResponse(message, response);
        });
    }

    private void rejectPrepareRequest(Message message, int generation) {
        System.out.println(id + ": REJECTED generation " + generation
                + ", already promised " + state.promisedGeneration());
        sendPrepareResponse(message, rejectedPrepareResponse());
    }

    private PrepareResponse promiseResponse() {
        return new PrepareResponse(
                true,
                state.promisedGeneration(),
                state.acceptedGeneration().orElse(null),
                state.acceptedValue().orElse(null)
        );
    }

    private PrepareResponse rejectedPrepareResponse() {
        return new PrepareResponse(
                false,
                state.promisedGeneration(),
                state.acceptedGeneration().orElse(null),
                state.acceptedValue().orElse(null)
        );
    }

    private String acceptedValueDescription() {
        return state.acceptedValue().isPresent() ? " (have accepted value)" : " (no accepted value)";
    }

    private void sendPrepareResponse(Message message, PrepareResponse response) {
        send(createMessage(message.source(), message.correlationId(), response,
                PaxosMessageTypes.PREPARE_RESPONSE));
    }

    private void handleProposeRequest(Message message) {
        ProposeRequest request = deserializePayload(message.payload(), ProposeRequest.class);
        long generation = request.generation();
        Operation value = request.value();

        System.out.println(id + ": Received PROPOSE for generation " + generation + ", value " + value);

        if (state.canAccept(generation)) {
            state = state.accept(generation, value);
            recordDecision(PaxosDecision.accept(generation, value), () -> {
                System.out.println(id + ": ACCEPTED value " + value + " for generation " + generation);
                send(createMessage(message.source(), message.correlationId(),
                        new ProposeResponse(true, generation), PaxosMessageTypes.PROPOSE_RESPONSE));
            });
            return;
        }

        System.out.println(id + ": REJECTED proposal for generation " + generation +
                " (promised " + state.promisedGeneration() + ")");
        send(createMessage(message.source(), message.correlationId(),
                new ProposeResponse(false, generation), PaxosMessageTypes.PROPOSE_RESPONSE));
    }

    private void handleCommitRequest(Message message) {
        CommitRequest request = deserializePayload(message.payload(), CommitRequest.class);
        Operation committedOperation = request.committedOperation();

        System.out.println(id + ": Received COMMIT for generation " + request.generation()
                + ", committedOperation " + committedOperation);

        commitAndExecute(request);
    }

    private void commitAndExecute(CommitRequest request) {
        Operation committedOperation = request.committedOperation();

        // Learning a chosen value must not lower the promise already made.
        // So commit records the chosen generation and value, while preserving the higher promise.
        state = state.commit(request.generation(), committedOperation);
        recordDecision(PaxosDecision.commit(request.generation(), committedOperation), () -> {
            String result = executeCommittedOperation(committedOperation);
            respondToClientWithCommittedResult(request, committedOperation, result);
        });
    }

    private String executeCommittedOperation(Operation committedOperation) {
        String result = executeOperation(committedOperation);
        System.out.println(id + ": COMMITTED and executed committedOperation "
                + committedOperation + ", result: " + result);
        return result;
    }

    private void respondToClientWithCommittedResult(CommitRequest request,
                                                    Operation committedOperation,
                                                    String result) {
        boolean clientRequestCommitted = committedClientOperation(request, committedOperation);
        if (!clientRequestCommitted) {
            logCommittedRecoveredOperation(request, committedOperation);
        }

        System.out.println(id + ": Coordinator responding to client with result: " + result);
        waitingList.handleResponse(request.clientKey(), new ExecuteResponse(clientRequestCommitted, result), id);
    }

    private static boolean committedClientOperation(CommitRequest request, Operation committedOperation) {
        return committedOperation.equals(request.clientMessage().operation());
    }

    private void logCommittedRecoveredOperation(CommitRequest request, Operation committedOperation) {
        System.out.println(id + ": Client operation was " + request.clientMessage().operation()
                + " but Paxos committed recovered operation " + committedOperation);
    }

    // ========== SHARED HELPERS ==========

    // ========== STATE MACHINE EXECUTION ==========

    private String executeOperation(Operation operation) {
        if (operation instanceof IncrementCounterOperation inc) {
            int newValue = counters.getOrDefault(inc.key(), 0) + 1;
            counters.put(inc.key(), newValue);
            System.out.println(id + ": Executed INCREMENT, counter=" + newValue);
            return String.valueOf(newValue);
        } else if (operation instanceof SetValueOperation set) {
            kvStore.put(set.key(), set.value());
            System.out.println(id + ": Executed SET, key=" + set.key() + ", value=" + set.value());
            return set.value();
        }
        throw new IllegalArgumentException("Unknown operation: " + operation);
    }

    // ========== PUBLIC ACCESSORS FOR TESTING ==========

    /** True while a decision has been applied to the state but is not yet durable, so no reply for it has gone out. */
    public boolean hasDecisionsNotYetDurable() {
        return decisionsNotYetDurable > 0;
    }

    public PaxosState getPaxosState() {
        return state;
    }

    public Integer getCounter(String key) {
        return counters.get(key);
    }

    public String getValue(String key) {
        return kvStore.get(key);
    }
}
