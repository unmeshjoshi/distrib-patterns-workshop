package continuations

import com.distribpatterns.twophase.IncrementCounterOperation
import com.distribpatterns.twophase.Operation
import com.distribpatterns.twophase.messages.AcceptRequest
import com.distribpatterns.twophase.messages.AcceptResponse
import com.distribpatterns.twophase.messages.CommitRequest
import com.distribpatterns.twophase.messages.ExecuteRequest
import com.distribpatterns.twophase.messages.ExecuteResponse
import com.distribpatterns.twophase.messages.QueryRequest
import com.distribpatterns.twophase.messages.QueryResponse
import com.distribpatterns.twophase.messages.TwoPhaseMessageTypes
import com.tickloom.ProcessId
import com.tickloom.ProcessParams
import com.tickloom.Replica
import com.tickloom.future.await
import com.tickloom.messaging.AsyncQuorumCallback
import com.tickloom.messaging.Message
import com.tickloom.messaging.MessageType
import com.tickloom.network.PeerType
import java.util.*
import kotlin.coroutines.createCoroutine
import kotlin.coroutines.resume

/**
 * ThreePhaseServer using Kotlin coroutines + tickloom's ListenableFuture.await().
 *
 * The key insight: AsyncQuorumCallback.getQuorumFuture() returns a ListenableFuture,
 * and tickloom now provides `suspend fun ListenableFuture<T>.await(): T`.
 * This eliminates all manual suspendCoroutine bridging.
 *
 * Compare with ThreePhaseServer.java — the callback-based version nests
 * onSuccess/onFailure inside onSuccess/onFailure. Here the coordinator
 * reads as straight-line code:
 *
 *   val queryResponses = awaitQueryQuorum(queryId)
 *   if (hasPending(queryResponses)) { recover(); return }
 *   awaitAcceptQuorum(requestId, operation)
 *   awaitLocalCommitExecution(requestId) { sendCommitToAll(requestId) }
 *   sendClientResponse(response)
 */
class ThreePhaseServerExample(
    peerIds: MutableList<ProcessId>,
    processParams: ProcessParams
) : Replica(peerIds, processParams) {

    private val counters = HashMap<String, Int>()
    private val preparedOperations = HashMap<String, Operation>()

    override fun initialiseHandlers(): Map<MessageType, Handler> {
        return mapOf(
            TwoPhaseMessageTypes.CLIENT_EXECUTE_REQUEST to Handler { msg -> handleClientExecuteRequest(msg) },
            TwoPhaseMessageTypes.QUERY_REQUEST to Handler { msg -> handleQueryRequest(msg) },
            TwoPhaseMessageTypes.QUERY_RESPONSE to Handler { msg -> handleQueryResponse(msg) },
            TwoPhaseMessageTypes.ACCEPT_REQUEST to Handler { msg -> handleAcceptRequest(msg) },
            TwoPhaseMessageTypes.ACCEPT_RESPONSE to Handler { msg -> handleAcceptResponse(msg) },
            TwoPhaseMessageTypes.COMMIT_REQUEST to Handler { msg -> handleCommitRequest(msg) }
        )
    }

    // =====================================================================
    // BRIDGING: tickloom's await() makes these one-liners
    // =====================================================================

    private suspend fun awaitQueryQuorum(queryId: String): Map<ProcessId, QueryResponse> {
        val quorumCallback = AsyncQuorumCallback<QueryResponse>(allNodes.size) { it != null }
        val queryReq = QueryRequest(queryId)
        broadcastToAllReplicas(quorumCallback) { node, correlationId ->
            createMessage(node, correlationId, queryReq, TwoPhaseMessageTypes.QUERY_REQUEST)
        }
        return quorumCallback.quorumFuture.await()
    }

    private suspend fun awaitAcceptQuorum(requestId: String, operation: Operation): Map<ProcessId, AcceptResponse> {
        val quorumCallback = AsyncQuorumCallback<AcceptResponse>(allNodes.size) {
            it != null && it.accepted()
        }
        val acceptReq = AcceptRequest(requestId, operation)
        broadcastToAllReplicas(quorumCallback) { node, correlationId ->
            createMessage(node, correlationId, acceptReq, TwoPhaseMessageTypes.ACCEPT_REQUEST)
        }
        return quorumCallback.quorumFuture.await()
    }

    private suspend fun awaitLocalCommitExecution(
        requestId: String,
        trigger: () -> Unit
    ): ExecuteResponse {
        val future = com.tickloom.future.ListenableFuture<ExecuteResponse>()
        waitingList.add(requestId, object : com.tickloom.messaging.RequestCallback<Any> {
            override fun onResponse(response: Any, fromNode: ProcessId) {
                future.complete(response as ExecuteResponse)
            }
            override fun onError(error: Exception) {
                future.fail(error)
            }
        })
        trigger()
        return future.await()
    }

    // =====================================================================
    // COORDINATOR: Linear, readable flow — no nesting, no state machine
    // =====================================================================

    private fun handleClientExecuteRequest(clientMessage: Message) {
        println("$id: Starting three-phase execution")

        launchCoordinator {
            try {
                // Phase 0: Query all nodes for pending prepared requests
                val queryId = UUID.randomUUID().toString()
                println("$id: Phase 0 - Querying for pending requests")
                val queryResponses = awaitQueryQuorum(queryId)

                // Check if recovery is needed
                val pending = findHighestPending(queryResponses.values)
                if (pending != null) {
                    println("$id: RECOVERY MODE - completing pending ${pending.pendingRequestId()}")
                    recoverPendingRequest(pending)
                    println("$id: Recovery complete — ignoring client request")
                    return@launchCoordinator
                }

                // Normal path: proceed with two-phase commit
                val requestId = UUID.randomUUID().toString()
                val request = deserializePayload(clientMessage.payload(), ExecuteRequest::class.java)
                println("$id: Phase 1 - Awaiting Accept Quorum for $requestId")
                awaitAcceptQuorum(requestId, request.operation())

                println("$id: Phase 2 - Quorum reached, committing $requestId")
                val response = awaitLocalCommitExecution(requestId) {
                    sendCommitToAll(requestId)
                }

                println("$id: Done — sending response to client")
                sendClientExecuteResponse(clientMessage, response)

            } catch (e: Exception) {
                println("$id: Three-phase txn failed: ${e.message}")
                sendClientExecuteResponse(clientMessage, ExecuteResponse(false, 0))
            }
        }
    }

    private suspend fun recoverPendingRequest(pending: QueryResponse) {
        val requestId = pending.pendingRequestId()
        val operation = pending.pendingOperation()

        println("$id: Recovery - re-running Accept for $requestId")
        awaitAcceptQuorum(requestId, operation)

        println("$id: Recovery - Accept quorum reached, sending Commit for $requestId")
        sendCommitToAll(requestId)
    }

    // =====================================================================
    // PARTICIPANT HANDLERS (same as Java version)
    // =====================================================================

    private fun handleQueryRequest(message: Message) {
        val request = deserializePayload(message.payload(), QueryRequest::class.java)

        val response = if (preparedOperations.isEmpty()) {
            QueryResponse.noPendingRequest(request.queryId())
        } else {
            val (key, value) = preparedOperations.entries.first()
            QueryResponse.withPendingRequest(request.queryId(), key, value)
        }

        send(createMessage(message.source(), message.correlationId(),
            response, TwoPhaseMessageTypes.QUERY_RESPONSE))
    }

    private fun handleQueryResponse(message: Message) {
        val response = deserializePayload(message.payload(), QueryResponse::class.java)
        waitingList.handleResponse(message.correlationId(), response, message.source())
    }

    private fun handleAcceptRequest(message: Message) {
        val request = deserializePayload(message.payload(), AcceptRequest::class.java)
        preparedOperations[request.transactionId()] = request.operation()

        send(createMessage(message.source(), message.correlationId(),
            AcceptResponse(request.transactionId(), true),
            TwoPhaseMessageTypes.ACCEPT_RESPONSE))
    }

    private fun handleAcceptResponse(message: Message) {
        val response = deserializePayload(message.payload(), AcceptResponse::class.java)
        waitingList.handleResponse(message.correlationId(), response, message.source())
    }

    private fun handleCommitRequest(message: Message) {
        val request = deserializePayload(message.payload(), CommitRequest::class.java)
        val operation = preparedOperations.remove(request.requestId()) ?: return

        val result = executeOperation(operation)
        waitingList.handleResponse(request.requestId(), ExecuteResponse(true, result), message.source())
    }

    // =====================================================================
    // HELPERS
    // =====================================================================

    private fun findHighestPending(responses: Collection<QueryResponse>): QueryResponse? {
        return responses
            .filter { it.hasPendingRequest() }
            .maxByOrNull { it.pendingRequestId() }
    }

    private fun sendCommitToAll(requestId: String) {
        for (node in allNodes) {
            send(createMessage(node, UUID.randomUUID().toString(),
                CommitRequest(requestId), TwoPhaseMessageTypes.COMMIT_REQUEST))
        }
    }

    private fun sendClientExecuteResponse(clientMessage: Message, response: ExecuteResponse) {
        send(Message(
            id, clientMessage.source(), PeerType.SERVER,
            TwoPhaseMessageTypes.CLIENT_EXECUTE_RESPONSE,
            messageCodec.encode(response), clientMessage.correlationId()
        ))
    }

    private fun executeOperation(operation: Operation): Int {
        if (operation is IncrementCounterOperation) {
            val currentValue = counters.getOrDefault(operation.key(), 0)
            val newValue = currentValue + operation.delta()
            counters[operation.key()] = newValue
            return newValue
        }
        throw IllegalArgumentException("Unknown operation type: $operation")
    }

    fun getCounterValue(key: String): Int? = counters[key]

    private fun launchCoordinator(block: suspend () -> Unit) {
        val completion = object : kotlin.coroutines.Continuation<Unit> {
            override val context = kotlin.coroutines.EmptyCoroutineContext
            override fun resumeWith(result: Result<Unit>) {
                result.onFailure { it.printStackTrace() }
            }
        }
        val suspendFunction = suspend { block() }
        suspendFunction.createCoroutine(completion).resume(Unit)
    }
}
