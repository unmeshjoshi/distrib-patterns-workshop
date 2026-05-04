package continuations

import com.distribpatterns.twophase.IncrementCounterOperation
import com.distribpatterns.twophase.Operation
import com.distribpatterns.twophase.messages.AcceptRequest
import com.distribpatterns.twophase.messages.AcceptResponse
import com.distribpatterns.twophase.messages.CommitRequest
import com.distribpatterns.twophase.messages.ExecuteRequest
import com.distribpatterns.twophase.messages.ExecuteResponse
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
 * TwoPhaseServer using Kotlin coroutines + tickloom's ListenableFuture.await().
 *
 * The coordinator flow reads linearly:
 *   awaitAcceptQuorum(requestId, request)
 *   val response = awaitLocalCommitExecution(requestId) { sendCommitToAll(requestId) }
 *   sendClientResponse(response)
 */
class TwoPhaseServerExample(peerIds: MutableList<ProcessId>, processParams: ProcessParams) : Replica(peerIds, processParams) {

    private val counters = HashMap<String, Int>()
    private val preparedOperations = HashMap<String, Operation>()

    override fun initialiseHandlers(): Map<MessageType, Handler> {
        return mapOf(
            TwoPhaseMessageTypes.CLIENT_EXECUTE_REQUEST to Handler { msg -> handleClientExecuteRequest(msg) },
            TwoPhaseMessageTypes.ACCEPT_REQUEST to Handler { msg -> handleAcceptRequest(msg) },
            TwoPhaseMessageTypes.ACCEPT_RESPONSE to Handler { msg -> handleAcceptResponse(msg) },
            TwoPhaseMessageTypes.COMMIT_REQUEST to Handler { msg -> handleCommitRequest(msg) }
        )
    }

    // =====================================================================
    // BRIDGING: tickloom's await() makes these clean
    // =====================================================================

    private suspend fun awaitAcceptQuorum(requestId: String, request: ExecuteRequest): Map<ProcessId, AcceptResponse> {
        val quorumCallback = AsyncQuorumCallback<AcceptResponse>(allNodes.size) {
            response -> response != null && response.accepted()
        }
        val acceptReq = AcceptRequest(requestId, request.operation())
        broadcastToAllReplicas(quorumCallback) { node, correlationId ->
            createMessage(node, correlationId, acceptReq, TwoPhaseMessageTypes.ACCEPT_REQUEST)
        }
        return quorumCallback.quorumFuture.await()
    }

    private suspend fun awaitLocalCommitExecution(requestId: String, trigger: () -> Unit): ExecuteResponse {
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
    // COORDINATOR: Linear flow
    // =====================================================================

    private fun handleClientExecuteRequest(clientMessage: Message) {
        val requestId = UUID.randomUUID().toString()
        val request = deserializePayload(clientMessage.payload(), ExecuteRequest::class.java)

        println("$id: Starting two phased execution $requestId")

        launchCoordinator {
            try {
                println("$id: Phase 1 - Awaiting Quorum")
                awaitAcceptQuorum(requestId, request)

                println("$id: Quorum reached for txn $requestId, sending COMMIT")
                val executeResponse = awaitLocalCommitExecution(requestId) {
                    sendCommitToAll(requestId)
                }

                println("$id: Sending response to client")
                sendClientExecuteResponse(clientMessage, executeResponse)

            } catch (e: Exception) {
                println("$id: TwoPhase Txn Failed: ${e.message}")
                sendClientExecuteResponse(clientMessage, ExecuteResponse(false, 0))
            }
        }
    }

    // =====================================================================
    // PARTICIPANT HANDLERS
    // =====================================================================

    private fun handleAcceptRequest(message: Message) {
        val request = deserializePayload(message.payload(), AcceptRequest::class.java)
        preparedOperations[request.transactionId()] = request.operation()

        val responseMsg = createMessage(
            message.source(), message.correlationId(),
            AcceptResponse(request.transactionId(), true),
            TwoPhaseMessageTypes.ACCEPT_RESPONSE
        )
        send(responseMsg)
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

    private fun sendCommitToAll(requestId: String) {
        for (node in allNodes) {
            send(createMessage(node, UUID.randomUUID().toString(), CommitRequest(requestId), TwoPhaseMessageTypes.COMMIT_REQUEST))
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
