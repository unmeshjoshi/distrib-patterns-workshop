package continuations

import com.distribpatterns.twophase.IncrementCounterOperation
import com.distribpatterns.twophase.ThreePhaseClient
import com.distribpatterns.twophase.messages.TwoPhaseMessageTypes
import com.tickloom.ProcessId
import com.tickloom.testkit.Cluster
import com.tickloom.testkit.NodeGroup
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

/**
 * Tests for ThreePhaseServerExample — Kotlin coroutine-based ThreePhaseServer.
 *
 * The coordinator flow reads linearly as:
 *
 *   val queryResponses = awaitQueryQuorum(queryId)
 *   val pending = findHighestPending(queryResponses)
 *   if (pending != null) { recover(pending); return }
 *   awaitAcceptQuorum(requestId, request)
 *   sendCommitToAll(requestId)
 *   val response = awaitLocalCommitExecution(requestId)
 *   sendClientResponse(response)
 *
 * No nesting, no callbacks, no state machine boilerplate.
 */
class ThreePhaseServerExampleTest {

    private val ATHENS = ProcessId.of("athens")
    private val BYZANTIUM = ProcessId.of("byzantium")
    private val CYRENE = ProcessId.of("cyrene")
    private val ALICE = ProcessId.of("alice")
    private val BOB = ProcessId.of("bob")
    private val COUNTER_KEY = "counter_key"

    @Test
    @DisplayName("Coroutines: Three-Phase Commit Happy Path (QUERY→ACCEPT→COMMIT)")
    fun testThreePhaseHappyPath() {
        Cluster()
            .withProcessIds(listOf(ATHENS, BYZANTIUM, CYRENE))
            .useSimulatedNetwork()
            .build<ThreePhaseServerExample> { peerIds, processParams ->
                ThreePhaseServerExample(peerIds, processParams)
            }
            .start().use { cluster ->
                val client = cluster.newClientConnectedTo<ThreePhaseClient>(ALICE, ATHENS) { peerIds, processParams ->
                    ThreePhaseClient(peerIds, processParams)
                }

                cluster.tickUntilComplete(client.execute(ATHENS,
                    IncrementCounterOperation(COUNTER_KEY, 5)))

                assertEquals(5, cluster.getNode<ThreePhaseServerExample>(ATHENS).getCounterValue(COUNTER_KEY))
                assertEquals(5, cluster.getNode<ThreePhaseServerExample>(BYZANTIUM).getCounterValue(COUNTER_KEY))
                assertEquals(5, cluster.getNode<ThreePhaseServerExample>(CYRENE).getCounterValue(COUNTER_KEY))
            }
    }

    @Test
    @DisplayName("Coroutines: Multiple sequential operations accumulate correctly")
    fun testMultipleOperations() {
        Cluster()
            .withProcessIds(listOf(ATHENS, BYZANTIUM, CYRENE))
            .useSimulatedNetwork()
            .build<ThreePhaseServerExample> { peerIds, processParams ->
                ThreePhaseServerExample(peerIds, processParams)
            }
            .start().use { cluster ->
                val client = cluster.newClientConnectedTo<ThreePhaseClient>(ALICE, ATHENS) { peerIds, processParams ->
                    ThreePhaseClient(peerIds, processParams)
                }

                cluster.tickUntilComplete(client.execute(ATHENS,
                    IncrementCounterOperation(COUNTER_KEY, 3)))

                cluster.tickUntilComplete(client.execute(ATHENS,
                    IncrementCounterOperation(COUNTER_KEY, 7)))

                assertEquals(10, cluster.getNode<ThreePhaseServerExample>(ATHENS).getCounterValue(COUNTER_KEY))
                assertEquals(10, cluster.getNode<ThreePhaseServerExample>(BYZANTIUM).getCounterValue(COUNTER_KEY))
                assertEquals(10, cluster.getNode<ThreePhaseServerExample>(CYRENE).getCounterValue(COUNTER_KEY))
            }
    }

    @Test
    @DisplayName("Coroutines: Coordinator failure recovery with orphaned prepared request")
    fun testCoordinatorFailureRecovery() {
        Cluster()
            .withProcessIds(listOf(ATHENS, BYZANTIUM, CYRENE))
            .useSimulatedNetwork()
            .build<ThreePhaseServerExample> { peerIds, processParams ->
                ThreePhaseServerExample(peerIds, processParams)
            }
            .start().use { cluster ->

                val aliceClient = cluster.newClientConnectedTo<ThreePhaseClient>(ALICE, ATHENS) { peerIds, processParams ->
                    ThreePhaseClient(peerIds, processParams)
                }

                // Drop COMMIT from Athens to followers
                cluster.dropMessagesOfType(
                    TwoPhaseMessageTypes.COMMIT_REQUEST,
                    NodeGroup.of(ATHENS),
                    NodeGroup.of(BYZANTIUM, CYRENE)
                )

                aliceClient.execute(ATHENS, IncrementCounterOperation(COUNTER_KEY, 10))
                cluster.tickUntil { hasCounterValue(cluster, ATHENS, 10) }

                assertNull(cluster.getNode<ThreePhaseServerExample>(BYZANTIUM).getCounterValue(COUNTER_KEY))
                assertNull(cluster.getNode<ThreePhaseServerExample>(CYRENE).getCounterValue(COUNTER_KEY))

                // Isolate Athens
                cluster.partitionNodes(
                    NodeGroup.of(ATHENS),
                    NodeGroup.of(BYZANTIUM, CYRENE)
                )

                // Bob sends to Byzantium — it should recover the orphaned request
                val bobClient = cluster.newClientConnectedTo<ThreePhaseClient>(BOB, BYZANTIUM) { peerIds, processParams ->
                    ThreePhaseClient(peerIds, processParams)
                }
                bobClient.execute(BYZANTIUM, IncrementCounterOperation(COUNTER_KEY, 20))

                cluster.tickUntil {
                    hasCounterValue(cluster, BYZANTIUM, 10) &&
                    hasCounterValue(cluster, CYRENE, 10)
                }
            }
    }

    private fun hasCounterValue(cluster: Cluster, nodeId: ProcessId, expectedValue: Int): Boolean {
        val node = cluster.getNode<ThreePhaseServerExample>(nodeId)
        return Integer.valueOf(expectedValue) == node.getCounterValue(COUNTER_KEY)
    }
}
