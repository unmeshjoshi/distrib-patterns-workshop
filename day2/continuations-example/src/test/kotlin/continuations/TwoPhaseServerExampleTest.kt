package continuations

import com.distribpatterns.twophase.IncrementCounterOperation
import com.distribpatterns.twophase.TwoPhaseClient
import com.distribpatterns.twophase.messages.ExecuteResponse
import com.tickloom.ProcessId
import com.tickloom.testkit.Cluster
import com.tickloom.testkit.ClusterAssertions.assertEventually
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.io.IOException

class TwoPhaseServerExampleTest {

    // Replica nodes
    private val ATHENS = ProcessId.of("athens")
    private val BYZANTIUM = ProcessId.of("byzantium")
    private val CYRENE = ProcessId.of("cyrene")

    // Clients
    private val ALICE = ProcessId.of("alice")

    @Test
    @DisplayName("Scenario 1: Two-Phase Commit with Coroutines - client sees success after commit executes on coordinator")
    @Throws(IOException::class)
    fun testClientSeesSuccessAfterCommitExecutesOnCoordinator() {
        Cluster()
            .withProcessIds(listOf(ATHENS, BYZANTIUM, CYRENE))
            .useSimulatedNetwork()
            .build<TwoPhaseServerExample> { peerIds, processParams -> TwoPhaseServerExample(peerIds, processParams) }
            .start().use { cluster ->

                val client = cluster.newClientConnectedTo<TwoPhaseClient>(ALICE, ATHENS) { peerIds, processParams ->
                    TwoPhaseClient(peerIds, processParams)
                }

                val executeResponse = client.execute(ATHENS, IncrementCounterOperation("counter_key", 2))
                
                assertEventually(cluster) { executeResponse.isCompleted && !executeResponse.isFailed }

                val athensServer = cluster.getNode<TwoPhaseServerExample>(ATHENS)
                assertEquals(2, athensServer.getCounterValue("counter_key"))
            }
    }
}
