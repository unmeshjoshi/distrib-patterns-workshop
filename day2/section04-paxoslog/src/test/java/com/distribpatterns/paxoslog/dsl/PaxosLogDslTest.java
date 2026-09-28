package com.distribpatterns.paxoslog.dsl;

import com.distribpatterns.paxoslog.PaxosLogClient;
import com.distribpatterns.paxoslog.PaxosLogServer;
import com.distribpatterns.paxoslog.messages.CompareAndSwapOperation;
import com.distribpatterns.paxoslog.messages.SetValueOperation;
import com.tickloom.ProcessId;
import com.tickloom.testkit.dsl.semanticmodel.Scenario;
import com.tickloom.testkit.dsl.semanticmodel.ScenarioResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PaxosLog test ported to the tickloom DSL form.
 *
 * <p>Mirrors the imperative scenarios in {@code PaxosLogTest} but expresses
 * each one via {@code servers / clients / steps} and reads end-of-run state
 * through {@link ScenarioResult#snapshot()}.
 */
public class PaxosLogDslTest {

    private static final ProcessId ATHENS = ProcessId.of("athens");
    private static final ProcessId BYZANTIUM = ProcessId.of("byzantium");
    private static final ProcessId CYRENE = ProcessId.of("cyrene");
    private static final ProcessId CLIENT = ProcessId.of("client");

    private static final String TITLE_KEY = "title";
    private static final String AUTHOR_KEY = "author";

    @Test
    @DisplayName("Single SetValue command commits at index 0 and replicates")
    void basicSetValue() throws Exception {
        Scenario<PaxosLogClient> s = PaxosLogScenarios.scenario("basic set value")
                .servers(ATHENS, BYZANTIUM, CYRENE)
                .clients(CLIENT)
                .steps(steps -> {
                    steps.client(CLIENT)
                         .execute(ATHENS, new SetValueOperation(TITLE_KEY, "Microservices"))
                         .expectResponse(r -> r.success() && "Microservices".equals(r.result()));
                });

        ScenarioResult result = s.run();

        assertValueOnAllReplicas(result, TITLE_KEY, "Microservices");
        assertHighWaterMarkOnAllReplicas(result, 0);
    }

    @Test
    @DisplayName("Sequential commands fill consecutive log indices")
    void multipleCommands() throws Exception {
        Scenario<PaxosLogClient> s = PaxosLogScenarios.scenario("multiple commands")
                .servers(ATHENS, BYZANTIUM, CYRENE)
                .clients(CLIENT)
                .steps(steps -> {
                    steps.client(CLIENT)
                         .execute(ATHENS, new SetValueOperation(TITLE_KEY, "Microservices"))
                         .expectResponse(r -> r.success());

                    steps.client(CLIENT)
                         .execute(BYZANTIUM, new SetValueOperation(AUTHOR_KEY, "Martin"))
                         .expectResponse(r -> r.success());

                    steps.client(CLIENT)
                         .execute(BYZANTIUM, new CompareAndSwapOperation(AUTHOR_KEY, "Martin", "Martin Fowler"))
                         .expectResponse(r -> r.success());
                });

        ScenarioResult result = s.run();

        PaxosLogServer athens = result.snapshot().node(ATHENS);
        assertEquals("Microservices", athens.getValue(TITLE_KEY));
        assertEquals("Martin Fowler", athens.getValue(AUTHOR_KEY));
        assertHighWaterMarkOnAllReplicas(result, 2);
    }

    @Test
    @DisplayName("Get value reads through Paxos via a no-op log entry")
    void getValueAfterSet() throws Exception {
        Scenario<PaxosLogClient> s = PaxosLogScenarios.scenario("get value after set")
                .servers(ATHENS, BYZANTIUM, CYRENE)
                .clients(CLIENT)
                .steps(steps -> {
                    steps.client(CLIENT)
                         .execute(ATHENS, new SetValueOperation(AUTHOR_KEY, "Martin"))
                         .expectResponse(r -> r.success());

                    steps.client(CLIENT)
                         .getValue(BYZANTIUM, AUTHOR_KEY)
                         .expectResponse(r -> "Martin".equals(r.value()));
                });

        s.run();
    }

    @Test
    @DisplayName("All replicas share the same committed log")
    void logConsistency() throws Exception {
        Scenario<PaxosLogClient> s = PaxosLogScenarios.scenario("log consistency")
                .servers(ATHENS, BYZANTIUM, CYRENE)
                .clients(CLIENT)
                .steps(steps -> {
                    for (int i = 0; i < 3; i++) {
                        steps.client(CLIENT)
                             .execute(ATHENS, new SetValueOperation("key" + i, "value" + i))
                             .expectResponse(r -> r.success());
                    }
                });

        ScenarioResult result = s.run();

        PaxosLogServer athens = result.snapshot().node(ATHENS);
        PaxosLogServer byzantium = result.snapshot().node(BYZANTIUM);
        PaxosLogServer cyrene = result.snapshot().node(CYRENE);

        int hwm = athens.getHighWaterMark();
        assertEquals(2, hwm, "Three entries should commit at indices 0, 1, 2");
        assertEquals(hwm, byzantium.getHighWaterMark());
        assertEquals(hwm, cyrene.getHighWaterMark());

        for (int i = 0; i <= hwm; i++) {
            assertCommittedAt(athens, i);
            assertCommittedAt(byzantium, i);
            assertCommittedAt(cyrene, i);
        }
    }

    private static void assertValueOnAllReplicas(ScenarioResult result, String key, String expected) {
        for (ProcessId id : List.of(ATHENS, BYZANTIUM, CYRENE)) {
            PaxosLogServer node = result.snapshot().node(id);
            assertEquals(expected, node.getValue(key), "value for " + key + " on " + id);
        }
    }

    private static void assertHighWaterMarkOnAllReplicas(ScenarioResult result, int expected) {
        for (ProcessId id : List.of(ATHENS, BYZANTIUM, CYRENE)) {
            PaxosLogServer node = result.snapshot().node(id);
            assertEquals(expected, node.getHighWaterMark(), "HWM on " + id);
        }
    }

    private static void assertCommittedAt(PaxosLogServer node, int index) {
        var entry = node.getLogEntry(index);
        assertNotNull(entry, "expected entry at index " + index);
        assertTrue(entry.committedValue().isPresent(), "expected committed value at index " + index);
    }
}
