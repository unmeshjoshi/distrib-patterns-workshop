package com.distribpatterns.paxoslog.dsl;

import com.distribpatterns.paxoslog.PaxosLogClient;
import com.distribpatterns.paxoslog.messages.ExecuteCommandResponse;
import com.distribpatterns.paxoslog.messages.GetValueResponse;
import com.distribpatterns.paxoslog.messages.Operation;
import com.tickloom.ProcessId;
import com.tickloom.history.Op;
import com.tickloom.testkit.dsl.EventOrAwaitScope;
import com.tickloom.testkit.dsl.StepBuilder;
import com.tickloom.testkit.dsl.semanticmodel.Action;
import com.tickloom.testkit.dsl.semanticmodel.ClusterEvent;

public class PaxosLogStepBuilder
        extends StepBuilder<PaxosLogClient, PaxosLogActionScope, PaxosLogSetupScope>
        implements PaxosLogActionScope, PaxosLogSetupScope {

    @Override
    protected PaxosLogActionScope actionBuilder() {
        return this;
    }

    @Override
    protected PaxosLogSetupScope setupBuilder() {
        return this;
    }

    // ---- Action verbs ----

    @Override
    public EventOrAwaitScope<PaxosLogActionScope, ExecuteCommandResponse> execute(ProcessId coordinator, Operation operation) {
        ProcessId clientId = currentClientId();
        Action<PaxosLogClient, ExecuteCommandResponse> action = (clients, cluster, recorder) ->
                recorder.invoke(clientId, new Op("execute"), operation.toString(),
                                () -> clients.get(clientId).execute(coordinator, operation),
                                response -> response.toString())
                        .thenApply(response -> response);
        return beginStep(action);
    }

    @Override
    public EventOrAwaitScope<PaxosLogActionScope, GetValueResponse> getValue(ProcessId coordinator, String key) {
        ProcessId clientId = currentClientId();
        Action<PaxosLogClient, GetValueResponse> action = (clients, cluster, recorder) ->
                recorder.invoke(clientId, Op.READ, key,
                                () -> clients.get(clientId).getValue(coordinator, key),
                                response -> response.toString())
                        .thenApply(response -> response);
        return beginStep(action);
    }

    // ---- Setup verbs ----

    @Override
    public PaxosLogSetupScope apply(ClusterEvent event) {
        addGiven(event);
        return this;
    }
}
