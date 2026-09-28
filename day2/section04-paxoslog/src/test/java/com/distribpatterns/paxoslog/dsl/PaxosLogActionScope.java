package com.distribpatterns.paxoslog.dsl;

import com.distribpatterns.paxoslog.messages.ExecuteCommandResponse;
import com.distribpatterns.paxoslog.messages.GetValueResponse;
import com.distribpatterns.paxoslog.messages.Operation;
import com.tickloom.ProcessId;
import com.tickloom.testkit.dsl.ActionScope;
import com.tickloom.testkit.dsl.EventOrAwaitScope;

public interface PaxosLogActionScope extends ActionScope {
    EventOrAwaitScope<PaxosLogActionScope, ExecuteCommandResponse> execute(ProcessId coordinator, Operation operation);
    EventOrAwaitScope<PaxosLogActionScope, GetValueResponse> getValue(ProcessId coordinator, String key);
}
