package com.distribpatterns.paxoslog.dsl;

import com.distribpatterns.paxoslog.PaxosLogClient;
import com.distribpatterns.paxoslog.PaxosLogServer;
import com.tickloom.testkit.dsl.Scenarios;
import com.tickloom.testkit.dsl.ServerStep;

public final class PaxosLogScenarios {

    private PaxosLogScenarios() { }

    public static ServerStep<PaxosLogClient, PaxosLogActionScope, PaxosLogSetupScope> scenario(String name) {
        return Scenarios.scenario(
                name,
                (peerIds, processParams) -> new PaxosLogServer(peerIds, processParams),
                PaxosLogClient::new,
                PaxosLogStepBuilder::new
        );
    }
}
