package com.distribpatterns.paxoslog.dsl;

import com.tickloom.testkit.dsl.SetupScope;
import com.tickloom.testkit.dsl.semanticmodel.ClusterEvent;

/**
 * PaxosLog has no protocol-specific initial conditions (no HLC, no log seed
 * verbs). Only the generic {@link #apply(ClusterEvent)} escape hatch is
 * exposed, narrowed to {@link PaxosLogSetupScope} so the fluent chain keeps
 * returning to this scope.
 */
public interface PaxosLogSetupScope extends SetupScope {
    @Override
    PaxosLogSetupScope apply(ClusterEvent event);
}
