package com.distribpatterns.multipaxosheartbeats;

/**
 * Phase 1: Leader proposes value at log index.
 * No Prepare phase needed (already done during election).
 */
public record ProposeRequest(int logIndex, int generation, Operation value) {
}

