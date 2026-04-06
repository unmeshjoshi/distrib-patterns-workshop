package com.distribpatterns.twophase.messages;

import com.distribpatterns.twophase.Operation;

/**
 * Client request to execute an operation through Two-Phase Commit.
 * The operation will be executed only after quorum acceptance and commit.
 */
public record ExecuteRequest(Operation operation) {
}
