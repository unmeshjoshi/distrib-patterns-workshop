package com.distribpatterns.twophase;

/**
 * Client request to execute an operation through Two-Phase Commit.
 * The operation will be executed only after quorum acceptance and commit.
 */
public record ExecuteRequest(Operation operation) {
}

