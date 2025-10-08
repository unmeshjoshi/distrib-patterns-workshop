package com.distribpatterns.multipaxosheartbeats;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Sealed interface for log operations.
 * Each operation represents a command that can be replicated via PaxosLog.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = SetValueOperation.class, name = "set"),
    @JsonSubTypes.Type(value = CompareAndSwapOperation.class, name = "cas"),
    @JsonSubTypes.Type(value = NoOpOperation.class, name = "noop")
})
public sealed interface Operation permits SetValueOperation, CompareAndSwapOperation, NoOpOperation {
}
