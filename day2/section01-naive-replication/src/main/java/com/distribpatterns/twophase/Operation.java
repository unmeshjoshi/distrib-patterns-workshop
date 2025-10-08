package com.distribpatterns.twophase;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Sealed interface for state machine operations.
 * Operations are executed through consensus (Two-Phase Commit).
 * 
 * Jackson annotations enable polymorphic serialization/deserialization.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = IncrementCounterOperation.class, name = "increment")
})
public sealed interface Operation permits IncrementCounterOperation {
}

