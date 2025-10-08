package com.distribpatterns.paxos;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Generic operation interface for state machine commands.
 * 
 * Jackson annotations enable polymorphic JSON serialization/deserialization
 * without manual serialize/deserialize methods.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = IncrementCounterOperation.class, name = "increment"),
    @JsonSubTypes.Type(value = SetValueOperation.class, name = "set")
})
public sealed interface Operation 
    permits IncrementCounterOperation, SetValueOperation {
}

