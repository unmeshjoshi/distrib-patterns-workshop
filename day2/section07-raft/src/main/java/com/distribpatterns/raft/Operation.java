package com.distribpatterns.raft;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * State machine operation sealed interface.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = SetValueOperation.class, name = "set"),
    @JsonSubTypes.Type(value = NoOpOperation.class, name = "noop")
})
public sealed interface Operation permits SetValueOperation, NoOpOperation {
}


