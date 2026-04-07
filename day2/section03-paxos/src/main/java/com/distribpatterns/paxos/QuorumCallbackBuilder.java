package com.distribpatterns.paxos;

import com.tickloom.ProcessId;
import com.tickloom.messaging.AsyncQuorumCallback;

import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class QuorumCallbackBuilder<T> {
    private final int totalResponses;
    private Predicate<T> successCondition = response -> true;
    private Consumer<Map<ProcessId, T>> onSuccess = responses -> { };
    private Consumer<Throwable> onFailure = error -> { };

    private QuorumCallbackBuilder(int totalResponses) {
        this.totalResponses = totalResponses;
    }

    public static <T> QuorumCallbackBuilder<T> forTotalResponses(int totalResponses) {
        return new QuorumCallbackBuilder<>(totalResponses);
    }

    public QuorumCallbackBuilder<T> succeedsWhen(Predicate<T> successCondition) {
        this.successCondition = Objects.requireNonNull(successCondition);
        return this;
    }

    public QuorumCallbackBuilder<T> onSuccess(Consumer<Map<ProcessId, T>> onSuccess) {
        this.onSuccess = Objects.requireNonNull(onSuccess);
        return this;
    }

    public QuorumCallbackBuilder<T> onFailure(Consumer<Throwable> onFailure) {
        this.onFailure = Objects.requireNonNull(onFailure);
        return this;
    }

    public AsyncQuorumCallback<T> build() {
        return new AsyncQuorumCallback<>(totalResponses, successCondition)
                .onSuccess(onSuccess)
                .onFailure(onFailure);
    }
}
