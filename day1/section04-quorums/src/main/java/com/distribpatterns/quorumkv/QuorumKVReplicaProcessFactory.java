package com.distribpatterns.quorumkv;

import com.tickloom.Process;
import com.tickloom.ProcessFactory;
import com.tickloom.ProcessId;
import com.tickloom.messaging.MessageBus;
import com.tickloom.network.MessageCodec;
import com.tickloom.storage.Storage;
import com.tickloom.util.Clock;

import java.util.List;

public class QuorumKVReplicaProcessFactory implements ProcessFactory {
    @Override
    public Process create(ProcessId id, List<ProcessId> peerIds, MessageBus messageBus, MessageCodec messageCodec, Storage storage, Clock clock, int timeoutTicks) {
        return new QuorumKVReplica(id, peerIds, messageBus, messageCodec, storage, clock, timeoutTicks);
    }
}

