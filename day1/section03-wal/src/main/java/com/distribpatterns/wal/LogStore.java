package com.distribpatterns.wal;

import com.tickloom.future.ListenableFuture;
import com.tickloom.storage.Storage;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An example implementation of storing multiple 'logs' in a single Storage.
 */
public class LogStore {
    private static final byte[] RFTL = "rftl".getBytes(StandardCharsets.US_ASCII);
    private static final long INITIAL_INDEX = 0L;

    private final Storage storage;
    private final Map<String, Long> lastLogIndexes;
    public boolean isInitialised = false;

    public LogStore(List<String> logIds, Storage storage) {
        this.storage = Objects.requireNonNull(storage, "Storage cannot be null");
        this.lastLogIndexes = new HashMap<>(logIds.size());
        initializeLogIndices(logIds);
    }

    private void initializeLogIndices(List<String> logIds) {
        logIds.forEach(logId -> {
            lastLogIndexes.put(logId, INITIAL_INDEX);
            fetchLastLogIndex(logId, logIds);
        });
    }

    private void fetchLastLogIndex(String logId, List<String> allLogIds) {
        ListenableFuture<byte[]> lastIndexFuture = storage.lowerKey(createLogKey(logId, Long.MAX_VALUE));
        lastIndexFuture.andThen((result, error) -> handleIndexResult(logId, allLogIds, result, error));
    }

    private void handleIndexResult(String logId, List<String> allLogIds, byte[] result, Throwable error) {
        if (error != null) {
            return; // Error handling could be improved here
        }

        long index = result != null ? getIndex(result) : INITIAL_INDEX;
        updateLogIndex(logId, index, allLogIds);
    }

    private void updateLogIndex(String logId, long index, List<String> allLogIds) {
        lastLogIndexes.put(logId, index);
        maybeMarkInitialised(allLogIds);
    }

    private void maybeMarkInitialised(List<String> allLogIds) {
        if (lastLogIndexes.size() == allLogIds.size()) {
            isInitialised = true;
        }
    }

    public ListenableFuture<Boolean> append(String logId, byte[] entry) {
        if (!isInitialised) {
            throw new IllegalStateException("The LogStore is not initialised");
        }
        long nextIndex = getAndIncrementIndex(logId);
        return storage.put(createLogKey(logId, nextIndex), entry);
    }

    private long getAndIncrementIndex(String logId) {
        return lastLogIndexes.compute(logId, (k, v) -> v + 1);
    }

    public byte[] createLogKey(String logId, long logIndex) {
        return ByteBuffer.allocate(logId.length() + RFTL.length + Long.BYTES)
                .put(logId.getBytes())
                .put(RFTL)
                .putLong(logIndex)
                .array();
    }

    public long getIndex(byte[] key) {
        return ByteBuffer.wrap(key).getLong(key.length - Long.BYTES);
    }

    public long getLastLogIndex(String logId) {
        return lastLogIndexes.getOrDefault(logId, INITIAL_INDEX);
    }

    public boolean isInitialised() {
        return isInitialised;
    }
}

