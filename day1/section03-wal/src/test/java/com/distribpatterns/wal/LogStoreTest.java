package com.distribpatterns.wal;

import com.tickloom.future.ListenableFuture;
import com.tickloom.storage.SimulatedStorage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class LogStoreTest {

    @Test
    public void testLogEntryKey() {
        // Create simulated storage
        Random random = new Random(12345L);
        SimulatedStorage storage = new SimulatedStorage(random);

        // Create LogStore with multiple log IDs (simulating different shards/key-ranges)
        List<String> logIds = List.of("shard1", "shard2", "shard3");
        LogStore logStore = new LogStore(logIds, storage);

        // Test key structure and ordering
        byte[] shard1Key1 = logStore.createLogKey("shard1", 1L);
        byte[] shard1Key2 = logStore.createLogKey("shard1", 2L);
        byte[] shard2Key1 = logStore.createLogKey("shard2", 1L);

        byte[] rftl = new byte[4];
        System.arraycopy(shard1Key1, "shard1".length(), rftl, 0, 4);
        assertArrayEquals("rftl".getBytes(), rftl, "Key should contain RFTL marker");

        long index1 = ByteBuffer.wrap(shard1Key1).getLong(shard1Key1.length - Long.BYTES);
        assertEquals(1L, index1, "Key should end with correct index");

        // Verify lexicographic ordering (big-endian ensures numeric order)
        assertTrue(Arrays.compare(shard1Key1, shard1Key2) < 0, "Keys should be in lexicographic order");
        assertTrue(Arrays.compare(shard1Key2, shard2Key1) < 0, "Different shards should be ordered");

        System.out.println("\n=== Test passed: LogStore Multi-tenant ===\n");
    }

    @Test
    @DisplayName("LogStore: Multi-tenant log storage with prefixes")
    public void testLogAppendWithLogId() {
        System.out.println("\n=== TEST: LogStore Multi-tenant ===\n");

        // Create simulated storage
        Random random = new Random(12345L);
        SimulatedStorage storage = new SimulatedStorage(random);

        // Create LogStore with multiple log IDs (simulating different shards/key-ranges)
        List<String> logIds = List.of("shard1", "shard2", "shard3");
        LogStore logStore = new LogStore(logIds, storage);

        // Wait for initialization
        while (!logStore.isInitialised()) {
            storage.tick();
        }

        // Test appending to different logs
        ListenableFuture<Boolean> shard1Append1 = logStore.append("shard1", "entry1".getBytes());
        ListenableFuture<Boolean> shard1Append2 = logStore.append("shard1", "entry2".getBytes());
        ListenableFuture<Boolean> shard2Append1 = logStore.append("shard2", "entry3".getBytes());
        ListenableFuture<Boolean> shard3Append1 = logStore.append("shard3", "entry4".getBytes());

        storage.tick();

        // Verify all appends succeeded
        assertTrue(shard1Append1.getResult(), "Shard1 append1 should succeed");
        assertTrue(shard1Append2.getResult(), "Shard1 append2 should succeed");
        assertTrue(shard2Append1.getResult(), "Shard2 append1 should succeed");
        assertTrue(shard3Append1.getResult(), "Shard3 append1 should succeed");
    }

    @Test
    @DisplayName("LogStore: Initialization with existing logs using lowerKey")
    public void testLogEntriesWithMultipleLogIds() {
        System.out.println("\n=== TEST: LogStore Initialization with Existing Logs ===\n");

        Random random = new Random(123L);
        SimulatedStorage storage = new SimulatedStorage(random);

        List<String> logIds = List.of("shardA", "shardB", "shardC");

        // Initialize the actual LogStore
        LogStore logStore = new LogStore(logIds, storage);
        // Wait for initialization
        while (!logStore.isInitialised()) {
            storage.tick();
        }

        logStore.append("shardA", "entry1".getBytes());
        logStore.append("shardA", "entry2".getBytes());
        logStore.append("shardA", "entry3".getBytes());
        logStore.append("shardA", "entry4".getBytes());

        logStore.append("shardB", "entry1".getBytes());
        logStore.append("shardB", "entry2".getBytes());


        logStore.append("shardC", "entry1".getBytes());
        logStore.append("shardC", "entry2".getBytes());
        logStore.append("shardC", "entry3".getBytes());
        logStore.append("shardC", "entry4".getBytes());
        logStore.append("shardC", "entry5".getBytes());
        logStore.append("shardC", "entry6".getBytes());

        // Verify lastLogIndexes are correctly retrieved using lowerKey
        assertEquals(4L, logStore.getLastLogIndex("shardA"));
        assertEquals(2L, logStore.getLastLogIndex("shardB"));
        assertEquals(6L, logStore.getLastLogIndex("shardC"));

    }
}