package com.distribpatterns.wal;

import org.junit.jupiter.api.Test;
import com.distribpatterns.common.Config;
import com.distribpatterns.common.TestUtils;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DurableKVStoreTest {

    @Test
    public void shouldRecoverKVStoreStateFromWAL() {
        File walDir = TestUtils.tempDir("distrib/patterns/wal");
        DurableKVStore kv = new DurableKVStore(new Config(walDir.getAbsolutePath()));
        kv.put("title", "Microservices");

        // crash..
        // client got success;
        // client is sure that key1 is saved
        kv.put("author", "Martin");
        kv.put("newTitle", "Distributed Systems");

        // KV crashes.
        kv.close();

        // simulates process restart. A new instance is created at startup.
        DurableKVStore recoveredKvStore = new DurableKVStore(new Config(walDir.getAbsolutePath()));

        assertEquals("Microservices", recoveredKvStore.get("title"));
        assertEquals("Martin", recoveredKvStore.get("author"));
        assertEquals("Distributed Systems", recoveredKvStore.get("newTitle"));
    }
}
