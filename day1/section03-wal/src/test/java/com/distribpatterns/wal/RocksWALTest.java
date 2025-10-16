package com.distribpatterns.wal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import static com.distribpatterns.wal.RocksWAL.WAL_PREFIX;
import static org.junit.Assert.assertEquals;

class RocksWALTest {

    @Test
    public void writesIndexedEntriesToRocksDb(@TempDir Path tempDir) throws IOException, RocksDBException {
        Path walDir = tempDir.resolve("wal");
        walDir.toFile().mkdir();
        RocksWAL wal = new RocksWAL(walDir);
        byte[] bytes = wal.lastKey();
        long logEntryIndex = 1;
        wal.append(logEntryIndex++, new byte[]{},
                EntryType.DATA, 0);

        wal.append(logEntryIndex++, new byte[]{},
                EntryType.DATA, 0);

        wal.append(logEntryIndex++, new byte[]{},
                EntryType.DATA, 0);

        bytes = wal.lastKey();
        ByteBuffer keyBuffer = ByteBuffer.wrap(bytes);
        byte[] prefixBuffer = new byte[4];
        keyBuffer.get(prefixBuffer);

        assertEquals("wal1", new String(prefixBuffer));
        assertEquals(keyBuffer.getLong(WAL_PREFIX.length), 3L);
    }

}