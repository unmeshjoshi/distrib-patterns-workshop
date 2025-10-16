package com.distribpatterns.wal;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

/**
 * CockroachDB
 *https://github.com/cockroachdb/cockroach/blob/9df329537c7b33db43b276a27d930d5c88697b2d/pkg/kv/kvserver/logstore/logstore.go#L400
 *https://github.com/cockroachdb/cockroach/blob/9df329537c7b33db43b276a27d930d5c88697b2d/pkg/keys/keys.go#L444
 *
 * Dragonboat
 * http://github.com/lni/dragonboat/blob/076c7f6497dcc18880aed6323246d5079661942c/internal/logdb/db.go#L487
 * https://github.com/lni/dragonboat/blob/076c7f6497dcc18880aed6323246d5079661942c/internal/logdb/plain.go#L39
 * https://github.com/lni/dragonboat/blob/076c7f6497dcc18880aed6323246d5079661942c/internal/logdb/key.go#L105
 */
public class RocksWAL {
    RocksDB rocksDB;
    static byte[] WAL_PREFIX = "wal1".getBytes(StandardCharsets.UTF_8);

    public RocksWAL(Path path) throws RocksDBException {
        rocksDB = RocksDB.open(path.toAbsolutePath().toString());
    }

    public byte[] lastKey() {
        RocksIterator rocksIterator = rocksDB.newIterator();
        rocksIterator.seekToLast();
        return rocksIterator.key();
    }

    public void append(long logEntryIndex, byte[] data, EntryType entryType, int generation) throws RocksDBException {
        var logEntry = new WALEntry(logEntryIndex, data, entryType, generation);

        ByteBuffer keyBuffer = ByteBuffer.allocate(WAL_PREFIX.length + Long.BYTES)
                .order(ByteOrder.BIG_ENDIAN);
        keyBuffer.put(WAL_PREFIX);
        keyBuffer.putLong(logEntryIndex);

        rocksDB.put(keyBuffer.array(),  logEntry.serialize().array());
    }
}
