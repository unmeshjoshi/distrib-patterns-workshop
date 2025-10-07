package com.distribpatterns.wal;

import com.distribpatterns.common.Config;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * A realistic Write-Ahead Log with three key optimizations: Based on BookKeeper Journal implementation.
 * 1. Buffered writes - Reduce system calls by batching in memory
 * 2. Group commit - Amortize fsync cost across multiple entries
 * 3. Pre-allocation - Avoid filesystem fragmentation
 */
public class OptimizedWriteAheadLog {
    private static final int LENGTH_PREFIX_BYTES = 4;
    private static final int LOW_BUFFER_THRESHOLD = 1024;
    private static final int MAX_ENTRY_SIZE = 10 * 1024 * 1024; // 10MB sanity limit

    private final File walFile;
    private final FileChannel fileChannel;

    // Buffered writes
    private final ByteBuffer writeBuffer;
    private final int bufferSize;
    private long currentPosition = 0;

    // Group commit
    private final int groupCommitBatchSize;
    private final long groupCommitMaxWaitMs;
    private long lastFlushTimeMs;
    private int pendingWriteCount = 0;

    // Pre-allocation
    private final long preAllocChunkSize;
    private long nextPreAllocPosition;

    // Metrics
    private long totalWrites = 0;
    private long totalFsyncs = 0;

    public OptimizedWriteAheadLog(Config config) throws IOException {
        this(config.getWalDir(), 64 * 1024, 100, 10, 16 * 1024 * 1024);
    }

    public OptimizedWriteAheadLog(File walDir, int bufferSize, int batchSize,
                                  long maxWaitMs, long preAllocSize) throws IOException {
        ensureDirectoryExists(walDir);

        this.walFile = new File(walDir, "optimized-wal.log");
        RandomAccessFile raf = new RandomAccessFile(walFile, "rw");
        this.fileChannel = raf.getChannel();

        this.bufferSize = bufferSize;
        this.writeBuffer = ByteBuffer.allocateDirect(bufferSize);

        this.groupCommitBatchSize = batchSize;
        this.groupCommitMaxWaitMs = maxWaitMs;
        this.lastFlushTimeMs = System.currentTimeMillis();

        this.preAllocChunkSize = preAllocSize;
        this.nextPreAllocPosition = preAllocSize;
        preAllocateNextChunk();

        logInitialization();
    }

    /**
     * Append an entry to the WAL.
     *
     * Note: Because entries are buffered in memory before being flushed to disk,
     * this method could return a ListenableFuture<Long> that completes when the
     * entry is actually fsynced to durable storage. This would allow callers to:
     * - Continue processing without blocking on disk I/O
     * - Be notified when their specific write is durable
     * - Handle fsync failures asynchronously
     *
     * Current implementation returns immediately with the entry position, but the
     * entry is not guaranteed to be durable until flush() completes.
     * Apache Bookeeper guarantees that once the response is returned, the entry is flushed.
     *
     * @param data The entry data to append
     * @return The position of this entry in the log
     * @throws IOException If an I/O error occurs
     */
    public synchronized long append(byte[] data) throws IOException {
        long entryPosition = currentPosition;

        ensurePreAllocatedSpace(LENGTH_PREFIX_BYTES + data.length);
        writeEntryToBuffer(data);

        pendingWriteCount++;
        totalWrites++;

        if (shouldFlush()) {
            flush();
        }

        return entryPosition;
    }

    public synchronized void flush() throws IOException {
        if (nothingToFlush()) {
            return;
        }

        flushBufferToChannel();
        fsyncToStorage();

        logFlushSuccess();
        pendingWriteCount = 0;
        lastFlushTimeMs = System.currentTimeMillis();
    }

    public List<byte[]> readAll() throws IOException {
        flush();
        return readEntriesFromFile();
    }

    public Stats getStats() {
        long fsyncReduction = totalFsyncs > 0 ? totalWrites / totalFsyncs : 0;
        return new Stats(totalWrites, totalFsyncs, currentPosition, walFile.length(), fsyncReduction);
    }

    public void close() throws IOException {
        flush();
        fileChannel.close();
    }

    // Private helpers - Write operations

    private void writeEntryToBuffer(byte[] data) throws IOException {
        writeLengthPrefix(data.length);
        writeDataBytes(data);
    }

    private void writeLengthPrefix(int length) throws IOException {
        ensureBufferSpace(LENGTH_PREFIX_BYTES);
        writeBuffer.putInt(length);
        currentPosition += LENGTH_PREFIX_BYTES;
    }

    private void writeDataBytes(byte[] data) throws IOException {
        int offset = 0;
        while (offset < data.length) {
            ensureBufferSpace(1);
            int toWrite = Math.min(data.length - offset, writeBuffer.remaining());
            writeBuffer.put(data, offset, toWrite);
            offset += toWrite;
            currentPosition += toWrite;
        }
    }

    private void ensureBufferSpace(int requiredBytes) throws IOException {
        if (writeBuffer.remaining() < requiredBytes) {
            flushBufferToChannel();
        }
    }

    private void flushBufferToChannel() throws IOException {
        if (writeBuffer.position() == 0) {
            return;
        }

        writeBuffer.flip();
        while (writeBuffer.hasRemaining()) {
            fileChannel.write(writeBuffer);
        }
        writeBuffer.clear();
    }

    // Private helpers - Group commit

    private boolean shouldFlush() {
        return batchSizeReached() || maxWaitTimeExceeded() || bufferAlmostFull();
    }

    private boolean batchSizeReached() {
        return pendingWriteCount >= groupCommitBatchSize;
    }

    private boolean maxWaitTimeExceeded() {
        return (System.currentTimeMillis() - lastFlushTimeMs) >= groupCommitMaxWaitMs;
    }

    private boolean bufferAlmostFull() {
        return writeBuffer.remaining() < LOW_BUFFER_THRESHOLD;
    }

    private boolean nothingToFlush() {
        return writeBuffer.position() == 0 && pendingWriteCount == 0;
    }

    private void fsyncToStorage() throws IOException {
        fileChannel.force(false);  // Sync data only, not metadata
        totalFsyncs++;
    }

    private void logFlushSuccess() {
        if (pendingWriteCount > 0) {
            System.out.println("Flushed " + pendingWriteCount + " entries with 1 fsync " +
                    "(efficiency: " + pendingWriteCount + "× reduction)");
        }
    }

    // Private helpers - Pre-allocation

    private void ensurePreAllocatedSpace(int requiredBytes) throws IOException {
        if (currentPosition + requiredBytes > nextPreAllocPosition) {
            preAllocateNextChunk();
        }
    }

    private void preAllocateNextChunk() throws IOException {
        long savedPosition = fileChannel.position();

        // Extend file by writing one byte at chunk boundary
        ByteBuffer oneByte = ByteBuffer.allocate(1);
        oneByte.put((byte) 0);
        oneByte.flip();
        fileChannel.write(oneByte, nextPreAllocPosition - 1);

        logPreAllocation();
        nextPreAllocPosition += preAllocChunkSize;
        fileChannel.position(savedPosition);
    }

    private void logPreAllocation() {
        long chunkMB = preAllocChunkSize / 1024 / 1024;
        long totalMB = nextPreAllocPosition / 1024 / 1024;
        System.out.println("Pre-allocated " + chunkMB + "MB (file size: " + totalMB + "MB)");
    }

    // Private helpers - Reading

    private List<byte[]> readEntriesFromFile() throws IOException {
        List<byte[]> entries = new ArrayList<>();

        try (FileChannel readChannel = new RandomAccessFile(walFile, "r").getChannel()) {
            long position = 0;
            ByteBuffer lengthBuffer = ByteBuffer.allocate(LENGTH_PREFIX_BYTES);

            while (position < currentPosition) {
                byte[] entry = readSingleEntry(readChannel, lengthBuffer, position);
                if (entry == null) {
                    break;  // Incomplete or corrupt entry
                }
                entries.add(entry);
                position += LENGTH_PREFIX_BYTES + entry.length;
            }
        }

        return entries;
    }

    private byte[] readSingleEntry(FileChannel channel, ByteBuffer lengthBuffer, long position)
            throws IOException {
        int length = readEntryLength(channel, lengthBuffer, position);
        if (length < 0 || !isValidLength(length)) {
            return null;
        }

        return readEntryData(channel, position + LENGTH_PREFIX_BYTES, length);
    }

    private int readEntryLength(FileChannel channel, ByteBuffer lengthBuffer, long position)
            throws IOException {
        lengthBuffer.clear();
        int bytesRead = channel.read(lengthBuffer, position);

        if (bytesRead < LENGTH_PREFIX_BYTES) {
            return -1;
        }

        lengthBuffer.flip();
        return lengthBuffer.getInt();
    }

    private byte[] readEntryData(FileChannel channel, long position, int length)
            throws IOException {
        ByteBuffer dataBuffer = ByteBuffer.allocate(length);
        int bytesRead = channel.read(dataBuffer, position);

        if (bytesRead != length) {
            return null;  // Incomplete read
        }

        dataBuffer.flip();
        byte[] data = new byte[length];
        dataBuffer.get(data);
        return data;
    }

    private boolean isValidLength(int length) {
        return length > 0 && length <= MAX_ENTRY_SIZE;
    }

    // Private helpers - Initialization

    private static void ensureDirectoryExists(File directory) {
        if (!directory.exists()) {
            directory.mkdirs();
        }
    }

    private void logInitialization() {
        System.out.println("OptimizedWAL initialized:");
        System.out.println("  - Write buffer: " + bufferSize + " bytes");
        System.out.println("  - Group commit batch size: " + groupCommitBatchSize);
        System.out.println("  - Group commit max wait: " + groupCommitMaxWaitMs + "ms");
        System.out.println("  - Pre-allocation size: " + (preAllocChunkSize / 1024 / 1024) + "MB");
    }

    // Stats class
    public static class Stats {
        public final long totalWrites;
        public final long totalFsyncs;
        public final long bytesWritten;
        public final long fileSize;
        public final long fsyncReduction;

        Stats(long totalWrites, long totalFsyncs, long bytesWritten, long fileSize, long fsyncReduction) {
            this.totalWrites = totalWrites;
            this.totalFsyncs = totalFsyncs;
            this.bytesWritten = bytesWritten;
            this.fileSize = fileSize;
            this.fsyncReduction = fsyncReduction;
        }

        @Override
        public String toString() {
            return String.format("WAL Stats: writes=%d, fsyncs=%d, bytes=%d, fileSize=%d, efficiency=%dx",
                    totalWrites, totalFsyncs, bytesWritten, fileSize, fsyncReduction);
        }
    }
}