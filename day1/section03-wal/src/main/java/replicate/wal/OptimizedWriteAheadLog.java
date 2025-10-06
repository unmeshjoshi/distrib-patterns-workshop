package replicate.wal;

import replicate.common.Config;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Optimized Write-Ahead Log implementation showcasing production-grade optimizations:
 * 1. Buffered writes - Batch writes to reduce system calls
 * 2. Group commit - Amortize fsync cost across multiple entries
 * 3. Pre-allocation - Pre-allocate file space to avoid fragmentation
 *
 * Based on Apache BookKeeper Journal design patterns.
 */
public class OptimizedWriteAheadLog {
    private final File walFile;
    private final RandomAccessFile randomAccessFile;
    private final FileChannel fileChannel;
    
    // Optimization 1: Buffered writes
    private final ByteBuffer writeBuffer;
    private final int writeBufferSize;
    
    // Optimization 2: Group commit
    private final List<PendingWrite> pendingWrites;
    private final int groupCommitBatchSize;
    private long lastFlushTime;
    private final long groupCommitMaxWaitMs;
    
    // Optimization 3: Pre-allocation
    private final long preAllocSize;
    private long nextPreAllocPosition;
    
    // Metrics
    private long totalWrites = 0;
    private long totalFsyncs = 0;
    private long currentPosition = 0;
    
    public OptimizedWriteAheadLog(Config config) throws IOException {
        this(config.getWalDir(), 
             64 * 1024,           // 64KB write buffer
             100,                 // Batch up to 100 writes
             10,                  // Wait max 10ms for more writes
             16 * 1024 * 1024);   // Pre-allocate 16MB chunks
    }
    
    public OptimizedWriteAheadLog(File walDir, 
                                  int writeBufferSize,
                                  int groupCommitBatchSize,
                                  long groupCommitMaxWaitMs,
                                  long preAllocSize) throws IOException {
        if (!walDir.exists()) {
            walDir.mkdirs();
        }
        
        this.walFile = new File(walDir, "optimized-wal.log");
        this.randomAccessFile = new RandomAccessFile(walFile, "rw");
        this.fileChannel = randomAccessFile.getChannel();
        
        // Optimization 1: Buffered writes
        this.writeBufferSize = writeBufferSize;
        this.writeBuffer = ByteBuffer.allocateDirect(writeBufferSize);  // Direct buffer for performance
        
        // Optimization 2: Group commit
        this.pendingWrites = new ArrayList<>();
        this.groupCommitBatchSize = groupCommitBatchSize;
        this.groupCommitMaxWaitMs = groupCommitMaxWaitMs;
        this.lastFlushTime = System.currentTimeMillis();
        
        // Optimization 3: Pre-allocation
        this.preAllocSize = preAllocSize;
        this.nextPreAllocPosition = preAllocSize;
        preAllocate();  // Pre-allocate first chunk
        
        System.out.println("OptimizedWAL initialized:");
        System.out.println("  - Write buffer: " + writeBufferSize + " bytes");
        System.out.println("  - Group commit batch size: " + groupCommitBatchSize);
        System.out.println("  - Group commit max wait: " + groupCommitMaxWaitMs + "ms");
        System.out.println("  - Pre-allocation size: " + (preAllocSize / 1024 / 1024) + "MB");
    }
    
    /**
     * Append an entry to the WAL.
     * Uses buffered writes - entries are buffered in memory until flush.
     */
    public synchronized long append(byte[] data) throws IOException {
        long entryPosition = currentPosition;
        
        // Check if we need to pre-allocate more space
        preAllocIfNeeded(data.length + 4);  // 4 bytes for length prefix
        
        // Write length prefix
        if (writeBuffer.remaining() < 4) {
            flushBuffer();
        }
        writeBuffer.putInt(data.length);
        currentPosition += 4;
        
        // Write data
        int offset = 0;
        while (offset < data.length) {
            if (writeBuffer.remaining() == 0) {
                flushBuffer();
            }
            int toWrite = Math.min(data.length - offset, writeBuffer.remaining());
            writeBuffer.put(data, offset, toWrite);
            offset += toWrite;
            currentPosition += toWrite;
        }
        
        // Track pending write for group commit
        pendingWrites.add(new PendingWrite(entryPosition, data.length));
        totalWrites++;
        
        // Decide if we should flush now
        boolean shouldFlush = pendingWrites.size() >= groupCommitBatchSize
                           || (System.currentTimeMillis() - lastFlushTime) >= groupCommitMaxWaitMs
                           || writeBuffer.remaining() < 1024;  // Almost full
        
        if (shouldFlush) {
            flush();
        }
        
        return entryPosition;
    }
    
    /**
     * Flush buffered writes to disk with fsync (group commit).
     * All pending writes since last flush are persisted with a single fsync.
     */
    public synchronized void flush() throws IOException {
        if (writeBuffer.position() == 0 && pendingWrites.isEmpty()) {
            return;  // Nothing to flush
        }
        
        // Flush buffer to file
        flushBuffer();
        
        // Group commit: One fsync for all pending writes
        fileChannel.force(false);  // Sync data, not metadata
        totalFsyncs++;
        
        // Clear pending writes
        int batchSize = pendingWrites.size();
        pendingWrites.clear();
        lastFlushTime = System.currentTimeMillis();
        
        if (batchSize > 0) {
            System.out.println("Flushed " + batchSize + " entries with 1 fsync " +
                             "(efficiency: " + batchSize + "× reduction in fsync calls)");
        }
    }
    
    /**
     * Flush write buffer to file channel (but not fsync yet).
     */
    private void flushBuffer() throws IOException {
        if (writeBuffer.position() > 0) {
            writeBuffer.flip();
            while (writeBuffer.hasRemaining()) {
                fileChannel.write(writeBuffer);
            }
            writeBuffer.clear();
        }
    }
    
    /**
     * Pre-allocate file space if needed.
     * Prevents filesystem fragmentation and reduces metadata updates.
     */
    private void preAllocIfNeeded(int size) throws IOException {
        if (currentPosition + size > nextPreAllocPosition) {
            preAllocate();
        }
    }
    
    /**
     * Pre-allocate the next chunk of file space.
     */
    private void preAllocate() throws IOException {
        long oldPosition = fileChannel.position();
        
        // Write a single byte at the pre-allocation boundary
        // This extends the file without actually writing all the bytes
        ByteBuffer oneByte = ByteBuffer.allocate(1);
        oneByte.put((byte) 0);
        oneByte.flip();
        fileChannel.write(oneByte, nextPreAllocPosition - 1);
        
        System.out.println("Pre-allocated " + (preAllocSize / 1024 / 1024) + "MB " +
                         "(file size: " + (nextPreAllocPosition / 1024 / 1024) + "MB)");
        
        nextPreAllocPosition += preAllocSize;
        
        // Restore position
        fileChannel.position(oldPosition);
    }
    
    /**
     * Read entries from the WAL for recovery.
     */
    public List<byte[]> readAll() throws IOException {
        // Flush any pending writes first
        flush();
        
        List<byte[]> entries = new ArrayList<>();
        FileChannel readChannel = new RandomAccessFile(walFile, "r").getChannel();
        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        
        try {
            long position = 0;
            while (position < currentPosition) {
                // Read length
                lengthBuffer.clear();
                int bytesRead = readChannel.read(lengthBuffer, position);
                if (bytesRead < 4) {
                    break;  // End of valid data
                }
                lengthBuffer.flip();
                int length = lengthBuffer.getInt();
                position += 4;
                
                // Read data
                if (length > 0 && length < 10 * 1024 * 1024) {  // Sanity check: max 10MB
                    ByteBuffer dataBuffer = ByteBuffer.allocate(length);
                    bytesRead = readChannel.read(dataBuffer, position);
                    if (bytesRead == length) {
                        dataBuffer.flip();
                        byte[] data = new byte[length];
                        dataBuffer.get(data);
                        entries.add(data);
                        position += length;
                    } else {
                        break;  // Incomplete entry
                    }
                }
            }
        } finally {
            readChannel.close();
        }
        
        return entries;
    }
    
    /**
     * Get performance statistics.
     */
    public Stats getStats() {
        return new Stats(totalWrites, totalFsyncs, currentPosition, walFile.length());
    }
    
    public void close() throws IOException {
        flush();
        fileChannel.close();
        randomAccessFile.close();
    }
    
    /**
     * Tracks a pending write that hasn't been fsync'd yet.
     */
    private static class PendingWrite {
        final long position;
        final int length;
        
        PendingWrite(long position, int length) {
            this.position = position;
            this.length = length;
        }
    }
    
    /**
     * Performance statistics for the WAL.
     */
    public static class Stats {
        public final long totalWrites;
        public final long totalFsyncs;
        public final long bytesWritten;
        public final long fileSize;
        public final double fsyncReduction;
        
        Stats(long totalWrites, long totalFsyncs, long bytesWritten, long fileSize) {
            this.totalWrites = totalWrites;
            this.totalFsyncs = totalFsyncs;
            this.bytesWritten = bytesWritten;
            this.fileSize = fileSize;
            this.fsyncReduction = totalFsyncs > 0 ? (double) totalWrites / totalFsyncs : 0;
        }
        
        @Override
        public String toString() {
            return String.format("WAL Stats: writes=%d, fsyncs=%d, bytes=%d, fileSize=%d, efficiency=%.1fx",
                               totalWrites, totalFsyncs, bytesWritten, fileSize, fsyncReduction);
        }
    }
}

