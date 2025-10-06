package replicate.wal;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import replicate.common.Config;
import replicate.common.TestUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Compares naive WAL implementation vs optimized WAL implementation.
 * 
 * Demonstrates the performance impact of:
 * 1. Buffered writes
 * 2. Group commit
 * 3. Pre-allocation
 */
public class WALPerformanceComparisonTest {
    
    private File walDir;
    
    @Before
    public void setUp() {
        walDir = TestUtils.tempDir("wal-perf-test");
    }
    
    @After
    public void tearDown() {
        // Cleanup
        if (walDir != null && walDir.exists()) {
            deleteDirectory(walDir);
        }
    }
    
    @Test
    public void compareNaiveVsOptimized() throws IOException {
        int numEntries = 1000;
        byte[] data = "This is a test WAL entry with some data".getBytes();
        
        System.out.println("=".repeat(70));
        System.out.println("WAL Performance Comparison");
        System.out.println("=".repeat(70));
        System.out.println("Writing " + numEntries + " entries of " + data.length + " bytes each");
        System.out.println();
        
        // Test 1: Naive implementation
        System.out.println("Test 1: NAIVE WAL (fsync after each write)");
        System.out.println("-".repeat(70));
        long naiveTime = testNaiveWAL(numEntries, data);
        System.out.println();
        
        // Test 2: Optimized implementation  
        System.out.println("Test 2: OPTIMIZED WAL (buffered writes + group commit + pre-allocation)");
        System.out.println("-".repeat(70));
        long optimizedTime = testOptimizedWAL(numEntries, data);
        System.out.println();
        
        // Comparison
        System.out.println("=".repeat(70));
        System.out.println("RESULTS");
        System.out.println("=".repeat(70));
        System.out.println("Naive WAL:     " + naiveTime + "ms");
        System.out.println("Optimized WAL: " + optimizedTime + "ms");
        System.out.println("Speedup:       " + String.format("%.1fx faster", (double) naiveTime / optimizedTime));
        System.out.println();
        System.out.println("Key Insights:");
        System.out.println("1. Buffered writes reduce system calls by 100-1000x");
        System.out.println("2. Group commit reduces fsync calls from " + numEntries + " to ~10-20");
        System.out.println("3. Pre-allocation prevents filesystem fragmentation");
        System.out.println("4. Combined optimizations provide " + String.format("%.0fx", (double) naiveTime / optimizedTime) + " performance improvement!");
        System.out.println("=".repeat(70));
    }
    
    /**
     * Naive WAL: Direct writes with fsync after each entry.
     * This is how a simple implementation might look.
     */
    private long testNaiveWAL(int numEntries, byte[] data) throws IOException {
        File walFile = new File(walDir, "naive-wal.log");
        RandomAccessFile raf = new RandomAccessFile(walFile, "rw");
        FileChannel channel = raf.getChannel();
        
        long startTime = System.currentTimeMillis();
        
        try {
            ByteBuffer lengthBuf = ByteBuffer.allocate(4);
            ByteBuffer dataBuf = ByteBuffer.wrap(data);
            
            for (int i = 0; i < numEntries; i++) {
                // Write length
                lengthBuf.clear();
                lengthBuf.putInt(data.length);
                lengthBuf.flip();
                channel.write(lengthBuf);
                
                // Write data
                dataBuf.clear();
                channel.write(dataBuf);
                
                // EXPENSIVE: fsync after EVERY write
                channel.force(false);
                
                if ((i + 1) % 100 == 0) {
                    System.out.println("  Wrote " + (i + 1) + " entries, " + (i + 1) + " fsyncs");
                }
            }
        } finally {
            channel.close();
            raf.close();
        }
        
        long elapsed = System.currentTimeMillis() - startTime;
        
        System.out.println("Completed in " + elapsed + "ms");
        System.out.println("Throughput: " + (numEntries * 1000 / elapsed) + " writes/sec");
        System.out.println("Average latency: " + String.format("%.2fms per write", (double) elapsed / numEntries));
        System.out.println("fsyncs: " + numEntries + " (one per write)");
        System.out.println("File size: " + (walFile.length() / 1024) + " KB");
        
        return elapsed;
    }
    
    /**
     * Optimized WAL: Buffered writes + group commit + pre-allocation.
     * This is how a production-grade WAL works.
     */
    private long testOptimizedWAL(int numEntries, byte[] data) throws IOException {
        Config config = new Config(walDir.getAbsolutePath());
        OptimizedWriteAheadLog wal = new OptimizedWriteAheadLog(config);
        
        long startTime = System.currentTimeMillis();
        
        try {
            for (int i = 0; i < numEntries; i++) {
                wal.append(data);
                
                if ((i + 1) % 100 == 0) {
                    System.out.println("  Wrote " + (i + 1) + " entries...");
                }
            }
            
            // Final flush
            wal.flush();
        } finally {
            wal.close();
        }
        
        long elapsed = System.currentTimeMillis() - startTime;
        OptimizedWriteAheadLog.Stats stats = wal.getStats();
        
        System.out.println("Completed in " + elapsed + "ms");
        System.out.println("Throughput: " + (numEntries * 1000 / elapsed) + " writes/sec");
        System.out.println("Average latency: " + String.format("%.2fms per write", (double) elapsed / numEntries));
        System.out.println("fsyncs: " + stats.totalFsyncs + " (group commit efficiency: " + 
                         String.format("%.1fx", stats.fsyncReduction) + ")");
        System.out.println("File size: " + (stats.fileSize / 1024) + " KB " +
                         "(pre-allocated: " + (stats.fileSize / 1024 / 1024) + " MB)");
        
        return elapsed;
    }
    
    @Test
    public void showDetailedOptimizations() {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("DETAILED OPTIMIZATION BREAKDOWN");
        System.out.println("=".repeat(70));
        
        System.out.println("\n1. BUFFERED WRITES");
        System.out.println("   Problem: Each write() is a system call (user→kernel transition)");
        System.out.println("   Solution: Buffer writes in memory, flush when buffer full");
        System.out.println("   Impact: 100-1000× reduction in system calls");
        
        System.out.println("\n2. GROUP COMMIT");
        System.out.println("   Problem: fsync() is expensive (~1-10ms per call)");
        System.out.println("   Solution: Batch N writes, do one fsync for all");
        System.out.println("   Impact: N× reduction in fsync calls");
        System.out.println("   Example: 1000 writes → 10 fsyncs = 100× improvement");
        
        System.out.println("\n3. PRE-ALLOCATION");
        System.out.println("   Problem: File growth causes filesystem metadata updates");
        System.out.println("   Solution: Pre-allocate large chunks (16MB)");
        System.out.println("   Impact: Prevents fragmentation, predictable performance");
        
        System.out.println("\n" + "=".repeat(70));
        System.out.println("WHY THESE OPTIMIZATIONS MATTER");
        System.out.println("=".repeat(70));
        System.out.println("\nFrom Section 1 (Little's Law), we measured:");
        System.out.println("  - Disk service time: ~0.1ms per I/O");
        System.out.println("  - fsync latency: ~1-10ms");
        System.out.println("\nNaive approach:");
        System.out.println("  - 1000 writes × 10ms fsync = 10,000ms (10 seconds!)");
        System.out.println("\nOptimized approach:");
        System.out.println("  - 1000 writes batched → 10 fsyncs × 10ms = 100ms");
        System.out.println("  - 100× faster!");
        System.out.println("\n" + "=".repeat(70));
    }
    
    private void deleteDirectory(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        dir.delete();
    }
}

