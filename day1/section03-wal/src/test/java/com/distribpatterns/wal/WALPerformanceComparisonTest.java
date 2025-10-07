package com.distribpatterns.wal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.distribpatterns.common.Config;
import com.distribpatterns.common.TestUtils;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertTrue;

/**
 * Compares basic WAL implementation vs optimized WAL implementation.
 * 
 * Demonstrates the performance impact of:
 * 1. Buffered writes (reduces system calls)
 * 2. Group commit (reduces fsync calls)
 * 3. Pre-allocation (prevents filesystem fragmentation)
 */
public class WALPerformanceComparisonTest {
    
    private File walDir;
    
    @BeforeEach
    public void setUp() {
        walDir = TestUtils.tempDir("wal-perf-test");
    }
    
    @AfterEach
    public void tearDown() {
        // Cleanup
        if (walDir != null && walDir.exists()) {
            deleteDirectory(walDir);
        }
    }
    
    @Test
    public void compareBasicVsOptimized() throws IOException {
        int numEntries = 1000;
        byte[] data = "This is a test WAL entry with some data".getBytes();

        logBeginWriting(numEntries, data);

        // Test 1: Basic implementation (WriteAheadLog)
        log("Test 1: BASIC WAL (WriteAheadLog with fsync after each write)", "-".repeat(70));

        long basicTime = testBasicWAL(numEntries, data);

        newLine();

        // Clean up for next test
        deleteDirectory(walDir);
        walDir = TestUtils.tempDir("wal-perf-test");
        
        // Test 2: Optimized implementation (OptimizedWriteAheadLog)
        log("Test 2: OPTIMIZED WAL (buffered writes + group commit + pre-allocation)", "-".repeat(70));

        long optimizedTime = testOptimizedWAL(numEntries, data);
        newLine();

        assertTrue("Optimized time should lower than basic log write time", basicTime > optimizedTime);

        // Comparison
        logComparison(basicTime, optimizedTime, numEntries);
    }

    private static void logComparison(long basicTime, long optimizedTime, int numEntries) {
        log("=".repeat(70), "RESULTS");
        log("=".repeat(70), "Basic WAL:     " + basicTime + "ms");
        System.out.println("Optimized WAL: " + optimizedTime + "ms");

        if (optimizedTime > 0) {
            System.out.println("Speedup:       " + String.format("%.1fx faster", (double) basicTime / optimizedTime));
        }

        newLine();
        log("Key Insights:", "1. Buffered writes reduce system calls by 100-1000x");
        log("2. Group commit reduces fsync calls from " + numEntries + " to ~10-20", "3. Pre-allocation prevents filesystem fragmentation");

        if (optimizedTime > 0) {
            System.out.println("4. Combined optimizations provide " +
                             String.format("%.0fx", (double) basicTime / optimizedTime) +
                             " performance improvement!");
        }

        System.out.println("=".repeat(70));
    }

    private static void log(String x, String repeat) {
        System.out.println(x);
        System.out.println(repeat);
    }

    private static void newLine() {
        System.out.println();
    }

    private static void logBeginWriting(int numEntries, byte[] data) {
        log("=".repeat(70), "WAL Performance Comparison");
        log("=".repeat(70), "Writing " + numEntries + " entries of " + data.length + " bytes each");
        newLine();
    }

    /**
     * Basic WAL: Uses the standard WriteAheadLog implementation.
     * This implementation does fsync after each write.
     */
    private long testBasicWAL(int numEntries, byte[] data) throws IOException {
        Config config = new Config(walDir.getAbsolutePath());
        WriteAheadLog wal = WriteAheadLog.openWAL(config);
        
        long startTime = System.currentTimeMillis();
        
        try {
            for (int i = 0; i < numEntries; i++) {
                // WALEntry constructor: (Long entryIndex, byte[] data, EntryType entryType, long generation)
                WALEntry entry = new WALEntry((long) i, data, EntryType.DATA, 0L);
                wal.writeEntry(entry);
                
                if ((i + 1) % 100 == 0) {
                    System.out.println("  Wrote " + (i + 1) + " entries, " + (i + 1) + " fsyncs");
                }
            }
        } finally {
            wal.close();
        }
        
        long elapsed = System.currentTimeMillis() - startTime;
        
        System.out.println("Completed in " + elapsed + "ms");
        
        if (elapsed > 0) {
            log("Throughput: " + (numEntries * 1000 / elapsed) + " writes/sec", "Average latency: " + String.format("%.2fms per write", (double) elapsed / numEntries));
        }
        
        System.out.println("fsyncs: ~" + numEntries + " (fsync after each write)");
        
        // Calculate file size
        long fileSize = calculateDirSize(walDir);
        System.out.println("File size: " + (fileSize / 1024) + " KB");
        
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
        
        if (elapsed > 0) {
            log("Throughput: " + (numEntries * 1000 / elapsed) + " writes/sec", "Average latency: " + String.format("%.2fms per write", (double) elapsed / numEntries));
        }

        log("fsyncs: " + stats.totalFsyncs + " (group commit efficiency: " +
                String.format("%.1fx", (double) stats.fsyncReduction) + ")", "File size: " + (stats.fileSize / 1024) + " KB " +
                "(pre-allocated: " + (stats.fileSize / 1024 / 1024) + " MB)");

        return elapsed;
    }
    
    @Test
    public void showDetailedOptimizations() {
        log("\n" + "=".repeat(70), "DETAILED OPTIMIZATION BREAKDOWN");
        log("=".repeat(70), "\n1. BUFFERED WRITES");
        log("   Problem: Each write() is a system call (user→kernel transition)", "   Solution: Buffer writes in memory, flush when buffer full");
        log("   Impact: 100-1000× reduction in system calls", "\n2. GROUP COMMIT");
        log("   Problem: fsync() is expensive (~1-10ms per call)", "   Solution: Batch N writes, do one fsync for all");
        log("   Impact: N× reduction in fsync calls", "   Example: 1000 writes → 10 fsyncs = 100× improvement");

        log("\n3. PRE-ALLOCATION", "   Problem: File growth causes filesystem metadata updates");
        log("   Solution: Pre-allocate large chunks (16MB)", "   Impact: Prevents fragmentation, predictable performance");

        log("\n" + "=".repeat(70), "WHY THESE OPTIMIZATIONS MATTER");
        log("=".repeat(70), "\nFrom Section 1 (Little's Law), we measured:");
        log("  - Disk service time: ~0.1ms per I/O", "  - fsync latency: ~1-10ms");
        log("\nBasic approach:", "  - 1000 writes × 10ms fsync = 10,000ms (10 seconds!)");
        log("\nOptimized approach:", "  - 1000 writes batched → 10 fsyncs × 10ms = 100ms");
        log("  - 100× faster!", "\n" + "=".repeat(70));
    }
    
    private long calculateDirSize(File dir) {
        long size = 0;
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile()) {
                    size += file.length();
                } else if (file.isDirectory()) {
                    size += calculateDirSize(file);
                }
            }
        }
        return size;
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
