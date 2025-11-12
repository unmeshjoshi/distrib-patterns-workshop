package com.distribpatterns.wal;

import com.distribpatterns.common.Config;
import com.distribpatterns.common.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.io.File;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class WriteAheadLogTest {
    
    private WriteAheadLog wal;
    private File walDir;
    
    @AfterEach
    public void cleanup() {
        if (wal != null) {
            try {
                wal.close();
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
    }

    @Test
    @DisplayName("readAt() should read from saved segments, not just open segment")
    public void testReadAtFromSavedSegments() {
        walDir = TestUtils.tempDir("wal-readat-test");
        // Use small max log size to force segment rolling
        Config config = new Config(walDir.getAbsolutePath(), 100L);
        wal = WriteAheadLog.openWAL(config);
        
        // Write entries to force multiple segments
        Long idx1 = wal.writeEntry("Entry 1 - some data to fill space".getBytes());
        wal.writeEntry("Entry 2 - some data to fill space".getBytes());
        Long idx3 = wal.writeEntry("Entry 3 - some data to fill space".getBytes());
        wal.writeEntry("Entry 4 - some data to fill space".getBytes());
        wal.writeEntry("Entry 5 - some data to fill space".getBytes());
        
        // Force flush to ensure segments are written
        wal.flush();
        
        // Verify we have saved segments (rolled over)
        assertTrue(wal.sortedSavedSegments.size() > 0, 
            "Should have rolled over to saved segments with small max log size");
        
        // This will fail if idx1 is in a saved segment
        WALEntry entry1 = wal.readAt(idx1);
        assertNotNull(entry1, "Should be able to read entry from saved segment");
        assertEquals(idx1, entry1.getEntryIndex());
        assertArrayEquals("Entry 1 - some data to fill space".getBytes(), entry1.getData());
        
        // Test reading from middle segment
        WALEntry entry3 = wal.readAt(idx3);
        assertNotNull(entry3);
        assertEquals(idx3, entry3.getEntryIndex());
    }
    
    @Test
    @DisplayName("truncate() should delete entries after specified index in open segment only")
    public void testTruncateFromOpenSegmentOnly() {
        walDir = TestUtils.tempDir("wal-truncate-open");
        Config config = new Config(walDir.getAbsolutePath(), 10 * 1024L); // Large enough to avoid rolling
        wal = WriteAheadLog.openWAL(config);
        
        // Write entries without forcing segment rolling
        Long idx1 = wal.writeEntry("Entry 1".getBytes());
        Long idx2 = wal.writeEntry("Entry 2".getBytes());
        Long idx3 = wal.writeEntry("Entry 3".getBytes());
        Long idx4 = wal.writeEntry("Entry 4".getBytes());
        Long idx5 = wal.writeEntry("Entry 5".getBytes());
        wal.flush();
        
        // Verify no saved segments
        assertEquals(0, wal.sortedSavedSegments.size(), "Should have no saved segments");
        assertEquals(5, wal.readAll().size(), "Should have 5 entries");
        
        // Truncate at idx3 (keeps 1, 2; deletes 3, 4, 5)
        wal.truncate(idx3);
        
        // Verify entries 1-2 exist, 3-5 don't
        assertTrue(wal.exists(new WALEntry(idx1, new byte[0], EntryType.DATA, 0)));
        assertTrue(wal.exists(new WALEntry(idx2, new byte[0], EntryType.DATA, 0)));
        assertFalse(wal.exists(new WALEntry(idx3, new byte[0], EntryType.DATA, 0)));
        assertFalse(wal.exists(new WALEntry(idx4, new byte[0], EntryType.DATA, 0)));
        assertFalse(wal.exists(new WALEntry(idx5, new byte[0], EntryType.DATA, 0)));
        
        assertEquals(2, wal.readAll().size(), "Should have 2 entries after truncation");
        assertEquals(idx2, wal.getLastLogIndex(), "Last index should be idx2");
    }
    
    @Test
    @DisplayName("truncate() should work when truncating in saved segments")
    public void testTruncateInSavedSegments() {
        walDir = TestUtils.tempDir("wal-truncate-saved");
        Config config = new Config(walDir.getAbsolutePath(), 150L);
        wal = WriteAheadLog.openWAL(config);
        
        // Write entries with enough data to force segment rolling
        byte[] largeData = new byte[60];
        Long idx1 = wal.writeEntry(largeData);
        Long idx2 = wal.writeEntry(largeData);
        Long idx3 = wal.writeEntry(largeData);
        Long idx4 = wal.writeEntry(largeData);
        Long idx5 = wal.writeEntry(largeData);
        Long idx6 = wal.writeEntry(largeData);
        wal.flush();
        
        int savedSegmentsBefore = wal.sortedSavedSegments.size();
        assertTrue(savedSegmentsBefore > 0, "Should have saved segments");
        
        // Truncate at idx4 (in a saved segment) - keeps 1,2,3; deletes 4,5,6
        wal.truncate(idx4);
        
        // Verify entries 1-3 exist, 4-6 don't
        assertTrue(wal.exists(new WALEntry(idx1, new byte[0], EntryType.DATA, 0)));
        assertTrue(wal.exists(new WALEntry(idx2, new byte[0], EntryType.DATA, 0)));
        assertTrue(wal.exists(new WALEntry(idx3, new byte[0], EntryType.DATA, 0)));
        assertFalse(wal.exists(new WALEntry(idx4, new byte[0], EntryType.DATA, 0)));
        assertFalse(wal.exists(new WALEntry(idx5, new byte[0], EntryType.DATA, 0)));
        assertFalse(wal.exists(new WALEntry(idx6, new byte[0], EntryType.DATA, 0)));
        
        assertEquals(3, wal.readAll().size(), "Should have 3 entries after truncation");
        assertEquals(idx3, wal.getLastLogIndex(), "Last index should be idx3");
        
        // Segments may have been reorganized
        assertTrue(wal.sortedSavedSegments.size() <= savedSegmentsBefore, 
            "Should have same or fewer saved segments after truncation");
        
        // Verify we can continue writing
        Long idx7 = wal.writeEntry(largeData);
        assertEquals(idx3 + 1, (long)idx7, "New entry should have next index");
    }
    
    @Test
    @DisplayName("truncate() should handle truncating at first entry")
    public void testTruncateAtFirstEntry() {
        walDir = TestUtils.tempDir("wal-truncate-first");
        Config config = new Config(walDir.getAbsolutePath());
        wal = WriteAheadLog.openWAL(config);
        
        // Write multiple entries
        Long idx1 = wal.writeEntry("Entry 1".getBytes());
        Long idx2 = wal.writeEntry("Entry 2".getBytes());
        Long idx3 = wal.writeEntry("Entry 3".getBytes());
        wal.flush();
        
        assertEquals(3, wal.readAll().size());
        
        // Truncate at idx1 (Deletes complete log)
        wal.truncate(idx1);
        
        // Verify only first entry exists
        assertFalse(wal.exists(new WALEntry(idx1, new byte[0], EntryType.DATA, 0)));
        assertFalse(wal.exists(new WALEntry(idx2, new byte[0], EntryType.DATA, 0)));
        assertFalse(wal.exists(new WALEntry(idx3, new byte[0], EntryType.DATA, 0)));
        
        assertEquals(0, wal.readAll().size(), "Should have only 1 entry");
        assertEquals(0, wal.getLastLogIndex());
    }
    
    @Test
    @DisplayName("truncate() should delete all entries from truncation point onwards")
    public void testTruncateDeletesFromPointOnwards() {
        walDir = TestUtils.tempDir("wal-truncate-onwards");
        Config config = new Config(walDir.getAbsolutePath());
        wal = WriteAheadLog.openWAL(config);
        
        // Write entries
        Long idx1 = wal.writeEntry("Entry 1".getBytes());
        Long idx2 = wal.writeEntry("Entry 2".getBytes());
        Long idx3 = wal.writeEntry("Entry 3".getBytes());
        Long idx4 = wal.writeEntry("Entry 4".getBytes());
        Long idx5 = wal.writeEntry("Entry 5".getBytes());
        wal.flush();
        
        assertEquals(5, wal.readAll().size(), "Should have 5 entries");
        
        // Truncate at idx4 - keeps 1,2,3; deletes 4,5
        wal.truncate(idx4);
        
        // Verify only 3 entries remain
        List<WALEntry> remainingEntries = wal.readAll();
        assertEquals(3, remainingEntries.size(), "Should have 3 entries after truncation");
        
        // Verify correct entries remain
        assertEquals(idx1, remainingEntries.get(0).getEntryIndex());
        assertEquals(idx2, remainingEntries.get(1).getEntryIndex());
        assertEquals(idx3, remainingEntries.get(2).getEntryIndex());
        
        // Last index should be idx3
        assertEquals(idx3, wal.getLastLogIndex());
        
        // Can continue writing
        Long idx6 = wal.writeEntry("Entry 6".getBytes());
        assertEquals(idx3 + 1, (long)idx6);
    }
    
    @Test
    @DisplayName("truncate() at last index should remove that entry")
    public void testTruncateAtLastIndex() {
        walDir = TestUtils.tempDir("wal-truncate-last");
        Config config = new Config(walDir.getAbsolutePath());
        wal = WriteAheadLog.openWAL(config);
        
        Long idx1 = wal.writeEntry("Entry 1".getBytes());
        Long idx2 = wal.writeEntry("Entry 2".getBytes());
        Long idx3 = wal.writeEntry("Entry 3".getBytes());
        Long idx4 = wal.writeEntry("Entry 4".getBytes());
        wal.flush();
        
        // Truncate at idx3 (keeps 1,2; deletes 3,4)
        wal.truncate(idx3);
        
        assertTrue(wal.exists(new WALEntry(idx1, new byte[0], EntryType.DATA, 0)));
        assertTrue(wal.exists(new WALEntry(idx2, new byte[0], EntryType.DATA, 0)));
        assertFalse(wal.exists(new WALEntry(idx3, new byte[0], EntryType.DATA, 0)));
        assertFalse(wal.exists(new WALEntry(idx4, new byte[0], EntryType.DATA, 0)));
        
        assertEquals(2, wal.readAll().size());
        assertEquals(idx2, wal.getLastLogIndex());
    }
    
    @Test
    @DisplayName("openWAL() should handle non-existent directory gracefully")
    public void testOpenWALWithNonExistentDirectory() {
        walDir = new File("/tmp/non-existent-dir-" + System.currentTimeMillis());
        Config config = new Config(walDir.getAbsolutePath());
        
        // BUG: NPE when walFiles is null (directory doesn't exist)
        // Should either create directory or throw meaningful exception
        assertDoesNotThrow(() -> {
            wal = WriteAheadLog.openWAL(config);
        }, "Should handle non-existent directory gracefully");
        
        // Should be able to write after creating WAL
        assertNotNull(wal);
        Long idx = wal.writeEntry("test data".getBytes());
        assertNotNull(idx);
    }

    
    @Test
    @DisplayName("Null config should throw IllegalArgumentException")
    public void testNullConfigValidation() {
        // BUG: No null validation on config
        assertThrows(IllegalArgumentException.class, () -> {
            wal = new WriteAheadLog(List.of(), null);
        }, "Should validate null config");
    }
    
    @Test
    @DisplayName("Empty WAL should handle readAt gracefully")
    public void testReadAtOnEmptyWAL() {
        walDir = TestUtils.tempDir("wal-empty-test");
        Config config = new Config(walDir.getAbsolutePath());
        wal = WriteAheadLog.openWAL(config);
        
        assertTrue(wal.isEmpty());
        
        assertThrows(IllegalArgumentException.class, () -> {
            wal.readAt(1L);
        }, "Should throw exception for non-existent index");
    }
    
    @Test
    @DisplayName("Recover WAL from existing segments")
    public void testRecoverFromExistingSegments() {
        walDir = TestUtils.tempDir("wal-recover-test");
        Config config = new Config(walDir.getAbsolutePath(), 100L);
        
        // Create first WAL and write data
        wal = WriteAheadLog.openWAL(config);
        Long idx1 = wal.writeEntry("Persistent entry 1".getBytes());
        wal.writeEntry("Persistent entry 2".getBytes());
        Long idx3 = wal.writeEntry("Persistent entry 3".getBytes());
        wal.flush();
        wal.close();
        
        // Reopen WAL (simulate restart)
        wal = WriteAheadLog.openWAL(config);
        
        // Should be able to read old entries
        WALEntry entry1 = wal.readAt(idx1);
        assertNotNull(entry1);
        assertArrayEquals("Persistent entry 1".getBytes(), entry1.getData());
        
        WALEntry entry3 = wal.readAt(idx3);
        assertNotNull(entry3);
        assertArrayEquals("Persistent entry 3".getBytes(), entry3.getData());
        
        // Should be able to continue writing
        Long idx4 = wal.writeEntry("New entry after recovery".getBytes());
        assertTrue(idx4 > idx3, "New entries should have higher indices");
    }
    
    @Test
    @DisplayName("Large number of segments should not cause performance issues")
    public void testManySegments() {
        walDir = TestUtils.tempDir("wal-many-segments-test");
        // Very small segments to create many
        Config config = new Config(walDir.getAbsolutePath(), 50L);
        wal = WriteAheadLog.openWAL(config);
        
        // Write enough to create many segments
        for (int i = 0; i < 50; i++) {
            wal.writeEntry(("Entry " + i).getBytes());
        }
        wal.flush();
        
        // Should have many saved segments
        assertTrue(wal.sortedSavedSegments.size() > 5, 
            "Should have created multiple segments");
        
        // ReadAll should work efficiently
        long start = System.currentTimeMillis();
        List<WALEntry> entries = wal.readAll();
        long duration = System.currentTimeMillis() - start;
        
        assertEquals(50, entries.size());
        assertTrue(duration < 1000, "Reading 50 entries should be fast");
    }
    
    @Test
    @DisplayName("readFrom() should work across segment boundaries")
    public void testReadFromAcrossSegments() {
        walDir = TestUtils.tempDir("wal-readfrom-test");
        Config config = new Config(walDir.getAbsolutePath(), 150L);
        wal = WriteAheadLog.openWAL(config);
        
        // Write multiple entries with sufficient data
        byte[] data = new byte[60];
        wal.writeEntry(data);
        wal.writeEntry(data);
        Long idx3 = wal.writeEntry(data);
        wal.writeEntry(data);
        wal.writeEntry(data);
        wal.writeEntry(data);
        wal.flush();
        
        // Read all entries
        List<WALEntry> allEntries = wal.readAll();
        int totalEntries = allEntries.size();
        
        // Read from middle index
        List<WALEntry> entriesFromIdx3 = wal.readFrom(idx3);
        
        // Should get entries from idx3 onwards
        int expectedCount = totalEntries - 2; // We wrote idx1, idx2, then idx3...
        assertEquals(expectedCount, entriesFromIdx3.size(), 
            "Should read " + expectedCount + " entries from idx3 onwards");
        assertEquals(idx3, entriesFromIdx3.get(0).getEntryIndex());
    }

    @Test
    @DisplayName("readAt() should read from saved segments, not just open segment")
    public void testReadWalWithPadding() {
        walDir = TestUtils.tempDir("wal-readat-test");
        // Use small max log size to force segment rolling
        Config config = new Config(walDir.getAbsolutePath(), 100L);
        wal = WriteAheadLog.openWAL(config);
        wal.enablePadding();

        // Write entries to force multiple segments
        byte[] data = new byte[4095];
        Long idx1 = wal.writeEntry(data);
        // Force flush to ensure segments are written
        wal.flush();

        // This will fail if idx1 is in a saved segment
        var entries = wal.readAll();
        var entry1 = entries.get(0);
        assertNotNull(entry1, "Should be able to read entry from saved segment");
        assertEquals(idx1, entry1.getEntryIndex());
        assertArrayEquals(data, entry1.getData());

        var entry2 = entries.get(1);
        assertEquals(EntryType.PADDING, entry2.getEntryType());
        byte[] paddingBytes = entry2.getData();
        assertEquals(4033, paddingBytes.length);

    }
}
