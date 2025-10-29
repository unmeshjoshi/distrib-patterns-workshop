package com.distribpatterns.wal;

import com.distribpatterns.common.Config;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


public class WriteAheadLog {
    static int sizeOfInt = 4;
    static int sizeOfLong = 8;
    final TimeBasedLogCleaner logCleaner;
    public WALSegment openSegment;
    private Config config;
    //Segments are kept sorted in ascending order of log indexes.
    //So that it easier to traverse them to find specific entry.
    //#see getAllSegmentsContainingLogGreaterThan
    List<WALSegment> sortedSavedSegments;

    public static WriteAheadLog openWAL(Config config) {
        return new WriteAheadLog(openAllSegments(config.getWalDir()), config);
    }

    private static List<WALSegment> openAllSegments(File walDir) {
        // Validate and create directory if needed
        if (!walDir.exists()) {
            if (!walDir.mkdirs()) {
                throw new IllegalStateException("Failed to create WAL directory: " + walDir.getAbsolutePath());
            }
        }
        if (!walDir.isDirectory()) {
            throw new IllegalArgumentException("WAL path is not a directory: " + walDir.getAbsolutePath());
        }
        
        List<WALSegment> segments = new ArrayList<>();
        File[] walFiles = walDir.listFiles();
        
        // Handle null return (should not happen after validation, but be defensive)
        if (walFiles != null) {
            for (File walFile : walFiles) {
                String name = walFile.getName();
                Long baseOffset = WALSegment.getBaseOffsetFromFileName(name);
                if (baseOffset >= 0) {
                    segments.add(WALSegment.open(walFile));
                }
            }
        }
        
        if (segments.size() == 0) {
            segments.add(WALSegment.open(0l, walDir));
        }

        //compare ascending for baseoffsets
        Collections.sort(segments, Comparator.comparing(WALSegment::getBaseOffset));
        return segments;
    }

    public WriteAheadLog(List<WALSegment> segmentsSortedByIndex, Config config) {
        if (config == null) {
            throw new IllegalArgumentException("Config cannot be null");
        }
        if (segmentsSortedByIndex == null) {
            throw new IllegalArgumentException("Segments list cannot be null");
        }
        sortedSavedSegments = segmentsSortedByIndex;
        this.openSegment = lastOpenSegment(segmentsSortedByIndex, lastIndex());
        this.config = config;
       //<codeFragment name="logCleanerInit">
        this.logCleaner = newLogCleaner(config);
        this.logCleaner.startup();
       //</codeFragment>
    }

    private int lastIndex() {
        return sortedSavedSegments.size() - 1;
    }

    private WALSegment lastOpenSegment(List<WALSegment> segmentsSortedByIndex, int i) {
        return segmentsSortedByIndex.remove(i);
    }

    private TimeBasedLogCleaner newLogCleaner(Config config) {
        return new TimeBasedLogCleaner(config, this);
    }

    //<codeFragment name="rollSegment">
    public synchronized Long writeEntry(WALEntry entry) {
        maybeRoll();
        return openSegment.writeEntry(entry);
    }

    private void maybeRoll() {
        if (openSegment.
                size() >= config.getMaxLogSize()) {
            openSegment.flush();
            sortedSavedSegments.add(openSegment);
            long lastId = openSegment.getLastLogEntryIndex();
            openSegment = WALSegment.open(lastId, config.getWalDir());
        }
    }
    //</codeFragment>

    public synchronized List<WALEntry> readAll() {
        List<WALEntry> walEntries = new ArrayList<>();
        for (WALSegment sortedSavedSegment : sortedSavedSegments) {
            walEntries.addAll(sortedSavedSegment.readAll());
        }
        walEntries.addAll(openSegment.readAll());
        return walEntries;
    }


    public void flush() {
        openSegment.flush();
    }

    public void close() {
        // Shutdown background threads first
        if (logCleaner != null) {
            logCleaner.shutdown();
        }
        
        // Close all segments
        if (openSegment != null) {
            openSegment.close();
        }
        
        // Close saved segments
        synchronized (this) {
            for (WALSegment segment : sortedSavedSegments) {
                try {
                    segment.close();
                } catch (Exception e) {
                    // Log but continue closing others
                }
            }
        }
    }

    public synchronized void truncate(Long startIndex)  {
        try {
            SegmentPosition position = findSegmentContaining(startIndex);
            truncateSegmentAt(position.segment, startIndex);
            
            if (position.isInSavedSegments()) {
                deleteSegmentsAfter(position.indexInList);
                promoteToOpenSegment(position.segment, position.indexInList);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    private SegmentPosition findSegmentContaining(Long startIndex) {
        SegmentPosition position = findInSavedSegments(startIndex);
        if (position != null) {
            return position;
        }
        
        return findInOpenSegment(startIndex);
    }
    
    private SegmentPosition findInSavedSegments(Long startIndex) {
        for (int i = sortedSavedSegments.size() - 1; i >= 0; i--) {
            WALSegment segment = sortedSavedSegments.get(i);
            if (segmentContainsIndex(segment, startIndex)) {
                return new SegmentPosition(segment, i);
            }
        }
        return null;
    }
    
    private SegmentPosition findInOpenSegment(Long startIndex) {
        if (openSegment.getBaseOffset() <= startIndex) {
            return SegmentPosition.forOpenSegment(openSegment);
        }
        throw new IllegalArgumentException(
            "Log index " + startIndex + " not found in any segment");
    }
    
    private boolean segmentContainsIndex(WALSegment segment, Long startIndex) {
        return segment.getBaseOffset() <= startIndex && 
               segment.getLastLogEntryIndex() >= startIndex;
    }
    
    private void truncateSegmentAt(WALSegment segment, Long startIndex) throws IOException {
        segment.truncate(startIndex);
    }
    
    private void deleteSegmentsAfter(int targetSegmentIndex) {
        for (int i = sortedSavedSegments.size() - 1; i > targetSegmentIndex; i--) {
            WALSegment segmentToDelete = sortedSavedSegments.remove(i);
            segmentToDelete.delete();
        }
    }
    
    private void promoteToOpenSegment(WALSegment newOpenSegment, int savedSegmentIndex) {
        closeAndDeleteCurrentOpenSegment();
        removeFromSavedSegments(savedSegmentIndex);
        openSegment = newOpenSegment;
    }
    
    private void closeAndDeleteCurrentOpenSegment() {
        openSegment.close();
        openSegment.delete();
    }
    
    private void removeFromSavedSegments(int index) {
        sortedSavedSegments.remove(index);
    }
    
    private static class SegmentPosition {
        final WALSegment segment;
        final int indexInList;
        
        SegmentPosition(WALSegment segment, int indexInList) {
            this.segment = segment;
            this.indexInList = indexInList;
        }
        
        static SegmentPosition forOpenSegment(WALSegment segment) {
            return new SegmentPosition(segment, -1);
        }
        
        boolean isInSavedSegments() {
            return indexInList >= 0;
        }
    }

    public synchronized WALEntry readAt(Long index) {
        // Search in saved segments first
        for (WALSegment segment : sortedSavedSegments) {
            if (segment.getBaseOffset() <= index && 
                segment.getLastLogEntryIndex() >= index) {
                return segment.readAt(index);
            }
        }
        
        // If not found in saved segments, try open segment
        if (openSegment.getBaseOffset() <= index) {
            return openSegment.readAt(index);
        }
        
        throw new IllegalArgumentException("No entry found at index: " + index);
    }

    //<codeFragment name="segmentRead">
    public synchronized List<WALEntry> readFrom(Long startIndex) {
        List<WALSegment> segments = getAllSegmentsContainingLogGreaterThan(startIndex);
        return readWalEntriesFrom(startIndex, segments);
    }
    //</codeFragment>

    private List<WALEntry> readWalEntriesFrom(Long startIndex, List<WALSegment> segments) {
        List<WALEntry> allEntries = new ArrayList<>();
        for (WALSegment segment : segments) {
            List<WALEntry> walEntries = segment.readFrom(startIndex);
            allEntries.addAll(walEntries);
        }
        return allEntries;
    }

    //<codeFragment name="segmentSelection">
    private List<WALSegment> getAllSegmentsContainingLogGreaterThan(Long startIndex) {
        List<WALSegment> segments = new ArrayList<>();
        //Start from the last segment to the first segment with starting offset less than startIndex
        //This will get all the segments which have log entries more than the startIndex
        // Use a copy to avoid ConcurrentModificationException
        List<WALSegment> savedSegmentsCopy = new ArrayList<>(sortedSavedSegments);
        for (int i = savedSegmentsCopy.size() - 1; i >= 0; i--) {
            WALSegment walSegment = savedSegmentsCopy.get(i);
            segments.add(walSegment);

            if (walSegment.getBaseOffset() <= startIndex) {
                break; // break for the first segment with baseoffset less than startIndex
            }
        }

        // Always add open segment if it has entries
        // (it contains the latest entries which might be > startIndex)
        if (openSegment.getLastLogEntryIndex() > 0) {
            segments.add(openSegment);
        }

        return segments;
    }
    //</codeFragment>

    public synchronized void removeAndDeleteSegment(WALSegment walSegment) {
        int index = indexOf(walSegment);
        sortedSavedSegments.remove(index);
        walSegment.delete();
    }

    private int indexOf(WALSegment walSegment) {
        for (int i = 0; i < sortedSavedSegments.size(); i++) {
            WALSegment segment = sortedSavedSegments.get(i);
            if (segment.getBaseOffset() == walSegment.getBaseOffset())
                return i;
        }
        throw new RuntimeException("No log segment found");
    }

    public synchronized long getLastLogIndex() {
        return openSegment.getLastLogEntryIndex();
    }

    public synchronized WALEntry getLastLogEntry() {
        return readAt(getLastLogIndex());
    }

    public synchronized boolean isEmpty() {
        return openSegment.size() == 0;
    }

    public synchronized Long writeEntry(byte[] data) {
        return writeEntry(data, 0);
    }

    public synchronized Long writeEntry(byte[] data, long generation) {
        var logEntryId = getLastLogIndex() + 1;
        var logEntry = new WALEntry(logEntryId, data, EntryType.DATA, generation);
        return writeEntry(logEntry);
    }

    public synchronized Long getLastLogEntryGeneration() {
        if (isEmpty()) {
            return 0l;
        }
        return getLastLogEntry().getGeneration();
    }

    public synchronized boolean exists(WALEntry entry) {
        return getLastLogIndex() >= entry.getEntryIndex();
    }

    public synchronized long getLogStartIndex() {
        return isEmpty()? 0:readAt(1l).getEntryIndex();
    }
}


