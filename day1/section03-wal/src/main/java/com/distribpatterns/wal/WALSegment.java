package com.distribpatterns.wal;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class WALSegment {
    private static String logSuffix = ".log";
    private static String logPrefix = "wal";
    final RandomAccessFile randomAccessFile;
    final FileChannel fileChannel;
    private final Integer pageSize = 4096;
    private final Long maxFlushDelayMs = 100L;
    Map<Long, Long> entryOffsets = new HashMap<Long, Long>();
    private File file;
    public Long logicalBlockSize;
    //enable adding padding bytes if log entry written to file is smaller than logical block size of the storage device.
    boolean enablePadding = false;

    private WALSegment(Long startIndex, File file) {
        try {
            this.file = file;
            this.randomAccessFile = new RandomAccessFile(file, "rw");
            this.fileChannel = randomAccessFile.getChannel();

            Path path = this.file.toPath();
            FileStore fileStore = Files.getFileStore(path);
            this.logicalBlockSize = fileStore.getBlockSize();

            //build index;
            buildOffsetIndex();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized String getFileName() {
        return file.getPath();
    }

    public synchronized Long getBaseOffset() {
        return getBaseOffsetFromFileName(file.getName());
    }

    public static WALSegment open(Long startIndex, File walDir) {
        var file = new File(walDir, createFileName(startIndex));
        return new WALSegment(startIndex, file);
    }

    public static WALSegment open(File file) {
        return new WALSegment(getBaseOffsetFromFileName(file.getName()), file);
    }

    public synchronized List<WALEntry> readFrom(Long starIndex) {
            var entries = new ArrayList<WALEntry>();
            var deserializer = new WALEntryDeserializer(fileChannel);
            List<Long> indexes = entryOffsets.keySet().stream().filter(index -> index >= starIndex).collect(Collectors.toList());
            for (Long index : indexes) {
                var entryOffset = entryOffsets.get(index);
                try {
                    WALEntry entry = deserializer.readEntry(entryOffset);
                    entries.add(entry);

                } catch (Exception e) {
                      throw new RuntimeException("Error reading from entryOffset " + entryOffset, e);
                }
            }
            return entries;
    }

    public synchronized void buildOffsetIndex() {
        try {
            entryOffsets = new HashMap<>();
            var totalBytesRead = 0L;
            var deserializer = new WALEntryDeserializer(fileChannel);
            while (totalBytesRead < fileChannel.size()) {
                WALEntry entry = deserializer.readEntry(totalBytesRead);
                entryOffsets.put(entry.getEntryIndex(), totalBytesRead);
                totalBytesRead += entry.logEntrySize(); //size of entry + size of int which stores length
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized List<WALEntry> readAll() {
        try {
            var totalBytesRead = 0L;
            var entries = new ArrayList<WALEntry>();
            var deserializer = new WALEntryDeserializer(fileChannel);
            while (totalBytesRead < fileChannel.size()) {
                WALEntry entry = deserializer.readEntry(totalBytesRead);
                totalBytesRead += entry.logEntrySize(); //size of entry + size of int which stores length
                entries.add(entry);
            }
            return entries;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized Long getLastLogEntryTimestamp() {
        if (entryOffsets.isEmpty()) {
            return 0l;
        }
        return readAt(getLastLogEntryIndex()).getTimeStamp();
    }

    public synchronized Long getLastLogEntryIndex() {
        return entryOffsets.keySet().stream().max(Long::compareTo).orElse(0l);
    }

    public synchronized Long writeEntry(WALEntry logEntry) {
        try {
            long entryOffset = fileChannel.size();
            ByteBuffer dataToWrite = logEntry.serialize();
            //addPadding if data to write is not aligned on 4KB
            if (enablePadding) {
                dataToWrite = addPaddingBytesIfNeeded(dataToWrite);
            }
            writeToChannel(dataToWrite);
            entryOffsets.put(logEntry.getEntryIndex(), entryOffset);
            return logEntry.getEntryIndex();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public ByteBuffer addPaddingBytesIfNeeded(ByteBuffer dataToWrite) {
        int sizeOfBuffer = dataToWrite.limit();
        long remainingBytes = sizeOfBuffer % logicalBlockSize;
        if (remainingBytes == 0) {
            return dataToWrite;
        }

        long paddingSize = (logicalBlockSize - remainingBytes);
        if (paddingSize < WALEntry.sizeOfHeader()) {
            //we need to add l additional bytes;
            //l needs to enough to accomodate padding marker + 4 bytes length of padding block + padding bytes
            //so if l < (padding marker (integer) + 4 bytes), we add one more logical block and add one
            // more logical block size to the padding bytes.
            paddingSize = paddingSize + (logicalBlockSize);
        }
        int paddingByteSize = Math.toIntExact(paddingSize - WALEntry.sizeOfHeader() - WriteAheadLog.sizeOfInt);
        ByteBuffer paddedData = ByteBuffer.allocate((int) (sizeOfBuffer + paddingSize));
        paddedData.put(dataToWrite.flip());
        WALEntry entry = new WALEntry(-1l, new byte[Math.toIntExact(paddingByteSize)], EntryType.PADDING, 0);
        paddedData.put(entry.serialize().flip());
        return paddedData;
    }

    private Long writeToChannel(ByteBuffer buffer) {
        try {
            buffer.flip();
            while (buffer.hasRemaining()) {
                fileChannel.write(buffer);
            }
            flush();
            return fileChannel.position();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void flush() {
        try {
            fileChannel.force(true);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void close() {
        flush();

        try {
            fileChannel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //<codeFragment name="logTruncation">
    public synchronized  void truncate(Long logIndex) throws IOException {
        var filePosition = entryOffsets.get(logIndex);
        if (filePosition == null) throw new IllegalArgumentException("No file position available for logIndex=" + logIndex);

        fileChannel.truncate(filePosition);
        truncateIndex(logIndex);
    }

    private void truncateIndex(Long logIndex) {
        entryOffsets.entrySet().removeIf(entry -> entry.getKey() >= logIndex);
    }
    //</codeFragment>

    //<codeFragment name="walFileName">
    public static String createFileName(Long startIndex) {
        return logPrefix + "_" + startIndex + logSuffix;
    }

    public static Long getBaseOffsetFromFileName(String fileName) {
        String[] nameAndSuffix = fileName.split(logSuffix);
        String[] prefixAndOffset = nameAndSuffix[0].split("_");
        if (prefixAndOffset[0].equals(logPrefix))
            return Long.parseLong(prefixAndOffset[1]);

        return -1l;
    }
    //</codeFragment>
    public synchronized long size() {
        try {
            return fileChannel.size();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void delete() {
        try {
            fileChannel.close();
            randomAccessFile.close();
            Files.deleteIfExists(file.toPath());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public WALEntry readAt(Long index) {
        var filePosition = entryOffsets.get(index);
        if (filePosition == null) {
            throw new IllegalArgumentException("No file position available for logIndex=" + index);
        }
        var deserializer = new WALEntryDeserializer(fileChannel);
        return deserializer.readEntry(filePosition);
    }
}
