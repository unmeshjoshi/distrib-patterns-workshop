package com.distribpatterns.wal;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.CRC32;

public class WALEntry {
    private final CRC32 crc = new CRC32();
//<codeFragment name="walEntry">
    private final Long entryIndex;
    private final byte[] data;
    private final EntryType entryType;
    private final long timeStamp;
//</codeFragment>
    private final Long generation;

    public WALEntry(byte[] data) {
        this(-1l, data, EntryType.DATA, 0);
    };

    public WALEntry(Long entryIndex, byte[] data, EntryType entryType, long generation) {
        this.entryIndex = entryIndex;
        this.data = data;
        this.entryType = entryType;
        this.generation = generation;
        this.timeStamp = System.currentTimeMillis();
    }

    public Long getEntryIndex() {
        return entryIndex;
    }

    public byte[] getData() {
        return data;
    }

    public EntryType getEntryType() {
        return entryType;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public ByteBuffer serialize() {
        Integer entrySize = serializedSize();
        var bufferSize = logEntrySize(); //4 bytes for record length + walEntry size
        var buffer = ByteBuffer.allocate(bufferSize);
        buffer.clear();
        buffer.putInt(entrySize);
        buffer.putInt(entryType.getValue());
        buffer.putLong(generation);
        buffer.putLong(entryIndex);
        buffer.putLong(timeStamp);

        crc.update(data);
        buffer.putLong(crc.getValue());


        buffer.put(data);
        return buffer;
    }

    public Long getGeneration() {
        return generation;
    }

    Integer logEntrySize() { //4 bytes for size + size of serialized entry.
        return WriteAheadLog.sizeOfInt + serializedSize();
    }

    private Integer serializedSize() {
        return sizeOfData() + sizeOfHeader(); //size of all the fields
    }

    /**
     * Centralized layout definition for WAL entry format.
     * Single source of truth for serialization and deserialization.
     */
    public static final class Layout {
        // Size of the length prefix that precedes the header
        public static final int SIZE_LENGTH_PREFIX = WriteAheadLog.sizeOfInt;
        
        // Field sizes
        public static final int SIZE_ENTRY_TYPE = WriteAheadLog.sizeOfInt;
        public static final int SIZE_GENERATION = WriteAheadLog.sizeOfLong;
        public static final int SIZE_ENTRY_INDEX = WriteAheadLog.sizeOfLong;
        public static final int SIZE_TIMESTAMP = WriteAheadLog.sizeOfLong;
        public static final int SIZE_CRC = WriteAheadLog.sizeOfLong;
        
        // Offsets relative to start of header (i.e., after LENGTH_PREFIX)
        public static final int OFFSET_ENTRY_TYPE = 0;
        public static final int OFFSET_GENERATION = OFFSET_ENTRY_TYPE + SIZE_ENTRY_TYPE;
        public static final int OFFSET_ENTRY_INDEX = OFFSET_GENERATION + SIZE_GENERATION;
        public static final int OFFSET_TIMESTAMP = OFFSET_ENTRY_INDEX + SIZE_ENTRY_INDEX;
        public static final int OFFSET_CRC = OFFSET_TIMESTAMP + SIZE_TIMESTAMP;
        
        // Total header size (excludes LENGTH_PREFIX, excludes data)
        public static final int HEADER_SIZE = SIZE_ENTRY_TYPE + SIZE_GENERATION + 
                                              SIZE_ENTRY_INDEX + SIZE_TIMESTAMP + SIZE_CRC;
    }
    
    static int sizeOfHeader() {
        return Layout.HEADER_SIZE;
    }

    private int sizeOfData() {
        return data.length;
    }

    public boolean matchEntry(WALEntry entry) {
        return this.getGeneration() == entry.generation
                && this.entryIndex == entry.entryIndex
                && Arrays.equals(this.data, entry.data);
    }

    @Override
    public String toString() {
        return "WALEntry{" +
                "entryId=" + entryIndex +
                ", data=" + Arrays.toString(data) +
                ", entryType=" + entryType +
                ", timeStamp=" + timeStamp +
                ", generation=" + generation +
                '}';
    }
}
