package com.distribpatterns.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

class WALEntryDeserializer {
    final ByteBuffer intBuffer = ByteBuffer.allocate(WriteAheadLog.sizeOfInt);
    final ByteBuffer longBuffer = ByteBuffer.allocate(WriteAheadLog.sizeOfLong);
    private FileChannel logChannel;

    public WALEntryDeserializer(FileChannel logChannel) {
        this.logChannel = logChannel;
    }

    WALEntry readEntry() {
        try {
            return readEntry(logChannel.position());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    class Header {
        final long headerStartOffset;

        public Header(long headerStartOffset) {
            this.headerStartOffset = headerStartOffset;
        }

        Integer readEntryType() {
            return readInteger(headerStartOffset + WALEntry.Layout.OFFSET_ENTRY_TYPE);
        }
        
        Long readGeneration() {
            return readLong(headerStartOffset + WALEntry.Layout.OFFSET_GENERATION);
        }

        Long readEntryId() {
            return readLong(headerStartOffset + WALEntry.Layout.OFFSET_ENTRY_INDEX);
        }

        Long readEntryTimestamp() {
            return readLong(headerStartOffset + WALEntry.Layout.OFFSET_TIMESTAMP);
        }

        Long readCrc() {
            return readLong(headerStartOffset + WALEntry.Layout.OFFSET_CRC);
        }

        int getSize() {
            return WALEntry.Layout.HEADER_SIZE;
        }
    }

    WALEntry readEntry(long startPosition) {
        // Read length prefix
        Integer entrySize = readInteger(startPosition);
        
        // Read header (starts after length prefix)
        long headerStart = startPosition + WALEntry.Layout.SIZE_LENGTH_PREFIX;
        Header header = new Header(headerStart);
        Integer entryType = header.readEntryType();
        Long generation = header.readGeneration();
        Long entryId = header.readEntryId();
        Long entryTimestamp = header.readEntryTimestamp();
        Long crc = header.readCrc();
        
        // Read data (starts after header)
        int headerSize = header.getSize();
        int dataSize = entrySize - headerSize;
        ByteBuffer buffer = ByteBuffer.allocate(dataSize);
        long dataStart = headerStart + headerSize;
        readFromChannel(logChannel, buffer, dataStart);
        
        return new WALEntry(entryId, buffer.array(), EntryType.valueOf(entryType), generation);
    }

    public Long readLong(long position1) {
        long position = readFromChannel(logChannel, longBuffer, position1);
        return longBuffer.getLong();
    }

    public Integer readInteger(long position) {
        readFromChannel(logChannel, intBuffer, position);
        return intBuffer.getInt();
    }

    private long readFromChannel(FileChannel channel, ByteBuffer buffer, long filePosition) {

        try {
            buffer.clear();//clear to start reading.

            int bytesRead;
            do {
                bytesRead = channel.read(buffer, filePosition);
                filePosition += bytesRead;
            } while (bytesRead != -1 && buffer.hasRemaining());

            buffer.flip(); //read to be read

            return channel.position();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
