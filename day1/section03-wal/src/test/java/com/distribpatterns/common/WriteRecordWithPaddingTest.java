package com.distribpatterns.common;

import com.distribpatterns.wal.WALSegment;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WriteRecordWithPaddingTest {

    @Test
    public void addPaddingBytesToDataIfNoAlignedOnLogicalBlockSize() throws IOException {
        WALSegment segment = WALSegment.open(File.createTempFile("test", ".log"));
        assertEquals(4096, segment.logicalBlockSize);

        //should add padding bytes to make this entry aligned on logical block size of 4096
        ByteBuffer byteBuffer = segment.addPaddingBytesIfNeeded(ByteBuffer.wrap(new byte[1028]));
        assertEquals(4096, byteBuffer.capacity());
    }

    @Test
    public void addNextLogicalBlockIfRemainingBytesCanNotAccomodatePaddingMarker() throws IOException {
        WALSegment segment = WALSegment.open(File.createTempFile("test1" , ".log"));
        assertEquals(4096, segment.logicalBlockSize);

        //should add padding bytes to make this entry aligned on logical block size of 4096
        ByteBuffer byteBuffer = segment.addPaddingBytesIfNeeded(ByteBuffer.wrap(new byte[4095]));
        assertEquals(8192, byteBuffer.limit());
    }
}