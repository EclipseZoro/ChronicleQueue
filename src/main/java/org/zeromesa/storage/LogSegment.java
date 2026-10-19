package org.zeromesa.storage;

import java.io.File;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

public class LogSegment implements AutoCloseable {
    private final File file;
    private final FileChannel channel;
    private final long baseOffset;
    private final AtomicLong currentPhysicalOffset;
    private final long maxSegmentSizeByte;

    public LogSegment(File baseDir, long baseOffset, long maxSegmentSizeByte) throws IOException {
        this.baseOffset = baseOffset;
        this.maxSegmentSizeByte = maxSegmentSizeByte;

        // 20-digit zero-padded file naming convention
        String fileName = String.format("%020d.log", baseOffset);
        this.file = new File(baseDir, fileName);

        //Open in read-write mode
        RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
        this.channel = raf.getChannel();
        this.currentPhysicalOffset = new AtomicLong(channel.size());
    }

    // Appends a payload sequentially to the segment file
    // @return Physical byte position of the record in the file , or -1 if segment is full.

    public synchronized long append(byte[] payload) throws IOException {
        ByteBuffer frame = Record.serialize(payload);
        long writePosition = currentPhysicalOffset.get();

        if (writePosition + frame.remaining() > maxSegmentSizeByte) {
            return -1;
        }

        while (frame.hasRemaining()) {
            channel.write(frame,  writePosition);
        }

        currentPhysicalOffset.addAndGet(frame.position());
        return writePosition;
    }

    // reads a record payload starting at a given physical byte offset

    public byte[] read(long physicalPosition) throws IOException {
        ByteBuffer headerBuffer =  ByteBuffer.allocate(Record.HEADER_SIZE);
        channel.read(headerBuffer, physicalPosition);
        headerBuffer.flip();

        byte magic = headerBuffer.get();
        if (magic != Record.MAGIC_BYTE){
            throw new CorruptRecordException("Corrupt record header at postion: " + physicalPosition);
        }

        int checksum = headerBuffer.getInt();
        int payloadLength = headerBuffer.getInt();

        // read the variable length payload
        ByteBuffer payloadBuffer = ByteBuffer.allocate(payloadLength);
        channel.read(payloadBuffer, physicalPosition + Record.HEADER_SIZE);
        payloadBuffer.flip();

        // validate integrity
        CRC32 crc = new CRC32();
        crc.update(payloadBuffer.array());
        if ((int) crc.getValue() != checksum) {
            throw new CorruptRecordException("CRC32 mismatch at postion: " + physicalPosition);
        }
        return payloadBuffer.array();

    }

    public long getBaseOffset(){
        return baseOffset;
    }

    public long getCurrentSize(){
        return currentPhysicalOffset.get();
    }

    public FileChannel getChannel(){
        return channel;
    }

    @Override
    public void close() throws IOException {
        if (channel.isOpen()) {
            // force the OS dirty pages from OS page cache down to physical storage
            channel.force(true);
            channel.close();
        }
    }

}
