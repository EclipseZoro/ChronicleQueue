package org.zeromesa.storage;

import java.io.File;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

public class OffsetIndex implements AutoCloseable {
    private final File file;
    private final FileChannel channel;
    private final MappedByteBuffer mappedBuffer;
    private final AtomicInteger entriesCount;
    private final int maxEntries;

    public OffsetIndex(File file, int maxEntries) throws IOException {
        this.file = file;
        this.maxEntries = maxEntries;
        int maxSizeBytes = maxEntries * IndexEntry.ENTRY_SIZE_BYTES;

        RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
        long initialFileLength = raf.length();

        // Ensure disk backing is sized for mmap
        if (initialFileLength < maxSizeBytes) {
            raf.setLength(maxSizeBytes);
        }

        this.channel = raf.getChannel();
        this.mappedBuffer = this.channel.map(FileChannel.MapMode.READ_WRITE, 0, maxSizeBytes);

        // If file was newly created or empty before resize, start count at 0
        if (initialFileLength == 0) {
            this.entriesCount = new AtomicInteger(0);
        } else {
            // Reopening existing populated index: find the high watermark
            int count = 0;
            while (count < maxEntries) {
                int pos = count * IndexEntry.ENTRY_SIZE_BYTES;
                // Once we hit unwritten space (both offset and pos are 0 for count > 0)
                if (count > 0 && mappedBuffer.getInt(pos) == 0 && mappedBuffer.getLong(pos + 4) == 0L) {
                    break;
                }
                count++;
            }
            this.entriesCount = new AtomicInteger(count);
        }
    }

    /**
     * Appends an entry into the memory-mapped sparse index.
     */
    public synchronized void append(int relativeOffset, long physicalPosition) {
        if (entriesCount.get() >= maxEntries) {
            throw new IllegalStateException("OffsetIndex is full. Entries count: " + entriesCount.get());
        }

        int writeSlot = entriesCount.get() * IndexEntry.ENTRY_SIZE_BYTES;
        mappedBuffer.putInt(writeSlot, relativeOffset);
        mappedBuffer.putLong(writeSlot + 4, physicalPosition);

        entriesCount.incrementAndGet();
    }

    /**
     * Finds the largest physical file position whose relative offset is <= targetOffset
     * using Binary Search across the off-heap MappedByteBuffer.
     */
    public synchronized long lookup(int targetOffset) {
        int count = entriesCount.get();
        if (count == 0) {
            return 0L;
        }

        int low = 0;
        int high = count - 1;
        int bestSlot = -1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int midOffset = mappedBuffer.getInt(mid * IndexEntry.ENTRY_SIZE_BYTES);

            if (midOffset == targetOffset) {
                return mappedBuffer.getLong((mid * IndexEntry.ENTRY_SIZE_BYTES) + 4);
            } else if (midOffset < targetOffset) {
                bestSlot = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        if (bestSlot == -1) {
            return 0L;
        }

        return mappedBuffer.getLong((bestSlot * IndexEntry.ENTRY_SIZE_BYTES) + 4);
    }

    public int getEntriesCount() {
        return entriesCount.get();
    }

    @Override
    public void close() throws IOException {
        if (mappedBuffer != null) {
            mappedBuffer.force(); // fsync on memory map
        }
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
    }
}