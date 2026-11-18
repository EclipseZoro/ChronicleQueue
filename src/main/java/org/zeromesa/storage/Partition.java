package org.zeromesa.storage;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class Partition implements AutoCloseable {
    private final File partitionDir;
    private final long maxSegmentSizeBytes;
    private final int indexIntervalBytes;
    private final int maxIndexEntries;

    // Maps segment baseOffset -> SegmentBundle (allows O(log N) floor lookup for reads)
    private final ConcurrentSkipListMap<Long, SegmentBundle> segments = new ConcurrentSkipListMap<>();
    private final AtomicLong nextOffset = new AtomicLong(0L);
    private volatile SegmentBundle activeSegment;

    private record SegmentBundle(LogSegment log, OffsetIndex index, AtomicLong bytesSinceLastIndex) implements AutoCloseable {
        @Override
        public void close() throws IOException {
            index.close();
            log.close();
        }
    }

    public Partition(File partitionDir, long maxSegmentSizeBytes, int indexIntervalBytes) throws IOException {
        this.partitionDir = partitionDir;
        if (!partitionDir.exists() && !partitionDir.mkdirs()) {
            throw new IOException("Failed to create partition directory: " + partitionDir);
        }
        this.maxSegmentSizeBytes = maxSegmentSizeBytes;
        this.indexIntervalBytes = indexIntervalBytes;
        // Calculate max index entries needed if every indexIntervalBytes triggers an entry
        this.maxIndexEntries = Math.max(16, (int) (maxSegmentSizeBytes / Math.max(1, indexIntervalBytes)) + 16);

        // Initialize the first segment starting at logical offset 0
        rollSegment(0L);
    }

    private synchronized void rollSegment(long newBaseOffset) throws IOException {
        LogSegment newLog = new LogSegment(partitionDir, newBaseOffset, maxSegmentSizeBytes);
        File indexFile = new File(partitionDir, String.format("%020d.index", newBaseOffset));
        OffsetIndex newIndex = new OffsetIndex(indexFile, maxIndexEntries);

        // Initialize bytesSinceLastIndex to indexIntervalBytes so the very first record (offset 0) is always indexed
        SegmentBundle bundle = new SegmentBundle(newLog, newIndex, new AtomicLong(indexIntervalBytes));
        segments.put(newBaseOffset, bundle);
        this.activeSegment = bundle;
    }

    /**
     * Appends a payload to the partition, handling sparse indexing and automatic segment rollover.
     * @return The assigned global logical offset.
     */
    public synchronized long append(byte[] payload) throws IOException {
        long assignedOffset = nextOffset.get();
        long physicalPos = activeSegment.log().append(payload);

        // If -1 is returned, active segment hit maxSegmentSizeBytes -> trigger rollover
        if (physicalPos == -1L) {
            rollSegment(assignedOffset);
            physicalPos = activeSegment.log().append(payload);
            if (physicalPos == -1L) {
                throw new IllegalArgumentException("Single record exceeds maxSegmentSizeBytes");
            }
        }

        int frameSize = Record.HEADER_SIZE + payload.length;
        long accumulatedBytes = activeSegment.bytesSinceLastIndex().addAndGet(frameSize);

        // Sparse Index Trigger: record an entry every N bytes
        if (accumulatedBytes >= indexIntervalBytes) {
            int relativeOffset = (int) (assignedOffset - activeSegment.log().getBaseOffset());
            activeSegment.index().append(relativeOffset, physicalPos);
            activeSegment.bytesSinceLastIndex().set(0L);
        }

        nextOffset.incrementAndGet();
        return assignedOffset;
    }

    /**
     * Reads a record by its global logical offset using:
     * 1. O(log S) SkipList lookup to find the target segment
     * 2. O(log I) mmap binary search to find the closest physical byte position
     * 3. Sequential forward scan to the exact target offset
     */
    public Record read(long targetOffset) throws IOException {
        if (targetOffset < 0 || targetOffset >= nextOffset.get()) {
            return null; // Offset out of bounds
        }

        Map.Entry<Long, SegmentBundle> entry = segments.floorEntry(targetOffset);
        if (entry == null) {
            return null;
        }

        SegmentBundle bundle = entry.getValue();
        long baseOffset = bundle.log().getBaseOffset();
        int relativeTarget = (int) (targetOffset - baseOffset);

        // Step 1: Sparse index binary search for floor physical position
        long currentPhysicalPos = bundle.index().lookup(relativeTarget);

        // Step 2: Determine which logical offset corresponds to that floor physical position
        // Because we walk from the indexed position, we need to find the exact offset of the record at currentPhysicalPos.
        // We scan the sparse index backwards or find the floor relative offset:
        long currentLogicalOffset = findIndexedRelativeOffset(bundle.index(), relativeTarget) + baseOffset;

        // Step 3: Walk sequentially through the .log file until currentLogicalOffset == targetOffset
        while (currentLogicalOffset <= targetOffset) {
            LogSegment.RawFrame frame = bundle.log().readFrame(currentPhysicalPos);
            if (frame == null) {
                return null;
            }
            if (currentLogicalOffset == targetOffset) {
                return new Record(targetOffset, frame.payload());
            }
            currentPhysicalPos += frame.frameSizeBytes();
            currentLogicalOffset++;
        }

        return null;
    }

    private int findIndexedRelativeOffset(OffsetIndex index, int targetRelativeOffset) {
        // Binary search to recover the relative offset matching the floor lookup
        int count = index.getEntriesCount();
        if (count == 0) return 0;

        long targetPos = index.lookup(targetRelativeOffset);
        if (targetPos == 0L) return 0;

        // Linear/binary scan of index entries to match physical position
        int low = 0, high = count - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            // Access via reflection-free helper or expose entry getter
            long midPos = index.getPhysicalPositionAtSlot(mid);
            if (midPos == targetPos) {
                return index.getRelativeOffsetAtSlot(mid);
            } else if (midPos < targetPos) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return 0;
    }

    /**
     * Zero-Copy slice transfer starting at targetOffset directly to a network SocketChannel.
     */
    public long transferZeroCopy(long targetOffset, long maxBytes, java.nio.channels.WritableByteChannel target) throws IOException {
        if (targetOffset < 0 || targetOffset >= nextOffset.get()) {
            return 0L;
        }
        Map.Entry<Long, SegmentBundle> entry = segments.floorEntry(targetOffset);
        if (entry == null) {
            return 0L;
        }
        SegmentBundle bundle = entry.getValue();
        int relativeTarget = (int) (targetOffset - bundle.log().getBaseOffset());
        long physicalPos = bundle.index().lookup(relativeTarget);
        return bundle.log().transferTo(physicalPos, maxBytes, target);
    }

    public int getSegmentCount() {
        return segments.size();
    }

    public long getHighWatermark() {
        return nextOffset.get();
    }

    @Override
    public void close() throws IOException {
        for (SegmentBundle bundle : segments.values()) {
            bundle.close();
        }
    }
}