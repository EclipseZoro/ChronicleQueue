package org.zeromesa.concurrency;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.concurrent.locks.LockSupport;

/**
 * Lock-free, multi-producer single-consumer (MPSC) ring buffer modeled after the LMAX Disruptor.
 */
public class LockFreeRingBuffer {
    private final RingBufferSlot[] slots;
    private final int bufferSize;
    private final int indexMask;
    private final int indexShift;

    // Tracks the highest sequence number committed in each slot (solves out-of-order multi-producer commits)
    private final long[] committedSequences;
    private static final VarHandle LONG_ARRAY_HANDLE = MethodHandles.arrayElementVarHandle(long[].class);

    // Cache-line isolated cursors
    private final PaddedSequence producerClaimCursor = new PaddedSequence(-1L);
    private final PaddedSequence consumerCursor = new PaddedSequence(-1L);

    public LockFreeRingBuffer(int bufferSize) {
        if (bufferSize <= 0 || (bufferSize & (bufferSize - 1)) != 0) {
            throw new IllegalArgumentException("bufferSize must be a positive power of 2, got: " + bufferSize);
        }
        this.bufferSize = bufferSize;
        this.indexMask = bufferSize - 1;
        this.indexShift = Integer.numberOfTrailingZeros(bufferSize);

        this.slots = new RingBufferSlot[bufferSize];
        this.committedSequences = new long[bufferSize];
        Arrays.fill(this.committedSequences, -1L);

        for (int i = 0; i < bufferSize; i++) {
            this.slots[i] = new RingBufferSlot();
        }
    }

    /**
     * Publishes an event into the ring buffer without acquiring any locks.
     * Uses CAS to claim a slot and StoreRelease memory fences to publish visibility.
     *
     * @return The ring buffer sequence number assigned to this event.
     */
    public long publish(String routingKey, byte[] payload) {
        long currentClaim;
        long nextClaim;

        // 1. Lock-free CAS loop to claim the next sequence number
        do {
            currentClaim = producerClaimCursor.getOpaque();
            nextClaim = currentClaim + 1;

            // Backpressure check: ensure we do not lap the consumer cursor
            long wrapPoint = nextClaim - bufferSize;
            while (wrapPoint > consumerCursor.getAcquire()) {
                // Buffer is full; park briefly for 1 microsecond to yield CPU without kernel lock overhead
                LockSupport.parkNanos(1_000L);
            }
        } while (!producerClaimCursor.compareAndSet(currentClaim, nextClaim));

        // 2. Write payload into the exclusively claimed pre-allocated slot
        int slotIndex = (int) (nextClaim & indexMask);
        slots[slotIndex].set(routingKey, payload);

        // 3. Publish availability using setRelease so payload writes are guaranteed visible first
        LONG_ARRAY_HANDLE.setRelease(committedSequences, slotIndex, nextClaim);
        return nextClaim;
    }

    /**
     * Polls and processes up to maxBatchSize contiguous committed events from the ring buffer.
     * Designed to be called by the async disk-flusher thread.
     *
     * @return Number of events processed in this batch.
     */
    public int drainBatch(int maxBatchSize, EventHandler handler) throws Exception {
        long nextConsumerSeq = consumerCursor.getOpaque() + 1;
        int processed = 0;

        while (processed < maxBatchSize) {
            int slotIndex = (int) (nextConsumerSeq & indexMask);
            long committedSeq = (long) LONG_ARRAY_HANDLE.getAcquire(committedSequences, slotIndex);

            // Stop if this slot has not yet been committed for nextConsumerSeq
            if (committedSeq != nextConsumerSeq) {
                break;
            }

            RingBufferSlot slot = slots[slotIndex];
            handler.onEvent(slot, nextConsumerSeq);
            slot.clear(); // Allow payload GC while retaining slot object

            processed++;
            nextConsumerSeq++;
        }

        if (processed > 0) {
            // Advance consumer cursor so producers can reuse these slots
            consumerCursor.setRelease(nextConsumerSeq - 1);
        }

        return processed;
    }

    public long getProducerCursor() {
        return producerClaimCursor.getAcquire();
    }

    public long getConsumerCursor() {
        return consumerCursor.getAcquire();
    }

    @FunctionalInterface
    public interface EventHandler {
        void onEvent(RingBufferSlot slot, long sequence) throws Exception;
    }
}