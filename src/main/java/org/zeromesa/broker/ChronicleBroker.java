package org.zeromesa.broker;

import org.zeromesa.concurrency.LockFreeRingBuffer;
import org.zeromesa.routing.ConsumerGroupQueue;
import org.zeromesa.routing.ExchangeType;
import org.zeromesa.routing.TopicRoutingTrie;
import org.zeromesa.storage.Partition;
import org.zeromesa.storage.Record;

import java.io.File;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;

public class ChronicleBroker implements AutoCloseable {
    private final LockFreeRingBuffer ringBuffer;
    private final Partition partition;
    private final TopicRoutingTrie router;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread flusherThread;

    public ChronicleBroker(File dataDir, int ringBufferSize, long maxSegmentBytes, int indexIntervalBytes) throws IOException {
        this.ringBuffer = new LockFreeRingBuffer(ringBufferSize);
        this.partition = new Partition(dataDir, maxSegmentBytes, indexIntervalBytes);
        this.router = new TopicRoutingTrie();

        // Start dedicated Async WAL Flusher platform thread
        this.flusherThread = Thread.ofPlatform().name("wal-async-flusher").start(this::flushLoop);
    }

    public long publish(String routingKey, byte[] payload) {
        return ringBuffer.publish(routingKey, payload);
    }

    public void bindConsumerGroup(ExchangeType type, String pattern, String groupId) {
        router.bind(type, pattern, groupId);
    }

    public ConsumerGroupQueue getConsumerQueue(String groupId) {
        return router.getGroupQueue(groupId);
    }

    public Record readAtOffset(long offset) throws IOException {
        return partition.read(offset);
    }

    public long transferZeroCopy(long offset, long maxBytes, WritableByteChannel target) throws IOException {
        return partition.transferZeroCopy(offset, maxBytes, target);
    }

    private void flushLoop() {
        while (running.get() || ringBuffer.getProducerCursor() > ringBuffer.getConsumerCursor()) {
            try {
                int drained = ringBuffer.drainBatch(128, (slot, seq) -> {
                    long assignedOffset = partition.append(slot.getPayload());
                    slot.setAssignedOffset(assignedOffset);
                    router.route(assignedOffset, slot.getRoutingKey(), slot.getPayload());
                });
                if (drained == 0) {
                    Thread.onSpinWait();
                }
            } catch (Exception e) {
                if (running.get()) {
                    e.printStackTrace();
                }
            }
        }
    }

    public long getPersistedHighWatermark() {
        return partition.getHighWatermark();
    }

    @Override
    public void close() throws Exception {
        running.set(false);
        flusherThread.join(3_000);
        partition.close();
    }
}