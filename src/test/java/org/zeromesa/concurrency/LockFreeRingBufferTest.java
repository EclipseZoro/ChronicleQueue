package org.zeromesa.concurrency;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class LockFreeRingBufferTest {

    @Test
    void shouldHandleConcurrentProducersAndPreserveAllEventsWithoutLocks() throws Exception {
        int ringSize = 1024; // Power of 2
        LockFreeRingBuffer ringBuffer = new LockFreeRingBuffer(ringSize);

        int producerThreads = 8;
        int messagesPerProducer = 5_000;
        int totalExpected = producerThreads * messagesPerProducer;

        Set<String> receivedPayloads = ConcurrentHashMap.newKeySet();
        AtomicInteger consumedCount = new AtomicInteger(0);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch producersDone = new CountDownLatch(producerThreads);

        // Start consumer thread simulating the Async WAL Flusher
        Thread consumerThread = Thread.ofPlatform().start(() -> {
            try {
                while (consumedCount.get() < totalExpected) {
                    int drained = ringBuffer.drainBatch(64, (slot, seq) -> {
                        receivedPayloads.add(new String(slot.getPayload(), StandardCharsets.UTF_8));
                        consumedCount.incrementAndGet();
                    });
                    if (drained == 0) {
                        Thread.onSpinWait();
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // Launch 8 concurrent producer threads
        try (ExecutorService producers = Executors.newFixedThreadPool(producerThreads)) {
            for (int p = 0; p < producerThreads; p++) {
                final int producerId = p;
                producers.submit(() -> {
                    try {
                        startLatch.await();
                        for (int m = 0; m < messagesPerProducer; m++) {
                            String msg = "P" + producerId + "-MSG-" + m;
                            ringBuffer.publish("orders.created", msg.getBytes(StandardCharsets.UTF_8));
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        producersDone.countDown();
                    }
                });
            }

            startLatch.countDown(); // Release all producers simultaneously
            producersDone.await();
            consumerThread.join(5_000);
        }

        assertEquals(totalExpected, consumedCount.get(), "Consumer must drain every published event");
        assertEquals(totalExpected, receivedPayloads.size(), "No duplicate or overwritten events allowed");
    }
}