package org.zeromesa.routing;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ConsumerGroupQueue {
    private final String groupId;
    private final LinkedBlockingQueue<RoutedEvent> buffer;

    public record RoutedEvent(long partitionOffset, String routingKey, byte[] payload) {}

    public ConsumerGroupQueue(String groupId, int capacity) {
        this.groupId = groupId;
        this.buffer = new LinkedBlockingQueue<>(capacity);
    }

    public boolean offer(RoutedEvent event) {
        return buffer.offer(event);
    }

    public RoutedEvent poll(long timeoutMs) throws InterruptedException {
        return buffer.poll(timeoutMs, TimeUnit.MILLISECONDS);
    }

    public int size() {
        return buffer.size();
    }

    public String getGroupId() {
        return groupId;
    }
}