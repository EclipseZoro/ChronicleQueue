package org.zeromesa.concurrency;

public class RingBufferSlot {
    private String routingKey;
    private byte[] payload;
    private long assignedOffset;

    public void set(String routingKey, byte[] payload) {
        this.routingKey = routingKey;
        this.payload = payload;
        this.assignedOffset = -1L;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public byte[] getPayload() {
        return payload;
    }

    public long getAssignedOffset() {
        return assignedOffset;
    }

    public void setAssignedOffset(long assignedOffset) {
        this.assignedOffset = assignedOffset;
    }

    public void clear() {
        this.routingKey = null;
        this.payload = null;
    }
}