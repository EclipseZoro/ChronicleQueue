package org.zeromesa.concurrency;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * A 64-byte cache-line isolated sequence counter using Java VarHandle memory barriers.
 * Prevents CPU false sharing between producer and consumer cores.
 */
public class PaddedSequence {
    // Left padding: 7 longs = 56 bytes (prevents sharing cache line with preceding objects)
    @SuppressWarnings("unused")
    private long p1, p2, p3, p4, p5, p6, p7;

    // The actual 8-byte cursor value (total 56 + 8 = 64 bytes)
    private volatile long value;

    // Right padding: 7 longs = 56 bytes (prevents sharing cache line with trailing fields)
    @SuppressWarnings("unused")
    private long p8, p9, p10, p11, p12, p13, p14;

    private static final VarHandle VALUE_HANDLE;

    static {
        try {
            VALUE_HANDLE = MethodHandles.lookup()
                    .findVarHandle(PaddedSequence.class, "value", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public PaddedSequence(long initialValue) {
        this.value = initialValue;
    }

    public long getAcquire() {
        return (long) VALUE_HANDLE.getAcquire(this);
    }

    public long getOpaque() {
        return (long) VALUE_HANDLE.getOpaque(this);
    }

    public void setRelease(long newValue) {
        VALUE_HANDLE.setRelease(this, newValue);
    }

    public boolean compareAndSet(long expected, long update) {
        return VALUE_HANDLE.compareAndSet(this, expected, update);
    }
}