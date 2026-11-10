package org.zeromesa.storage;

public record IndexEntry(int relativeOffset, long physicalPosition) {
    public static final int ENTRY_SIZE_BYTES = 4+8; // integer (4B) + long (8B) = 12 bytes
}
