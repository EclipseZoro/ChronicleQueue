package org.zeromesa.storage;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public record Record(long offset, byte[] payload) {
    public static final byte MAGIC_BYTE = (byte) 0xBF;
    public static final int HEADER_SIZE = 1 + 4 + 4; // Magic(1) + CRC32(4) + Length(4)

    // Serializes a raw byte array into our 9-byte framed binary envelope.

    public static ByteBuffer serialize(byte[] payload) {
        int totalSize = HEADER_SIZE + payload.length;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        // Compute a 32-bit cyclical redundancy check across the payload
        CRC32 crc = new CRC32();
        crc.update(payload);
        int checksum = (int) crc.getValue();

        buffer.put(MAGIC_BYTE);
        buffer.putInt(checksum);
        buffer.putInt(payload.length);
        buffer.put(payload);

        // Flip buffer from write more to read mode
        buffer.flip();
        return buffer;
    }

    public static byte[] deserialize(ByteBuffer buffer) {
        byte magic = buffer.get();
        if (magic != MAGIC_BYTE) {
            throw new CorruptRecordException(
                    String.format("Invalid magic byte: expected 0x%02x, got 0x%02x", MAGIC_BYTE, magic)
            );
        }
        int checksum = buffer.getInt();
        int length = buffer.getInt();
        byte[] payload = new byte[length];
        buffer.get(payload);

        CRC32 crc = new CRC32();
        crc.update(payload);
        if ((int) crc.getValue() != checksum) {
            throw new CorruptRecordException("Data corruption detected: CRC32 checksum mismatch");
        }
        return payload;
    }

}

