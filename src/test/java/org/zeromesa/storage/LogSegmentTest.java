package org.zeromesa.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.*;

public class LogSegmentTest {
    private Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("zeromesa-test-");
    }

    @AfterEach
    void tearDown() throws IOException {
        Files.walk(tempDir)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }

    @Test
    void shouldWriteAndReadRecordsConsistently() throws IOException {
        long segmentThreshold = 1024 * 1024; // 1MB
        try (LogSegment segment = new LogSegment(tempDir.toFile(), 0L, segmentThreshold)) {
            byte[] msg1 = "EVENT_PAYLOAD_ALPHA".getBytes();
            byte[] msg2 = "EVENT_PAYLOAD_BETA".getBytes();

            long pos1 = segment.append(msg1);
            long pos2 = segment.append(msg2);

            assertTrue(pos1 >= 0);
            assertTrue(pos2 > pos1);

            byte[] retrieved1 = segment.read(pos1);
            byte[] retrieved2 = segment.read(pos2);

            assertArrayEquals(msg1, retrieved1);
            assertArrayEquals(msg2, retrieved2);
        }
    }

    @Test
    void shouldSignalRolloverWhenSegmentCapacityExceeded() throws IOException {
        long smallThreshold = 50; // 50 bytes forces an immediate rollover
        try (LogSegment segment = new LogSegment(tempDir.toFile(), 0L, smallThreshold)) {
            byte[] payload = "MESSAGE_EXCEEDING_LIMIT".getBytes();
            long pos1 = segment.append(payload);
            assertEquals(0L, pos1);

            long pos2 = segment.append(payload);
            assertEquals(-1L, pos2); // Must trigger rollover signal
        }
    }
}