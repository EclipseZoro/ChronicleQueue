package org.zeromesa.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.*;

public class PartitionTest {
    private Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("zeromesa-partition-test-");
    }

    @AfterEach
    void tearDown() throws IOException {
        Files.walk(tempDir)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }

    @Test
    void shouldRollOverSegmentsAndReadArbitraryLogicalOffsets() throws IOException {
        // 500 bytes per segment, index entry written every 128 bytes
        long maxSegmentBytes = 500L;
        int indexIntervalBytes = 128;

        try (Partition partition = new Partition(tempDir.toFile(), maxSegmentBytes, indexIntervalBytes)) {
            int totalMessages = 100;

            for (int i = 0; i < totalMessages; i++) {
                String payload = "EVENT_PAYLOAD_NUMBER_" + i;
                long offset = partition.append(payload.getBytes(StandardCharsets.UTF_8));
                assertEquals(i, offset);
            }

            // 100 messages * ~32 bytes = ~3200 bytes -> must have rolled across ~7 segments
            assertTrue(partition.getSegmentCount() > 1, "Expected multiple rolled segments");

            // Verify random access across segment boundaries and between sparse index points
            for (int i = 0; i < totalMessages; i++) {
                Record record = partition.read(i);
                assertNotNull(record, "Record missing at offset " + i);
                assertEquals(i, record.offset());
                assertEquals("EVENT_PAYLOAD_NUMBER_" + i, new String(record.payload(), StandardCharsets.UTF_8));
            }
        }
    }
}