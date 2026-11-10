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

public class OffsetIndexTest {
    private Path tempDir;
    private File indexFile;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("zeromesa-index-test-");
        indexFile = new File(tempDir.toFile(), "00000000000000000000.index");
    }

    @AfterEach
    void tearDown() throws IOException {
        Files.walk(tempDir)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }

    @Test
    void shouldAppendAndPerformBinarySearchCorrectly() throws IOException {
        int maxEntries = 100;
        try (OffsetIndex index = new OffsetIndex(indexFile, maxEntries)) {
            // Simulate sparse entries added every few records
            index.append(0, 0L);       // Message 0 starts at byte 0
            index.append(5, 520L);     // Message 5 starts at byte 520
            index.append(10, 1150L);   // Message 10 starts at byte 1150
            index.append(20, 2400L);   // Message 20 starts at byte 2400

            assertEquals(4, index.getEntriesCount());

            // Exact hits
            assertEquals(0L, index.lookup(0));
            assertEquals(520L, index.lookup(5));
            assertEquals(1150L, index.lookup(10));
            assertEquals(2400L, index.lookup(20));

            // Sparse lookups: target falls in between indexed offsets
            // Offset 7 should return position of 5 (520L) so reader can scan forward to 7
            assertEquals(520L, index.lookup(7));

            // Offset 15 should return position of 10 (1150L)
            assertEquals(1150L, index.lookup(15));

            // Offset 99 (beyond highest) should return position of 20 (2400L)
            assertEquals(2400L, index.lookup(99));
        }
    }
}