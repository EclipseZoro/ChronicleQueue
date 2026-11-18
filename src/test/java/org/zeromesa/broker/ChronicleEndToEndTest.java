package org.zeromesa.broker;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.zeromesa.network.ZeroCopyTcpServer;
import org.zeromesa.routing.ConsumerGroupQueue;
import org.zeromesa.routing.ExchangeType;
import org.zeromesa.storage.Record;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.*;

public class ChronicleEndToEndTest {
    private Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("chronicle-e2e-");
    }

    @AfterEach
    void tearDown() throws IOException {
        Files.walk(tempDir)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }

    @Test
    void shouldIngestViaTcpPersistToWalRouteViaTrieAndPullZeroCopy() throws Exception {
        try (ChronicleBroker broker = new ChronicleBroker(tempDir.toFile(), 1024, 1024 * 1024, 256);
             ZeroCopyTcpServer server = new ZeroCopyTcpServer(0, broker)) {

            // Bind AMQP-style consumer groups
            broker.bindConsumerGroup(ExchangeType.TOPIC, "orders.*.success", "billing-service");
            broker.bindConsumerGroup(ExchangeType.FANOUT, "", "audit-logger");

            server.start();
            int port = server.getPort();

            // 1. Connect TCP Producer and publish 2 events
            try (SocketChannel producer = SocketChannel.open(new InetSocketAddress("127.0.0.1", port))) {
                sendTcpPublish(producer, "orders.us.success", "ORDER_1001_PAID");
                sendTcpPublish(producer, "orders.eu.failed", "ORDER_1002_DECLINED");
            }

            // Wait briefly for async WAL flusher
            long deadline = System.currentTimeMillis() + 2_000;
            while (broker.getPersistedHighWatermark() < 2 && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            assertEquals(2L, broker.getPersistedHighWatermark());

            // 2. Verify AMQP Topic & Fanout Routing
            ConsumerGroupQueue billingQueue = broker.getConsumerQueue("billing-service");
            ConsumerGroupQueue auditQueue = broker.getConsumerQueue("audit-logger");

            assertEquals(1, billingQueue.size(), "Only orders.*.success should match billing-service");
            assertEquals("ORDER_1001_PAID", new String(billingQueue.poll(500).payload(), StandardCharsets.UTF_8));
            assertEquals(2, auditQueue.size(), "Fanout group must receive all events");

            // 3. Connect TCP Consumer and perform Zero-Copy transferTo pull from offset 0
            try (SocketChannel consumer = SocketChannel.open(new InetSocketAddress("127.0.0.1", port))) {
                ByteBuffer pullReq = ByteBuffer.allocate(1 + 8 + 8);
                pullReq.put(ZeroCopyTcpServer.CMD_PULL_ZERO_COPY);
                pullReq.putLong(0L);   // start offset
                pullReq.putLong(1024L); // max bytes
                pullReq.flip();
                consumer.write(pullReq);

                // Read raw binary framed record streamed directly from OS Page Cache
                ByteBuffer rawStream = ByteBuffer.allocate(256);
                int bytesRead = consumer.read(rawStream);
                assertTrue(bytesRead > Record.HEADER_SIZE);
                rawStream.flip();

                byte[] firstPayload = Record.deserialize(rawStream);
                assertEquals("ORDER_1001_PAID", new String(firstPayload, StandardCharsets.UTF_8));
            }
        }
    }

    private void sendTcpPublish(SocketChannel ch, String key, String payload) throws IOException {
        byte[] k = key.getBytes(StandardCharsets.UTF_8);
        byte[] p = payload.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + k.length + 4 + p.length);
        buf.put(ZeroCopyTcpServer.CMD_PUB);
        buf.putInt(k.length);
        buf.put(k);
        buf.putInt(p.length);
        buf.put(p);
        buf.flip();
        ch.write(buf);

        ByteBuffer ack = ByteBuffer.allocate(8);
        while (ack.hasRemaining()) ch.read(ack);
    }
}