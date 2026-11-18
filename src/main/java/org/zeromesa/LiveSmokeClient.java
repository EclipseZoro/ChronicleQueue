package org.zeromesa;

import org.zeromesa.network.ZeroCopyTcpServer;
import org.zeromesa.storage.Record;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

public class LiveSmokeClient {
    public static void main(String[] args) throws Exception {
        String host = "127.0.0.1";
        int port = 9092;

        System.out.println("Connecting to ChronicleQueue at " + host + ":" + port + "...");

        try (SocketChannel channel = SocketChannel.open(new InetSocketAddress(host, port))) {
            // 1. Publish functional sample events
            sendPublish(channel, "orders.india.success", "{\"orderId\":101,\"amount\":4500}");
            sendPublish(channel, "orders.us.failed", "{\"orderId\":102,\"reason\":\"TIMEOUT\"}");
            sendPublish(channel, "payments.upi.settled", "{\"txnId\":\"TXN-9981\"}");
            System.out.println("[1/3] Published 3 functional events over TCP.");

            // 2. Throughput Benchmark: Blast 50,000 events over TCP
            int burstCount = 50_000;
            byte[] samplePayload = "BENCHMARK_TELEMETRY_EVENT_PAYLOAD_DATA_001".getBytes(StandardCharsets.UTF_8);
            long startNs = System.nanoTime();

            for (int i = 0; i < burstCount; i++) {
                sendPublishBytes(channel, "orders.india.success", samplePayload);
            }

            long elapsedMs = (System.nanoTime() - startNs) / 1_000_000L;
            double msgsPerSec = (burstCount * 1000.0) / Math.max(1, elapsedMs);
            System.out.printf("[2/3] Ingested %,d events in %d ms (%,.0f msgs/sec over TCP)%n",
                    burstCount, elapsedMs, msgsPerSec);

            // Allow async flusher a brief moment to commit tail batch
            Thread.sleep(100);

            // 3. Zero-Copy Consumer Pull from Offset 0
            ByteBuffer pullReq = ByteBuffer.allocate(1 + 8 + 8);
            pullReq.put(ZeroCopyTcpServer.CMD_PULL_ZERO_COPY);
            pullReq.putLong(0L);     // Start at logical offset 0
            pullReq.putLong(2048L);  // Stream up to 2 KB directly from OS Page Cache
            pullReq.flip();
            channel.write(pullReq);

            ByteBuffer responseBuf = ByteBuffer.allocate(2048);
            int bytesRead = channel.read(responseBuf);
            responseBuf.flip();

            System.out.println("[3/3] Zero-Copy Pull received " + bytesRead + " raw bytes from OS Page Cache.");
            int decodedCount = 0;
            while (responseBuf.remaining() >= Record.HEADER_SIZE) {
                responseBuf.mark();
                int payloadLen = responseBuf.getInt(responseBuf.position() + 5);
                if (responseBuf.remaining() < Record.HEADER_SIZE + payloadLen) {
                    responseBuf.reset();
                    break;
                }
                byte[] decoded = Record.deserialize(responseBuf);
                if (decodedCount < 3) {
                    System.out.println("      -> Record #" + decodedCount + ": " + new String(decoded, StandardCharsets.UTF_8));
                }
                decodedCount++;
            }
        }
    }

    private static void sendPublish(SocketChannel ch, String routingKey, String message) throws IOException {
        sendPublishBytes(ch, routingKey, message.getBytes(StandardCharsets.UTF_8));
    }

    private static void sendPublishBytes(SocketChannel ch, String routingKey, byte[] payload) throws IOException {
        byte[] keyBytes = routingKey.getBytes(StandardCharsets.UTF_8);
        ByteBuffer frame = ByteBuffer.allocate(1 + 4 + keyBytes.length + 4 + payload.length);
        frame.put(ZeroCopyTcpServer.CMD_PUB);
        frame.putInt(keyBytes.length);
        frame.put(keyBytes);
        frame.putInt(payload.length);
        frame.put(payload);
        frame.flip();

        while (frame.hasRemaining()) {
            ch.write(frame);
        }

        ByteBuffer ack = ByteBuffer.allocate(8);
        while (ack.hasRemaining()) {
            ch.read(ack);
        }
    }
}