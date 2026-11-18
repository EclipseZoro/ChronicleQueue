package org.zeromesa;

import org.zeromesa.broker.ChronicleBroker;
import org.zeromesa.network.ZeroCopyTcpServer;
import org.zeromesa.routing.ExchangeType;

import java.io.File;

public class ChronicleApp {
    public static void main(String[] args) throws Exception {
        File dataDir = new File("chronicle-data");
        int port = 9092;
        int ringBufferSize = 4096;               // Power of 2
        long maxSegmentBytes = 1024 * 1024;      // 1 MB per segment (so you can see segment rollovers quickly)
        int indexIntervalBytes = 512;            // Write sparse index entry every 512 bytes

        ChronicleBroker broker = new ChronicleBroker(dataDir, ringBufferSize, maxSegmentBytes, indexIntervalBytes);

        // Register default AMQP-style consumer group bindings
        broker.bindConsumerGroup(ExchangeType.TOPIC, "orders.*.success", "billing-service");
        broker.bindConsumerGroup(ExchangeType.TOPIC, "orders.#", "order-analytics");
        broker.bindConsumerGroup(ExchangeType.FANOUT, "", "global-audit-log");

        ZeroCopyTcpServer server = new ZeroCopyTcpServer(port, broker);
        server.start();

        System.out.println("=========================================================");
        System.out.println(" ChronicleQueue Engine RUNNING on TCP port " + port);
        System.out.println(" Storage Directory : " + dataDir.getAbsolutePath());
        System.out.println(" Ring Buffer Slots : " + ringBufferSize + " (Lock-Free MPSC)");
        System.out.println(" Segment Rollover  : " + (maxSegmentBytes / 1024) + " KB");
        System.out.println("=========================================================");

        // Graceful shutdown hook (flushes WAL fsync on Ctrl+C / Stop)
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down ChronicleQueue cleanly...");
            try {
                server.close();
                broker.close();
                System.out.println("WAL flushed and closed.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));

        // Keep main thread alive
        Thread.currentThread().join();
    }
}