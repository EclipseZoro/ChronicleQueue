package org.zeromesa.network;

import org.zeromesa.broker.ChronicleBroker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Custom binary TCP server powered by Java 21 Virtual Threads.
 * Commands:
 *   [0x01][keyLen:int][keyBytes][payloadLen:int][payloadBytes] -> PUB (returns 8-byte seq)
 *   [0x02][offset:long][maxBytes:long]                        -> ZERO-COPY PULL (transferTo socket)
 */
public class ZeroCopyTcpServer implements AutoCloseable {
    public static final byte CMD_PUB = 0x01;
    public static final byte CMD_PULL_ZERO_COPY = 0x02;

    private final ChronicleBroker broker;
    private final ServerSocketChannel serverChannel;
    private final ExecutorService virtualExecutor;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public ZeroCopyTcpServer(int port, ChronicleBroker broker) throws IOException {
        this.broker = broker;
        this.serverChannel = ServerSocketChannel.open();
        this.serverChannel.bind(new InetSocketAddress(port));
        this.virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public void start() {
        virtualExecutor.submit(() -> {
            while (running.get()) {
                try {
                    SocketChannel client = serverChannel.accept();
                    virtualExecutor.submit(() -> handleClient(client));
                } catch (IOException e) {
                    if (running.get()) e.printStackTrace();
                }
            }
        });
    }

    private void handleClient(SocketChannel client) {
        try (client) {
            ByteBuffer cmdBuf = ByteBuffer.allocate(1);
            while (running.get() && client.read(cmdBuf) > 0) {
                cmdBuf.flip();
                byte cmd = cmdBuf.get();
                cmdBuf.clear();

                if (cmd == CMD_PUB) {
                    int keyLen = readInt(client);
                    byte[] keyBytes = readBytes(client, keyLen);
                    int payloadLen = readInt(client);
                    byte[] payload = readBytes(client, payloadLen);

                    long seq = broker.publish(new String(keyBytes, StandardCharsets.UTF_8), payload);
                    ByteBuffer ack = ByteBuffer.allocate(8).putLong(seq).flip();
                    client.write(ack);

                } else if (cmd == CMD_PULL_ZERO_COPY) {
                    ByteBuffer req = readBuffer(client, 16);
                    long offset = req.getLong();
                    long maxBytes = req.getLong();

                    // Zero-Copy transfer straight from OS Page Cache to client SocketChannel
                    broker.transferZeroCopy(offset, maxBytes, client);
                }
            }
        } catch (IOException ignored) {
        }
    }

    private int readInt(SocketChannel ch) throws IOException {
        return readBuffer(ch, 4).getInt();
    }

    private byte[] readBytes(SocketChannel ch, int len) throws IOException {
        byte[] arr = new byte[len];
        readBuffer(ch, len).get(arr);
        return arr;
    }

    private ByteBuffer readBuffer(SocketChannel ch, int size) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(size);
        while (buf.hasRemaining()) {
            if (ch.read(buf) == -1) throw new IOException("EOF");
        }
        buf.flip();
        return buf;
    }

    public int getPort() throws IOException {
        return ((InetSocketAddress) serverChannel.getLocalAddress()).getPort();
    }

    @Override
    public void close() throws IOException {
        running.set(false);
        serverChannel.close();
        virtualExecutor.close();
    }
}