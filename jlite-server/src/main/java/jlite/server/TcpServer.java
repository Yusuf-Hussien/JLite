package jlite.server;

import java.net.ServerSocket;

/**
 * TCP entry point for remote JLite access.
 *
 * One virtual thread is spawned per accepted connection — no explicit thread pool.
 * The main accept loop runs on a virtual thread as well, keeping the carrier thread free.
 *
 * TODO: TLS support via SSLServerSocket.
 * TODO: configurable bind address, max connections, backlog.
 * TODO: graceful shutdown: stop accepting, drain in-flight requests, close idle connections.
 */
public class TcpServer {

    private final int port;
    private volatile boolean running = false;

    public TcpServer(int port) {
        this.port = port;
    }

    public void start() throws Exception {
        running = true;
        try (var server = new ServerSocket(port)) {
            System.out.println("JLite TCP server listening on port " + port);
            while (running) {
                var socket = server.accept();
                Thread.ofVirtual().start(() -> {
                    // TODO: hand off to ConnectionHandler
                });
            }
        }
    }

    public void stop() { running = false; }
}
