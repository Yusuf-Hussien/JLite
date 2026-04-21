package jlite.server;

import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import com.fasterxml.jackson.databind.ObjectMapper;

import jlite.catalogue.Column;
import jlite.catalogue.DataType;
import jlite.catalogue.TableSchema;
import jlite.config.StorageConfigResolver;
import jlite.executor.QueryEngine;
import jlite.server.handler.ConnectionHandler;

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
    private final QueryEngine queryEngine;
    private final ConnectionPool connectionPool;
    private final SessionManager sessionManager;
    private final ObjectMapper objectMapper;
    private final CountDownLatch started = new CountDownLatch(1);

    private volatile ServerSocket serverSocket;
    private volatile boolean running = false;

    public TcpServer(int port) {
        this(port, createDefaultQueryEngine());
    }

    public TcpServer(int port, QueryEngine queryEngine) {
        this.port = port;
        this.queryEngine = queryEngine;
        this.connectionPool = new ConnectionPool(32);
        this.sessionManager = new SessionManager();
        this.objectMapper = new ObjectMapper();
    }

    public void start() throws Exception {
        running = true;
        try (var boundSocket = new ServerSocket(port)) {
            serverSocket = boundSocket;
            started.countDown();
            System.out.println("JLite TCP server listening on port " + port);
            while (running) {
                Socket socket = boundSocket.accept();
                Thread.ofVirtual().start(() -> {
                    new ConnectionHandler(socket, queryEngine, sessionManager, connectionPool, objectMapper).handle();
                });
            }
        } catch (SocketException socketException) {
            if (running) {
                throw socketException;
            }
        }
    }

    public void stop() {
        running = false;
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (java.io.IOException ignored) {
            // best effort shutdown
        }
    }

    public int getPort() {
        return serverSocket == null ? port : serverSocket.getLocalPort();
    }

    public void awaitStarted() throws InterruptedException {
        started.await();
    }

    private static QueryEngine createDefaultQueryEngine() {
        var queryEngine = createConfiguredEngine();
        if (queryEngine.catalogue().hasTable("users")) {
            return queryEngine;
        }

        queryEngine.createTable(new TableSchema(
            "users",
            List.of(
                new Column("id", DataType.INT, false, true),
                new Column("name", DataType.TEXT, true, false),
                new Column("age", DataType.INT, true, false),
                new Column("active", DataType.BOOLEAN, true, false)
            )
        ));
        queryEngine.insertRow("users", Map.of("id", 1L, "name", "Alice", "age", 30L, "active", true));
        queryEngine.insertRow("users", Map.of("id", 2L, "name", "Bob", "age", 17L, "active", false));
        queryEngine.insertRow("users", Map.of("id", 3L, "name", "Cara", "age", 41L, "active", true));
        return queryEngine;
    }

    private static QueryEngine createConfiguredEngine() {
        var configuredStorageDir = StorageConfigResolver.resolveStorageDir();
        return configuredStorageDir.map(QueryEngine::new).orElseGet(QueryEngine::new);
    }

    public static void main(String[] args) throws Exception {
        var server = new TcpServer(args.length > 0 ? Integer.parseInt(args[0]) : 5432);
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
        server.start();
    }
}
