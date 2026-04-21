package jlite.server.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import jlite.executor.QueryEngine;
import jlite.executor.QueryResult;
import jlite.server.ConnectionPool;
import jlite.server.SessionManager;
import jlite.server.protocol.ServerRequest;
import jlite.server.protocol.ServerResponse;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class ConnectionHandler {

    private final Socket socket;
    private final QueryEngine queryEngine;
    private final SessionManager sessionManager;
    private final ConnectionPool connectionPool;
    private final ObjectMapper objectMapper;

    public ConnectionHandler(Socket socket, QueryEngine queryEngine, SessionManager sessionManager, ConnectionPool connectionPool, ObjectMapper objectMapper) {
        this.socket = socket;
        this.queryEngine = queryEngine;
        this.sessionManager = sessionManager;
        this.connectionPool = connectionPool;
        this.objectMapper = objectMapper;
    }

    public void handle() {
        var sessionId = UUID.randomUUID().toString();
        try {
            connectionPool.acquire();
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            return;
        }

        try {
            sessionManager.createSession(sessionId);

            var input = new DataInputStream(socket.getInputStream());
            var output = new DataOutputStream(socket.getOutputStream());
            while (true) {
                var request = readRequest(input);
                if (request == null) {
                    break;
                }

                if ("CLOSE".equalsIgnoreCase(request.type())) {
                    safeWriteResponse(output, new ServerResponse("OK", null, null, "closed"));
                    break;
                }

                if (!"QUERY".equalsIgnoreCase(request.type())) {
                    safeWriteResponse(output, new ServerResponse("ERROR", null, null, "Unsupported request type: " + request.type()));
                    continue;
                }

                QueryResult result = queryEngine.execute(request.sql());
                safeWriteResponse(output, new ServerResponse("RESULT", result.columns(), result.rows(), null));
            }
        } catch (java.io.IOException | RuntimeException ex) {
            try {
                safeWriteResponse(new DataOutputStream(socket.getOutputStream()), new ServerResponse("ERROR", null, null, ex.getMessage()));
            } catch (java.io.IOException ignored) {
                // best-effort error reporting only
            }
        } finally {
            sessionManager.closeSession(sessionId);
            connectionPool.release();
            try {
                socket.close();
            } catch (java.io.IOException ignored) {
                // ignore close failure
            }
        }
    }

    private ServerRequest readRequest(DataInputStream input) throws java.io.IOException {
        try {
            var length = input.readInt();
            if (length <= 0) {
                return null;
            }

            var payload = new byte[length];
            input.readFully(payload);
            return objectMapper.readValue(new String(payload, StandardCharsets.UTF_8), ServerRequest.class);
        } catch (java.io.EOFException eof) {
            return null;
        }
    }

    private void safeWriteResponse(DataOutputStream output, ServerResponse response) throws java.io.IOException {
        var payload = objectMapper.writeValueAsString(response).getBytes(StandardCharsets.UTF_8);
        output.writeInt(payload.length);
        output.write(payload);
        output.flush();
    }
}