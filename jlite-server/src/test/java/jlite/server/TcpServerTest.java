package jlite.server;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import jlite.catalogue.Column;
import jlite.catalogue.DataType;
import jlite.catalogue.TableSchema;
import jlite.executor.QueryEngine;
import jlite.server.protocol.ServerRequest;
import jlite.server.protocol.ServerResponse;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TcpServerTest {

    @Test
    void servesQueryRequestsOverFramedJson() throws Exception {
        var engine = new QueryEngine();
        engine.createTable(new TableSchema(
            "users",
            List.of(
                new Column("id", DataType.INT, false, true),
                new Column("name", DataType.TEXT, true, false),
                new Column("age", DataType.INT, true, false),
                new Column("active", DataType.BOOLEAN, true, false)
            )
        ));
        engine.insertRow("users", Map.of("id", 1L, "name", "Alice", "age", 30L, "active", true));
        engine.insertRow("users", Map.of("id", 2L, "name", "Bob", "age", 17L, "active", false));
        engine.insertRow("users", Map.of("id", 3L, "name", "Cara", "age", 41L, "active", true));

        var server = new TcpServer(0, engine);
        var serverThread = Thread.ofVirtual().start(() -> {
            try {
                server.start();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });

        try {
            server.awaitStarted();
            var objectMapper = new ObjectMapper();
            try (var socket = new Socket("127.0.0.1", server.getPort());
                 var output = new DataOutputStream(socket.getOutputStream());
                 var input = new DataInputStream(socket.getInputStream())) {

                writeFrame(output, objectMapper.writeValueAsString(new ServerRequest("QUERY", "SELECT name, age FROM users WHERE active = true AND age >= 18")));

                var response = objectMapper.readValue(readFrame(input), ServerResponse.class);
                assertEquals("RESULT", response.type());
                assertEquals(List.of("name", "age"), response.columns());
                assertEquals(2, response.rows().size());
                assertEquals(List.of("Alice", 30), response.rows().get(0));
                assertEquals(List.of("Cara", 41), response.rows().get(1));

                writeFrame(output, objectMapper.writeValueAsString(new ServerRequest("CLOSE", null)));
                var closeResponse = objectMapper.readValue(readFrame(input), ServerResponse.class);
                assertEquals("OK", closeResponse.type());
            }
        } finally {
            server.stop();
            serverThread.join(3000);
        }
    }

    private void writeFrame(DataOutputStream output, String payload) throws Exception {
        var bytes = payload.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        output.writeInt(bytes.length);
        output.write(bytes);
        output.flush();
    }

    private String readFrame(DataInputStream input) throws Exception {
        var length = input.readInt();
        var bytes = new byte[length];
        input.readFully(bytes);
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }
}
