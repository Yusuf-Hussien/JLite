package jlite.mcp;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import jlite.executor.QueryEngine;

class McpServerTest {

    @Test
    void listsToolsAndExecutesSelectQuery() throws Exception {
        var engine = new QueryEngine();
        engine.execute("CREATE TABLE users (id INT, name TEXT)");
        engine.execute("INSERT INTO users VALUES (1, 'Alice')");

        var server = new McpServer(engine);
        var objectMapper = new ObjectMapper();

        var listRequest = objectMapper.writeValueAsString(Map.of(
            "id", "1",
            "method", "tools/list",
            "params", Map.of()
        ));
        var listResponseRaw = server.handleJsonLine(listRequest);
        @SuppressWarnings("unchecked")
        var listResponse = objectMapper.readValue(listResponseRaw, Map.class);

        assertEquals(true, listResponse.get("ok"));
        @SuppressWarnings("unchecked")
        var listResult = (Map<String, Object>) listResponse.get("result");
        @SuppressWarnings("unchecked")
        var tools = (java.util.List<Map<String, Object>>) listResult.get("tools");
        assertEquals("execute_sql", tools.get(0).get("name"));

        var callRequest = objectMapper.writeValueAsString(Map.of(
            "id", "2",
            "method", "tools/call",
            "params", Map.of(
                "name", "execute_query",
                "arguments", Map.of("sql", "SELECT id, name FROM users")
            )
        ));
        var callResponseRaw = server.handleJsonLine(callRequest);
        @SuppressWarnings("unchecked")
        var callResponse = objectMapper.readValue(callResponseRaw, Map.class);

        assertEquals(true, callResponse.get("ok"));
        @SuppressWarnings("unchecked")
        var result = (Map<String, Object>) callResponse.get("result");
        @SuppressWarnings("unchecked")
        var rows = (java.util.List<java.util.List<Object>>) result.get("rows");
        assertEquals(1, rows.size());
        assertTrue(rows.get(0).contains("Alice"));
    }
}
