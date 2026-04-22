package jlite.mcp.tools;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import jlite.executor.QueryEngine;

class ExecuteQueryToolTest {

    @Test
    void executesSchemaChangesAndReturnsStructuredResults() {
        var engine = new QueryEngine();
        var tool = new ExecuteQueryTool(engine, true);

        @SuppressWarnings("unchecked")
        var createResult = (Map<String, Object>) tool.execute("CREATE TABLE users (id INT)");
        assertTrue(createResult.containsKey("rowCount"));

        @SuppressWarnings("unchecked")
        var insertResult = (Map<String, Object>) tool.execute("INSERT INTO users VALUES (1)");
        assertTrue(insertResult.containsKey("rowCount"));

        @SuppressWarnings("unchecked")
        var selectResult = (Map<String, Object>) tool.execute("SELECT id FROM users");
        assertEquals(1, selectResult.get("rowCount"));
    }

    @Test
    void returnsStructuredResultForSelect() {
        var engine = new QueryEngine();
        engine.execute("CREATE TABLE users (id INT, name TEXT)");
        engine.execute("INSERT INTO users VALUES (1, 'Alice')");

        var tool = new ExecuteQueryTool(engine, true);
        @SuppressWarnings("unchecked")
        var result = (Map<String, Object>) tool.execute("SELECT id, name FROM users");

        assertEquals(1, result.get("rowCount"));
    }
}
