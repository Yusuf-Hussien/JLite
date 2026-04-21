package jlite.agent;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import jlite.catalogue.Column;
import jlite.catalogue.DataType;
import jlite.catalogue.TableSchema;
import jlite.executor.QueryEngine;

class NlAgentTest {

    @Test
    void translatesValidatesAndExecutesQuestion() {
        var engine = new QueryEngine();
        engine.createTable(new TableSchema("users", List.of(
            new Column("id", DataType.INT, false, true),
            new Column("name", DataType.TEXT, true, false)
        )));
        engine.insertRow("users", Map.of("id", 1L, "name", "Alice"));

        var schemaBuilder = new SchemaContextBuilder(engine.catalogue());
        var validator = new SqlValidator(engine.catalogue());
        var agent = new NlAgent(schemaBuilder, validator, engine, (question, schema) -> "SELECT id, name FROM users");

        var answer = agent.query("show users");

        assertTrue(answer.contains("Row count: 1"));
        assertTrue(answer.contains("Alice"));
    }

    @Test
    void fallsBackWhenPrimaryTranslatorFails() {
        var engine = new QueryEngine();
        engine.createTable(new TableSchema("users", List.of(
            new Column("id", DataType.INT, false, true),
            new Column("name", DataType.TEXT, true, false)
        )));
        engine.insertRow("users", Map.of("id", 1L, "name", "Alice"));

        var schemaBuilder = new SchemaContextBuilder(engine.catalogue());
        var validator = new SqlValidator(engine.catalogue());
        var agent = new NlAgent(schemaBuilder, validator, engine, (question, schema) -> {
            throw new RuntimeException("HTTP 429 quota exceeded");
        });

        var answer = agent.query("show users");

        assertTrue(answer.contains("SQL: SELECT * FROM users"));
        assertTrue(answer.contains("Alice"));
    }

    @Test
    void respondsToGreetingWithoutFailingTranslation() {
        var engine = new QueryEngine();
        var schemaBuilder = new SchemaContextBuilder(engine.catalogue());
        var validator = new SqlValidator(engine.catalogue());
        var agent = new NlAgent(schemaBuilder, validator, engine, (question, schema) -> {
            throw new RuntimeException("HTTP 429 quota exceeded");
        });

        var answer = agent.query("hi");

        assertTrue(answer.toLowerCase().contains("ask me about your data"));
    }

    @Test
    void normalizesSelectAllFromToSelectStar() {
        assertEquals("SELECT * FROM users", NlAgent.normalizeSql("SLECt all from users"));
    }

    @Test
    void remoteCheckReportsMissingKeyWhenNotConfigured() {
        var previous = System.getProperty("GEMINI_API_KEY");
        try {
            System.clearProperty("GEMINI_API_KEY");
            var result = NlAgent.remoteCheck();
            assertTrue(result.contains("REMOTE_STATUS=ERROR"));
        } finally {
            if (previous != null) {
                System.setProperty("GEMINI_API_KEY", previous);
            }
        }
    }
}
