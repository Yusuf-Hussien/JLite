package jlite.agent;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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
    void helpResponseListsSupportedStatementTypes() {
        var engine = new QueryEngine();
        var schemaBuilder = new SchemaContextBuilder(engine.catalogue());
        var validator = new SqlValidator(engine.catalogue());
        var agent = new NlAgent(schemaBuilder, validator, engine, (question, schema) -> {
            throw new RuntimeException("HTTP 429 quota exceeded");
        });

        var answer = agent.query("help");

        assertTrue(answer.contains("SELECT"));
        assertTrue(answer.contains("INSERT"));
        assertTrue(answer.contains("ALTER TABLE"));
    }

    @Test
    void capabilityPromptWhatUCanDoReturnsHelpText() {
        var engine = new QueryEngine();
        var schemaBuilder = new SchemaContextBuilder(engine.catalogue());
        var validator = new SqlValidator(engine.catalogue());
        var agent = new NlAgent(schemaBuilder, validator, engine, (question, schema) -> "SELECT * FROM users");

        var answer = agent.query("what u can do");

        assertTrue(answer.contains("Supported statements:"));
        assertTrue(answer.contains("SELECT"));
    }

    @Test
    void outOfContextConversationDoesNotExecuteSql() {
        var engine = new QueryEngine();
        var schemaBuilder = new SchemaContextBuilder(engine.catalogue());
        var validator = new SqlValidator(engine.catalogue());
        var agent = new NlAgent(schemaBuilder, validator, engine, (question, schema) -> "SELECT * FROM users");

        var first = agent.query("HOW ARE U");
        var second = agent.query("ok who are u");

        assertTrue(first.contains("I am JLite Agent"));
        assertTrue(second.contains("I am JLite Agent"));
    }

    @Test
    void fallsBackWhenPrimaryTranslatorReturnsInvalidSql() {
        var engine = new QueryEngine();
        engine.createTable(new TableSchema("users", List.of(
            new Column("id", DataType.INT, false, true),
            new Column("name", DataType.TEXT, true, false)
        )));
        engine.insertRow("users", Map.of("id", 1L, "name", "Alice"));

        var schemaBuilder = new SchemaContextBuilder(engine.catalogue());
        var validator = new SqlValidator(engine.catalogue());
        var agent = new NlAgent(schemaBuilder, validator, engine, (question, schema) -> "SELECT * FROM users LIMIT 1");

        var answer = agent.query("give me the first user");

        assertTrue(answer.contains("SQL: SELECT * FROM users WHERE id = 1"));
        assertTrue(answer.contains("Alice"));
    }

    @Test
    void retriesPrimaryTranslatorWithRepairPromptBeforeLocalFallback() {
        var engine = new QueryEngine();
        engine.createTable(new TableSchema("users", List.of(
            new Column("id", DataType.INT, false, true),
            new Column("name", DataType.TEXT, true, false)
        )));
        engine.insertRow("users", Map.of("id", 1L, "name", "Alice"));

        var callCount = new AtomicInteger(0);
        var schemaBuilder = new SchemaContextBuilder(engine.catalogue());
        var validator = new SqlValidator(engine.catalogue());
        var agent = new NlAgent(schemaBuilder, validator, engine, (question, schema) -> {
            if (callCount.incrementAndGet() == 1) {
                return "SELECT * FROM users LIMIT 1";
            }
            return "SELECT * FROM users WHERE id = 1";
        });

        var answer = agent.query("give me the first user");

        assertEquals(2, callCount.get());
        assertTrue(answer.contains("SQL: SELECT * FROM users WHERE id = 1"));
        assertTrue(answer.contains("Alice"));
    }

    @Test
    void narrowsBroadRemoteSqlForFirstUserIntent() {
        var engine = new QueryEngine();
        engine.createTable(new TableSchema("users", List.of(
            new Column("id", DataType.INT, false, true),
            new Column("name", DataType.TEXT, true, false)
        )));
        engine.insertRow("users", Map.of("id", 1L, "name", "Alice"));
        engine.insertRow("users", Map.of("id", 2L, "name", "Bob"));

        var callCount = new AtomicInteger(0);
        var schemaBuilder = new SchemaContextBuilder(engine.catalogue());
        var validator = new SqlValidator(engine.catalogue());
        var agent = new NlAgent(schemaBuilder, validator, engine, (question, schema) -> {
            if (callCount.incrementAndGet() == 1) {
                return "SELECT * FROM users";
            }
            return "SELECT * FROM users WHERE id = 1";
        });

        var answer = agent.query("give me the first user");

        assertEquals(2, callCount.get());
        assertTrue(answer.contains("SQL: SELECT * FROM users WHERE id = 1"));
        assertTrue(answer.contains("Alice"));
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
