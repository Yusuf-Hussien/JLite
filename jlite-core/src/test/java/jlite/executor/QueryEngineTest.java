package jlite.executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

import jlite.catalogue.Column;
import jlite.catalogue.DataType;
import jlite.catalogue.TableSchema;

class QueryEngineTest {

    @Test
    void executesEndToEndSqlAgainstInMemoryStore() {
        var engine = new QueryEngine();
        engine.createTable(new TableSchema(
            "users",
            java.util.List.of(
                new Column("id", DataType.INT, false, true),
                new Column("name", DataType.TEXT, true, false),
                new Column("age", DataType.INT, true, false),
                new Column("active", DataType.BOOLEAN, true, false)
            )
        ));
        engine.insertRow("users", java.util.Map.of("id", 1L, "name", "Alice", "age", 30L, "active", true));
        engine.insertRow("users", java.util.Map.of("id", 2L, "name", "Bob", "age", 17L, "active", false));
        engine.insertRow("users", java.util.Map.of("id", 3L, "name", "Cara", "age", 41L, "active", true));

        var result = engine.execute("SELECT name, age FROM users WHERE active = true AND age >= 18");

        assertEquals(java.util.List.of("name", "age"), result.columns());
        assertEquals(2, result.rows().size());
        assertEquals(java.util.List.of("Alice", 30L), result.rows().get(0));
        assertEquals(java.util.List.of("Cara", 41L), result.rows().get(1));
    }

    @Test
    void surfacesParserAndSemanticErrors() {
        var engine = new QueryEngine();
        engine.createTable(new TableSchema(
            "users",
            java.util.List.of(new Column("id", DataType.INT, false, true))
        ));

        var parseError = assertThrows(RuntimeException.class, () -> engine.execute("SELECT id users"));
        assertEquals(true, parseError.getMessage().contains("Expected FROM clause"));

        var semanticError = assertThrows(RuntimeException.class, () -> engine.execute("SELECT missing FROM users"));
        assertEquals(true, semanticError.getMessage().contains("Unknown column"));
    }

    @Test
    void executesDdlAndDmlFlow() {
        var engine = new QueryEngine();

        var create = engine.execute("CREATE TABLE projects (id INT, name TEXT)");
        assertEquals(java.util.List.of(java.util.List.of(0)), create.rows());

        var insert = engine.execute("INSERT INTO projects VALUES (1, 'Core'), (2, 'Server')");
        assertEquals(java.util.List.of(java.util.List.of(2)), insert.rows());

        var alterAdd = engine.execute("ALTER TABLE projects ADD COLUMN active BOOLEAN");
        assertEquals(java.util.List.of(java.util.List.of(0)), alterAdd.rows());

        var update = engine.execute("UPDATE projects SET active = true WHERE id = 1");
        assertEquals(java.util.List.of(java.util.List.of(1)), update.rows());

        var select = engine.execute("SELECT id, active FROM projects WHERE id = 1");
        assertEquals(java.util.List.of("id", "active"), select.columns());
        assertEquals(java.util.List.of(java.util.List.of(1L, true)), select.rows());

        var delete = engine.execute("DELETE FROM projects WHERE id = 2");
        assertEquals(java.util.List.of(java.util.List.of(1)), delete.rows());

        var alterDrop = engine.execute("ALTER TABLE projects DROP COLUMN active");
        assertEquals(java.util.List.of(java.util.List.of(0)), alterDrop.rows());

        var drop = engine.execute("DROP TABLE projects");
        assertEquals(java.util.List.of(java.util.List.of(0)), drop.rows());

        var unknownTable = assertThrows(RuntimeException.class, () -> engine.execute("SELECT * FROM projects"));
        assertEquals(true, unknownTable.getMessage().contains("Unknown table"));
    }
}
