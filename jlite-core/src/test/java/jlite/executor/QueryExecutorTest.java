package jlite.executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

import jlite.analyser.Analyser;
import jlite.catalogue.Catalogue;
import jlite.catalogue.Column;
import jlite.catalogue.DataType;
import jlite.catalogue.TableSchema;
import jlite.lexer.Lexer;
import jlite.parser.Parser;

class QueryExecutorTest {

    @Test
    void executesSelectWithWhereFilter() {
        var context = context();
        var statement = new Parser(new Lexer("SELECT name, age FROM users WHERE active = true AND age >= 18").tokenise())
            .parseStatement();
        context.analyser().validate(statement);

        var result = context.executor().execute(statement);

        assertEquals(java.util.List.of("name", "age"), result.columns());
        assertEquals(2, result.rows().size());
        assertEquals(java.util.List.of("Alice", 30L), result.rows().get(0));
        assertEquals(java.util.List.of("Cara", 41L), result.rows().get(1));
    }

    @Test
    void executesSelectStar() {
        var context = context();
        var statement = new Parser(new Lexer("SELECT * FROM users WHERE id = 2").tokenise()).parseStatement();
        context.analyser().validate(statement);

        var result = context.executor().execute(statement);

        assertEquals(java.util.List.of("id", "name", "age", "active"), result.columns());
        assertEquals(1, result.rows().size());
        assertEquals(java.util.List.of(2L, "Bob", 17L, false), result.rows().get(0));
    }

    @Test
    void executesInsertUpdateAndDelete() {
        var context = context();
        var insert = new Parser(new Lexer("INSERT INTO users (id, name, age, active) VALUES (4, 'Dina', 19, true)").tokenise()).parseStatement();
        context.analyser().validate(insert);
        var insertResult = context.executor().execute(insert);
        assertEquals(java.util.List.of("affected_rows"), insertResult.columns());
        assertEquals(java.util.List.of(java.util.List.of(1)), insertResult.rows());

        var update = new Parser(new Lexer("UPDATE users SET age = age + 1 WHERE id = 4").tokenise()).parseStatement();
        context.analyser().validate(update);
        var updateResult = context.executor().execute(update);
        assertEquals(java.util.List.of(java.util.List.of(1)), updateResult.rows());

        var delete = new Parser(new Lexer("DELETE FROM users WHERE id = 4").tokenise()).parseStatement();
        context.analyser().validate(delete);
        var deleteResult = context.executor().execute(delete);
        assertEquals(java.util.List.of(java.util.List.of(1)), deleteResult.rows());

        var verify = new Parser(new Lexer("SELECT id FROM users WHERE id = 4").tokenise()).parseStatement();
        context.analyser().validate(verify);
        var verifyResult = context.executor().execute(verify);
        assertEquals(0, verifyResult.rows().size());
    }

    private TestContext context() {
        var catalogue = new Catalogue();
        var tableStore = new InMemoryTableStore(catalogue);
        tableStore.createTable(new TableSchema(
            "users",
            java.util.List.of(
                new Column("id", DataType.INT, false, true),
                new Column("name", DataType.TEXT, true, false),
                new Column("age", DataType.INT, true, false),
                new Column("active", DataType.BOOLEAN, true, false)
            )
        ));
        tableStore.insertRow("users", java.util.Map.of("id", 1L, "name", "Alice", "age", 30L, "active", true));
        tableStore.insertRow("users", java.util.Map.of("id", 2L, "name", "Bob", "age", 17L, "active", false));
        tableStore.insertRow("users", java.util.Map.of("id", 3L, "name", "Cara", "age", 41L, "active", true));
        return new TestContext(new Analyser(catalogue), new QueryExecutor(tableStore));
    }

    private record TestContext(Analyser analyser, QueryExecutor executor) {}
}
