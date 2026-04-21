package jlite.analyser;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

import jlite.catalogue.Catalogue;
import jlite.catalogue.Column;
import jlite.catalogue.DataType;
import jlite.catalogue.TableSchema;
import jlite.lexer.Lexer;
import jlite.parser.Parser;

class AnalyserTest {

    @Test
    void validatesKnownTableAndColumns() {
        var analyser = analyser();
        var statement = new Parser(new Lexer("SELECT name, age FROM users WHERE active = true AND age >= 18").tokenise())
            .parseStatement();

        assertDoesNotThrow(() -> analyser.validate(statement));
    }

    @Test
    void rejectsUnknownTable() {
        var analyser = analyser();
        var statement = new Parser(new Lexer("SELECT name FROM missing").tokenise()).parseStatement();

        var ex = assertThrows(SemanticException.class, () -> analyser.validate(statement));
        assertEquals(true, ex.getMessage().contains("Unknown table"));
    }

    @Test
    void rejectsUnknownColumn() {
        var analyser = analyser();
        var statement = new Parser(new Lexer("SELECT missing FROM users").tokenise()).parseStatement();

        var ex = assertThrows(SemanticException.class, () -> analyser.validate(statement));
        assertEquals(true, ex.getMessage().contains("Unknown column"));
    }

    @Test
    void rejectsNonBooleanWhereClause() {
        var analyser = analyser();
        var statement = new Parser(new Lexer("SELECT name FROM users WHERE age + 1").tokenise()).parseStatement();

        var ex = assertThrows(SemanticException.class, () -> analyser.validate(statement));
        assertEquals(true, ex.getMessage().contains("WHERE clause must evaluate to BOOLEAN"));
    }

    private Analyser analyser() {
        var catalogue = new Catalogue();
        catalogue.createTable(new TableSchema(
            "users",
            java.util.List.of(
                new Column("id", DataType.INT, false, true),
                new Column("name", DataType.TEXT, true, false),
                new Column("age", DataType.INT, true, false),
                new Column("active", DataType.BOOLEAN, true, false)
            )
        ));
        return new Analyser(catalogue);
    }
}
