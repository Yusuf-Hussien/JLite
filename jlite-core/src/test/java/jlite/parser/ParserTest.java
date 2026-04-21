package jlite.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

import jlite.ast.AlterTableStatement;
import jlite.ast.BinaryOp;
import jlite.ast.ColumnRef;
import jlite.ast.CreateTableStatement;
import jlite.ast.DeleteStatement;
import jlite.ast.DropTableStatement;
import jlite.ast.InsertStatement;
import jlite.ast.Literal;
import jlite.ast.SelectStatement;
import jlite.ast.UpdateStatement;
import jlite.lexer.Lexer;

class ParserTest {

    @Test
    void parseSimpleSelectStar() {
        var parser = new Parser(new Lexer("SELECT * FROM users").tokenise());

        var statement = parser.parseStatement();

        var select = assertInstanceOf(SelectStatement.class, statement);
        assertEquals("users", select.fromTable());
        assertEquals(1, select.selectList().size());
        var star = assertInstanceOf(ColumnRef.class, select.selectList().get(0));
        assertNull(star.table());
        assertEquals("*", star.column());
        assertNull(select.whereClause());
    }

    @Test
    void parseSelectWithWhereExpressionPrecedence() {
        var sql = "SELECT name, age FROM users WHERE age >= 18 AND active = true OR score > 10";
        var parser = new Parser(new Lexer(sql).tokenise());

        var statement = parser.parseStatement();
        var select = assertInstanceOf(SelectStatement.class, statement);

        assertEquals("users", select.fromTable());
        assertEquals(2, select.selectList().size());

        var where = assertInstanceOf(BinaryOp.class, select.whereClause());
        assertEquals(BinaryOp.Op.OR, where.op());

        var left = assertInstanceOf(BinaryOp.class, where.left());
        assertEquals(BinaryOp.Op.AND, left.op());

        var leftLeft = assertInstanceOf(BinaryOp.class, left.left());
        assertEquals(BinaryOp.Op.GTE, leftLeft.op());

        var leftRight = assertInstanceOf(BinaryOp.class, left.right());
        assertEquals(BinaryOp.Op.EQ, leftRight.op());
        var boolLiteral = assertInstanceOf(Literal.class, leftRight.right());
        assertEquals(true, boolLiteral.value());

        var right = assertInstanceOf(BinaryOp.class, where.right());
        assertEquals(BinaryOp.Op.GT, right.op());
    }

    @Test
    void parseThrowsForMissingFromClause() {
        var parser = new Parser(new Lexer("SELECT id users").tokenise());
        var ex = assertThrows(ParseException.class, () -> {
            parser.parseStatement();
        });
        assertEquals(true, ex.getMessage().contains("Expected FROM clause"));
    }

    @Test
    void parseInsertUpdateDeleteAndDdlStatements() {
        var insert = new Parser(new Lexer("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')").tokenise()).parseStatement();
        var insertStmt = assertInstanceOf(InsertStatement.class, insert);
        assertEquals("users", insertStmt.table());
        assertEquals(2, insertStmt.rows().size());

        var update = new Parser(new Lexer("UPDATE users SET age = age + 1 WHERE id = 1").tokenise()).parseStatement();
        var updateStmt = assertInstanceOf(UpdateStatement.class, update);
        assertEquals("users", updateStmt.table());
        assertEquals(1, updateStmt.assignments().size());

        var delete = new Parser(new Lexer("DELETE FROM users WHERE id = 2").tokenise()).parseStatement();
        var deleteStmt = assertInstanceOf(DeleteStatement.class, delete);
        assertEquals("users", deleteStmt.table());

        var create = new Parser(new Lexer("CREATE TABLE users (id INT, name TEXT)").tokenise()).parseStatement();
        var createStmt = assertInstanceOf(CreateTableStatement.class, create);
        assertEquals("users", createStmt.table());
        assertEquals(2, createStmt.columns().size());

        var alter = new Parser(new Lexer("ALTER TABLE users ADD COLUMN active BOOLEAN").tokenise()).parseStatement();
        var alterStmt = assertInstanceOf(AlterTableStatement.class, alter);
        assertEquals(AlterTableStatement.Action.ADD_COLUMN, alterStmt.action());

        var drop = new Parser(new Lexer("DROP TABLE users").tokenise()).parseStatement();
        var dropStmt = assertInstanceOf(DropTableStatement.class, drop);
        assertEquals("users", dropStmt.table());
    }
}
