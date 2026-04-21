package jlite.parser;
import jlite.ast.AlterTableStatement;
import jlite.ast.BinaryOp;
import jlite.ast.ColumnRef;
import jlite.ast.CreateTableStatement;
import jlite.ast.DeleteStatement;
import jlite.ast.Expression;
import jlite.ast.InsertStatement;
import jlite.ast.Literal;
import jlite.ast.DropTableStatement;
import jlite.ast.SelectStatement;
import jlite.ast.Statement;
import jlite.ast.UpdateStatement;
import jlite.catalogue.DataType;
import jlite.lexer.Token;
import jlite.lexer.TokenType;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
/**
 * Recursive-descent SQL parser.
 * TODO: implement parseStatement() dispatching to all statement types.
 * TODO: parseExpression(), parsePredicate(), parseLiteral().
 */
public class Parser {
    private final List<Token> tokens;
    private int pos = 0;
    public Parser(List<Token> tokens) { this.tokens = tokens; }

    public Statement parseStatement() {
        Statement statement;
        if (match(TokenType.SELECT)) {
            statement = parseSelect();
        } else if (match(TokenType.INSERT)) {
            statement = parseInsert();
        } else if (match(TokenType.UPDATE)) {
            statement = parseUpdate();
        } else if (match(TokenType.DELETE)) {
            statement = parseDelete();
        } else if (match(TokenType.CREATE)) {
            statement = parseCreate();
        } else if (match(TokenType.DROP)) {
            statement = parseDrop();
        } else if (match(TokenType.ALTER)) {
            statement = parseAlter();
        } else {
            throw error("Unsupported statement");
        }

        if (match(TokenType.SEMICOLON)) {
            // Trailing semicolon is optional for single statement parse.
        }
        consume(TokenType.EOF, "Expected end of input");
        return statement;
    }

    private Statement parseInsert() {
        consume(TokenType.INTO, "Expected INTO after INSERT");
        var table = consumeIdentifier("Expected table name after INSERT INTO");

        List<String> columns = List.of();
        if (match(TokenType.LPAREN)) {
            var specified = new ArrayList<String>();
            specified.add(consumeIdentifier("Expected column name"));
            while (match(TokenType.COMMA)) {
                specified.add(consumeIdentifier("Expected column name"));
            }
            consume(TokenType.RPAREN, "Expected ')' after column list");
            columns = List.copyOf(specified);
        }

        consume(TokenType.VALUES, "Expected VALUES clause");
        var rows = new ArrayList<List<Expression>>();
        do {
            consume(TokenType.LPAREN, "Expected '(' before row values");
            var row = new ArrayList<Expression>();
            row.add(parseExpression());
            while (match(TokenType.COMMA)) {
                row.add(parseExpression());
            }
            consume(TokenType.RPAREN, "Expected ')' after row values");
            rows.add(List.copyOf(row));
        } while (match(TokenType.COMMA));

        return new InsertStatement(table, columns, List.copyOf(rows));
    }

    private Statement parseUpdate() {
        var table = consumeIdentifier("Expected table name after UPDATE");
        consume(TokenType.SET, "Expected SET in UPDATE statement");

        var assignments = new LinkedHashMap<String, Expression>();
        var firstColumn = consumeIdentifier("Expected column name in SET clause");
        consume(TokenType.EQ, "Expected '=' in SET assignment");
        assignments.put(firstColumn, parseExpression());
        while (match(TokenType.COMMA)) {
            var column = consumeIdentifier("Expected column name in SET clause");
            consume(TokenType.EQ, "Expected '=' in SET assignment");
            assignments.put(column, parseExpression());
        }

        Expression whereClause = null;
        if (match(TokenType.WHERE)) {
            whereClause = parseExpression();
        }

        return new UpdateStatement(table, assignments, whereClause);
    }

    private Statement parseDelete() {
        consume(TokenType.FROM, "Expected FROM after DELETE");
        var table = consumeIdentifier("Expected table name after DELETE FROM");
        Expression whereClause = null;
        if (match(TokenType.WHERE)) {
            whereClause = parseExpression();
        }
        return new DeleteStatement(table, whereClause);
    }

    private Statement parseCreate() {
        consume(TokenType.TABLE, "Expected TABLE after CREATE");
        var table = consumeIdentifier("Expected table name after CREATE TABLE");
        consume(TokenType.LPAREN, "Expected '(' after table name");

        var columns = new ArrayList<CreateTableStatement.ColumnDef>();
        columns.add(parseColumnDef());
        while (match(TokenType.COMMA)) {
            columns.add(parseColumnDef());
        }
        consume(TokenType.RPAREN, "Expected ')' after column definitions");

        return new CreateTableStatement(table, List.copyOf(columns));
    }

    private Statement parseDrop() {
        consume(TokenType.TABLE, "Expected TABLE after DROP");
        return new DropTableStatement(consumeIdentifier("Expected table name after DROP TABLE"));
    }

    private Statement parseAlter() {
        consume(TokenType.TABLE, "Expected TABLE after ALTER");
        var table = consumeIdentifier("Expected table name after ALTER TABLE");

        if (match(TokenType.ADD)) {
            match(TokenType.COLUMN);
            var column = consumeIdentifier("Expected column name after ADD COLUMN");
            var type = parseDataType();
            return new AlterTableStatement(table, AlterTableStatement.Action.ADD_COLUMN, column, type);
        }

        if (match(TokenType.DROP)) {
            match(TokenType.COLUMN);
            var column = consumeIdentifier("Expected column name after DROP COLUMN");
            return new AlterTableStatement(table, AlterTableStatement.Action.DROP_COLUMN, column, null);
        }

        throw error("Expected ADD COLUMN or DROP COLUMN in ALTER TABLE");
    }

    private CreateTableStatement.ColumnDef parseColumnDef() {
        var columnName = consumeIdentifier("Expected column name");
        var type = parseDataType();
        return new CreateTableStatement.ColumnDef(columnName, type);
    }

    private DataType parseDataType() {
        if (match(TokenType.INT)) {
            return DataType.INT;
        }
        if (match(TokenType.BIGINT)) {
            return DataType.BIGINT;
        }
        if (match(TokenType.FLOAT)) {
            return DataType.FLOAT;
        }
        if (match(TokenType.DOUBLE)) {
            return DataType.DOUBLE;
        }
        if (match(TokenType.TEXT)) {
            return DataType.TEXT;
        }
        if (match(TokenType.VARCHAR)) {
            if (match(TokenType.LPAREN)) {
                consume(TokenType.INTEGER_LITERAL, "Expected length in VARCHAR(n)");
                consume(TokenType.RPAREN, "Expected ')' after VARCHAR length");
            }
            return DataType.VARCHAR;
        }
        if (match(TokenType.BOOLEAN)) {
            return DataType.BOOLEAN;
        }
        if (match(TokenType.DATE)) {
            return DataType.DATE;
        }
        if (match(TokenType.TIMESTAMP)) {
            return DataType.TIMESTAMP;
        }
        throw error("Expected SQL data type");
    }

    private SelectStatement parseSelect() {
        var selectList = parseSelectList();
        consume(TokenType.FROM, "Expected FROM clause");
        var fromTable = consumeIdentifier("Expected table name after FROM");

        Expression whereClause = null;
        if (match(TokenType.WHERE)) {
            whereClause = parseExpression();
        }

        return new SelectStatement(selectList, fromTable, whereClause);
    }

    private List<Expression> parseSelectList() {
        var selectList = new ArrayList<Expression>();
        if (match(TokenType.STAR)) {
            selectList.add(new ColumnRef(null, "*"));
            return selectList;
        }

        selectList.add(parseExpression());
        while (match(TokenType.COMMA)) {
            selectList.add(parseExpression());
        }
        return selectList;
    }

    private Expression parseExpression() {
        return parseOr();
    }

    private Expression parseOr() {
        var expr = parseAnd();
        while (match(TokenType.OR)) {
            expr = new BinaryOp(expr, BinaryOp.Op.OR, parseAnd());
        }
        return expr;
    }

    private Expression parseAnd() {
        var expr = parseComparison();
        while (match(TokenType.AND)) {
            expr = new BinaryOp(expr, BinaryOp.Op.AND, parseComparison());
        }
        return expr;
    }

    private Expression parseComparison() {
        var expr = parseAdditive();
        while (true) {
            if (match(TokenType.EQ)) {
                expr = new BinaryOp(expr, BinaryOp.Op.EQ, parseAdditive());
            } else if (match(TokenType.NEQ)) {
                expr = new BinaryOp(expr, BinaryOp.Op.NEQ, parseAdditive());
            } else if (match(TokenType.LT)) {
                expr = new BinaryOp(expr, BinaryOp.Op.LT, parseAdditive());
            } else if (match(TokenType.GT)) {
                expr = new BinaryOp(expr, BinaryOp.Op.GT, parseAdditive());
            } else if (match(TokenType.LTE)) {
                expr = new BinaryOp(expr, BinaryOp.Op.LTE, parseAdditive());
            } else if (match(TokenType.GTE)) {
                expr = new BinaryOp(expr, BinaryOp.Op.GTE, parseAdditive());
            } else {
                return expr;
            }
        }
    }

    private Expression parseAdditive() {
        var expr = parseMultiplicative();
        while (true) {
            if (match(TokenType.PLUS)) {
                expr = new BinaryOp(expr, BinaryOp.Op.PLUS, parseMultiplicative());
            } else if (match(TokenType.MINUS)) {
                expr = new BinaryOp(expr, BinaryOp.Op.MINUS, parseMultiplicative());
            } else {
                return expr;
            }
        }
    }

    private Expression parseMultiplicative() {
        var expr = parsePrimary();
        while (true) {
            if (match(TokenType.STAR)) {
                expr = new BinaryOp(expr, BinaryOp.Op.STAR, parsePrimary());
            } else if (match(TokenType.SLASH)) {
                expr = new BinaryOp(expr, BinaryOp.Op.SLASH, parsePrimary());
            } else {
                return expr;
            }
        }
    }

    private Expression parsePrimary() {
        if (match(TokenType.LPAREN)) {
            var expr = parseExpression();
            consume(TokenType.RPAREN, "Expected ')' after expression");
            return expr;
        }

        if (match(TokenType.STRING_LITERAL)) {
            return new Literal(previous().value());
        }
        if (match(TokenType.INTEGER_LITERAL)) {
            return new Literal(Long.valueOf(previous().value()));
        }
        if (match(TokenType.FLOAT_LITERAL)) {
            return new Literal(Double.valueOf(previous().value()));
        }
        if (match(TokenType.BOOLEAN_LITERAL)) {
            return new Literal(Boolean.valueOf(previous().value()));
        }
        if (match(TokenType.NULL_LITERAL)) {
            return new Literal(null);
        }
        if (match(TokenType.IDENTIFIER)) {
            var first = previous().value();
            if (match(TokenType.DOT)) {
                var second = consumeIdentifier("Expected column name after '.'");
                return new ColumnRef(first, second);
            }
            return new ColumnRef(null, first);
        }

        throw error("Expected expression");
    }

    private String consumeIdentifier(String message) {
        consume(TokenType.IDENTIFIER, message);
        return previous().value();
    }

    private void consume(TokenType type, String message) {
        if (!match(type)) {
            throw error(message);
        }
    }

    private boolean match(TokenType type) {
        if (check(type)) {
            pos++;
            return true;
        }
        return false;
    }

    private boolean check(TokenType type) {
        return peek().type() == type;
    }

    private Token peek() {
        return tokens.get(pos);
    }

    private Token previous() {
        return tokens.get(pos - 1);
    }

    private ParseException error(String message) {
        var token = peek();
        return new ParseException(message + " at line " + token.line() + ", column " + token.column());
    }
}
