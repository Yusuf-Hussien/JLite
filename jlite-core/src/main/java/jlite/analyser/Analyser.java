package jlite.analyser;

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
import jlite.catalogue.Catalogue;
import jlite.catalogue.Column;
import jlite.catalogue.DataType;
import jlite.catalogue.TableSchema;

import java.util.HashSet;

/**
 * Minimal semantic analyser for SELECT statements.
 *
 * Validates table existence, column resolution, and simple expression typing.
 */
public class Analyser {

    private final Catalogue catalogue;

    public Analyser(Catalogue catalogue) {
        this.catalogue = catalogue;
    }

    public void validate(Statement statement) {
        if (statement instanceof SelectStatement selectStatement) {
            validateSelect(selectStatement);
            return;
        }
        if (statement instanceof InsertStatement insertStatement) {
            validateInsert(insertStatement);
            return;
        }
        if (statement instanceof UpdateStatement updateStatement) {
            validateUpdate(updateStatement);
            return;
        }
        if (statement instanceof DeleteStatement deleteStatement) {
            validateDelete(deleteStatement);
            return;
        }
        if (statement instanceof CreateTableStatement createTableStatement) {
            validateCreate(createTableStatement);
            return;
        }
        if (statement instanceof DropTableStatement dropTableStatement) {
            validateDrop(dropTableStatement);
            return;
        }
        if (statement instanceof AlterTableStatement alterTableStatement) {
            validateAlter(alterTableStatement);
            return;
        }

        throw new SemanticException("Unsupported statement type: " + typeName(statement));
    }

    private void validateSelect(SelectStatement statement) {
        var table = resolveTable(statement.fromTable());

        for (var expression : statement.selectList()) {
            validateExpression(expression, table, false);
        }

        if (statement.whereClause() != null) {
            var whereType = validateExpression(statement.whereClause(), table, true);
            if (whereType != DataType.BOOLEAN) {
                throw new SemanticException("WHERE clause must evaluate to BOOLEAN");
            }
        }
    }

    private DataType validateExpression(Expression expression, TableSchema table, boolean requireBoolean) {
        if (expression instanceof Literal literal) {
            return literalType(literal);
        }

        if (expression instanceof ColumnRef columnRef) {
            if ("*".equals(columnRef.column())) {
                if (requireBoolean) {
                    throw new SemanticException("Wildcard select item is not valid in WHERE clause");
                }
                return null;
            }
            return resolveColumn(columnRef, table).type();
        }

        if (expression instanceof BinaryOp binaryOp) {
            var leftType = validateExpression(binaryOp.left(), table, false);
            var rightType = validateExpression(binaryOp.right(), table, false);
            return validateBinary(binaryOp, leftType, rightType);
        }

        throw new SemanticException("Unsupported expression type: " + typeName(expression));
    }

    private void validateInsert(InsertStatement statement) {
        var table = resolveTable(statement.table());
        var targetColumns = statement.columns().isEmpty()
            ? table.columns().stream().map(Column::name).toList()
            : statement.columns();

        for (var columnName : targetColumns) {
            resolveColumn(new ColumnRef(null, columnName), table);
        }

        for (var row : statement.rows()) {
            if (row.size() != targetColumns.size()) {
                throw new SemanticException("INSERT row value count does not match target column count");
            }
            for (var expression : row) {
                validateExpression(expression, table, false);
            }
        }
    }

    private void validateUpdate(UpdateStatement statement) {
        var table = resolveTable(statement.table());

        for (var entry : statement.assignments().entrySet()) {
            resolveColumn(new ColumnRef(null, entry.getKey()), table);
            validateExpression(entry.getValue(), table, false);
        }

        if (statement.whereClause() != null) {
            var whereType = validateExpression(statement.whereClause(), table, true);
            if (whereType != DataType.BOOLEAN) {
                throw new SemanticException("WHERE clause must evaluate to BOOLEAN");
            }
        }
    }

    private void validateDelete(DeleteStatement statement) {
        var table = resolveTable(statement.table());
        if (statement.whereClause() != null) {
            var whereType = validateExpression(statement.whereClause(), table, true);
            if (whereType != DataType.BOOLEAN) {
                throw new SemanticException("WHERE clause must evaluate to BOOLEAN");
            }
        }
    }

    private void validateCreate(CreateTableStatement statement) {
        if (catalogue.getTable(statement.table()).isPresent()) {
            throw new SemanticException("Table already exists: " + statement.table());
        }
        if (statement.columns().isEmpty()) {
            throw new SemanticException("CREATE TABLE requires at least one column");
        }

        var seen = new HashSet<String>();
        for (var column : statement.columns()) {
            var key = column.name().toLowerCase();
            if (!seen.add(key)) {
                throw new SemanticException("Duplicate column in CREATE TABLE: " + column.name());
            }
        }
    }

    private void validateDrop(DropTableStatement statement) {
        resolveTable(statement.table());
    }

    private void validateAlter(AlterTableStatement statement) {
        var table = resolveTable(statement.table());
        if (statement.action() == AlterTableStatement.Action.ADD_COLUMN) {
            var exists = table.columns().stream().anyMatch(column -> column.name().equalsIgnoreCase(statement.column()));
            if (exists) {
                throw new SemanticException("Column already exists: " + statement.column());
            }
            if (statement.type() == null) {
                throw new SemanticException("ADD COLUMN requires a type");
            }
            return;
        }

        if (statement.action() == AlterTableStatement.Action.DROP_COLUMN) {
            resolveColumn(new ColumnRef(null, statement.column()), table);
        }
    }

    private DataType validateBinary(BinaryOp binaryOp, DataType leftType, DataType rightType) {
        return switch (binaryOp.op()) {
            case AND, OR -> {
                requireBoolean(leftType, "Left operand of " + binaryOp.op() + " must be BOOLEAN");
                requireBoolean(rightType, "Right operand of " + binaryOp.op() + " must be BOOLEAN");
                yield DataType.BOOLEAN;
            }
            case EQ, NEQ -> {
                requireComparable(leftType, rightType, binaryOp.op().name());
                yield DataType.BOOLEAN;
            }
            case LT, GT, LTE, GTE -> {
                requireComparable(leftType, rightType, binaryOp.op().name());
                yield DataType.BOOLEAN;
            }
            case PLUS, MINUS, STAR, SLASH -> {
                requireNumeric(leftType, "Left operand of " + binaryOp.op() + " must be numeric");
                requireNumeric(rightType, "Right operand of " + binaryOp.op() + " must be numeric");
                yield promoteNumeric(leftType, rightType);
            }
        };
    }

    private void requireBoolean(DataType type, String message) {
        if (type != DataType.BOOLEAN) {
            throw new SemanticException(message);
        }
    }

    private void requireNumeric(DataType type, String message) {
        if (!isNumeric(type)) {
            throw new SemanticException(message);
        }
    }

    private void requireComparable(DataType leftType, DataType rightType, String operator) {
        if (leftType == null || rightType == null) {
            throw new SemanticException("Cannot compare wildcard expressions with " + operator);
        }
        if (leftType != rightType) {
            if (!(isNumeric(leftType) && isNumeric(rightType))) {
                throw new SemanticException("Type mismatch for " + operator + ": " + leftType + " vs " + rightType);
            }
        }
    }

    private DataType promoteNumeric(DataType leftType, DataType rightType) {
        if (leftType == DataType.DOUBLE || rightType == DataType.DOUBLE) {
            return DataType.DOUBLE;
        }
        if (leftType == DataType.FLOAT || rightType == DataType.FLOAT) {
            return DataType.FLOAT;
        }
        if (leftType == DataType.BIGINT || rightType == DataType.BIGINT) {
            return DataType.BIGINT;
        }
        return DataType.INT;
    }

    private boolean isNumeric(DataType type) {
        return type == DataType.INT || type == DataType.BIGINT || type == DataType.FLOAT || type == DataType.DOUBLE;
    }

    private DataType literalType(Literal literal) {
        var value = literal.value();
        if (value == null) {
            return null;
        }
        if (value instanceof Boolean) {
            return DataType.BOOLEAN;
        }
        if (value instanceof Integer || value instanceof Long) {
            return DataType.BIGINT;
        }
        if (value instanceof Float) {
            return DataType.FLOAT;
        }
        if (value instanceof Double) {
            return DataType.DOUBLE;
        }
        return DataType.TEXT;
    }

    private Column resolveColumn(ColumnRef columnRef, TableSchema table) {
        if (columnRef.table() != null && !columnRef.table().equalsIgnoreCase(table.name())) {
            throw new SemanticException("Unknown table reference: " + columnRef.table());
        }

        return table.columns().stream()
            .filter(column -> column.name().equalsIgnoreCase(columnRef.column()))
            .findFirst()
            .orElseThrow(() -> new SemanticException("Unknown column: " + columnRef.column()));
    }

    private TableSchema resolveTable(String tableName) {
        return catalogue.getTable(tableName)
            .orElseThrow(() -> new SemanticException("Unknown table: " + tableName));
    }

    private String typeName(Object value) {
        return value == null ? "null" : value.getClass().getSimpleName();
    }
}