package jlite.executor;

import jlite.ast.AlterTableStatement;
import jlite.ast.BinaryOp;
import jlite.ast.ColumnRef;
import jlite.ast.CreateTableStatement;
import jlite.ast.DeleteStatement;
import jlite.ast.DropTableStatement;
import jlite.ast.Expression;
import jlite.ast.InsertStatement;
import jlite.ast.Literal;
import jlite.ast.SelectStatement;
import jlite.ast.Statement;
import jlite.ast.UpdateStatement;
import jlite.catalogue.Column;
import jlite.catalogue.TableSchema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class QueryExecutor {

    private final TableStore tableStore;

    public QueryExecutor(TableStore tableStore) {
        this.tableStore = tableStore;
    }

    public QueryResult execute(Statement statement) {
        if (statement instanceof SelectStatement selectStatement) {
            return executeSelect(selectStatement);
        }
        if (statement instanceof CreateTableStatement createTableStatement) {
            return executeCreateTable(createTableStatement);
        }
        if (statement instanceof DropTableStatement dropTableStatement) {
            return executeDropTable(dropTableStatement);
        }
        if (statement instanceof AlterTableStatement alterTableStatement) {
            return executeAlterTable(alterTableStatement);
        }
        if (statement instanceof InsertStatement insertStatement) {
            return executeInsert(insertStatement);
        }
        if (statement instanceof UpdateStatement updateStatement) {
            return executeUpdate(updateStatement);
        }
        if (statement instanceof DeleteStatement deleteStatement) {
            return executeDelete(deleteStatement);
        }

        throw new UnsupportedOperationException("Unsupported statement type: " + typeName(statement));
    }

    private QueryResult executeCreateTable(CreateTableStatement statement) {
        var columns = statement.columns().stream()
            .map(columnDef -> new Column(columnDef.name(), columnDef.type(), true, false))
            .toList();
        tableStore.createTable(new TableSchema(statement.table(), columns));
        return QueryResult.affectedRows(0);
    }

    private QueryResult executeDropTable(DropTableStatement statement) {
        tableStore.dropTable(statement.table());
        return QueryResult.affectedRows(0);
    }

    private QueryResult executeAlterTable(AlterTableStatement statement) {
        if (statement.action() == AlterTableStatement.Action.ADD_COLUMN) {
            tableStore.addColumn(statement.table(), new Column(statement.column(), statement.type(), true, false));
        } else {
            tableStore.dropColumn(statement.table(), statement.column());
        }
        return QueryResult.affectedRows(0);
    }

    private QueryResult executeInsert(InsertStatement statement) {
        var schema = tableStore.resolveSchema(statement.table());
        var schemaColumnNames = schema.columns().stream().map(Column::name).toList();
        var targetColumns = statement.columns().isEmpty() ? schemaColumnNames : statement.columns();

        var inserted = 0;
        for (var rowExpr : statement.rows()) {
            if (rowExpr.size() != targetColumns.size()) {
                throw new IllegalArgumentException("INSERT row value count does not match target column count");
            }

            var values = new LinkedHashMap<String, Object>();
            for (var columnName : schemaColumnNames) {
                values.put(columnName, null);
            }

            for (int i = 0; i < targetColumns.size(); i++) {
                values.put(targetColumns.get(i), evaluate(rowExpr.get(i), Map.of()));
            }

            tableStore.insertRow(statement.table(), values);
            inserted++;
        }

        return QueryResult.affectedRows(inserted);
    }

    private QueryResult executeUpdate(UpdateStatement statement) {
        var updated = tableStore.updateRows(
            statement.table(),
            row -> statement.whereClause() == null || asBoolean(evaluate(statement.whereClause(), row)),
            row -> {
                var original = new LinkedHashMap<>(row);
                for (var assignment : statement.assignments().entrySet()) {
                    row.put(assignment.getKey(), evaluate(assignment.getValue(), original));
                }
                return row;
            }
        );
        return QueryResult.affectedRows(updated);
    }

    private QueryResult executeDelete(DeleteStatement statement) {
        var deleted = tableStore.deleteRows(
            statement.table(),
            row -> statement.whereClause() == null || asBoolean(evaluate(statement.whereClause(), row))
        );
        return QueryResult.affectedRows(deleted);
    }

    private QueryResult executeSelect(SelectStatement statement) {
        var rows = tableStore.scan(statement.fromTable());
        var schemaColumns = tableStore.columns(statement.fromTable());

        var outputColumns = new ArrayList<String>();
        var outputRows = new ArrayList<List<Object>>();

        var selectAll = statement.selectList().size() == 1 && statement.selectList().get(0) instanceof ColumnRef columnRef && "*".equals(columnRef.column());
        if (selectAll) {
            for (var column : schemaColumns) {
                outputColumns.add(column.name());
            }
        } else {
            for (int i = 0; i < statement.selectList().size(); i++) {
                outputColumns.add(columnLabel(statement.selectList().get(i), i));
            }
        }

        for (var row : rows) {
            if (statement.whereClause() != null && !asBoolean(evaluate(statement.whereClause(), row))) {
                continue;
            }

            var outputRow = new ArrayList<Object>();
            if (selectAll) {
                for (var column : schemaColumns) {
                    outputRow.add(row.get(column.name()));
                }
            } else {
                for (var expression : statement.selectList()) {
                    outputRow.add(evaluate(expression, row));
                }
            }
            outputRows.add(outputRow);
        }

        return QueryResult.resultSet(List.copyOf(outputColumns), List.copyOf(outputRows));
    }

    private String columnLabel(Expression expression, int index) {
        if (expression instanceof ColumnRef columnRef) {
            return columnRef.column();
        }
        return "expr" + (index + 1);
    }

    private Object evaluate(Expression expression, Map<String, Object> row) {
        if (expression instanceof Literal literal) {
            return literal.value();
        }
        if (expression instanceof ColumnRef columnRef) {
            if ("*".equals(columnRef.column())) {
                throw new IllegalArgumentException("Wildcard cannot be evaluated as a scalar expression");
            }
            if (columnRef.table() != null && !columnRef.table().isBlank()) {
                // Table qualification is accepted syntactically; rows are single-table in this slice.
            }
            if (!row.containsKey(columnRef.column())) {
                throw new IllegalArgumentException("Unknown column in row: " + columnRef.column());
            }
            return row.get(columnRef.column());
        }
        if (expression instanceof BinaryOp binaryOp) {
            return evaluateBinary(binaryOp, row);
        }

        throw new UnsupportedOperationException("Unsupported expression type: " + typeName(expression));
    }

    private Object evaluateBinary(BinaryOp binaryOp, Map<String, Object> row) {
        return switch (binaryOp.op()) {
            case AND -> asBoolean(evaluate(binaryOp.left(), row)) && asBoolean(evaluate(binaryOp.right(), row));
            case OR -> asBoolean(evaluate(binaryOp.left(), row)) || asBoolean(evaluate(binaryOp.right(), row));
            case EQ -> compare(evaluate(binaryOp.left(), row), evaluate(binaryOp.right(), row)) == 0;
            case NEQ -> compare(evaluate(binaryOp.left(), row), evaluate(binaryOp.right(), row)) != 0;
            case LT -> compare(evaluate(binaryOp.left(), row), evaluate(binaryOp.right(), row)) < 0;
            case GT -> compare(evaluate(binaryOp.left(), row), evaluate(binaryOp.right(), row)) > 0;
            case LTE -> compare(evaluate(binaryOp.left(), row), evaluate(binaryOp.right(), row)) <= 0;
            case GTE -> compare(evaluate(binaryOp.left(), row), evaluate(binaryOp.right(), row)) >= 0;
            case PLUS -> add(evaluate(binaryOp.left(), row), evaluate(binaryOp.right(), row));
            case MINUS -> subtract(evaluate(binaryOp.left(), row), evaluate(binaryOp.right(), row));
            case STAR -> multiply(evaluate(binaryOp.left(), row), evaluate(binaryOp.right(), row));
            case SLASH -> divide(evaluate(binaryOp.left(), row), evaluate(binaryOp.right(), row));
        };
    }

    private boolean asBoolean(Object value) {
        if (value instanceof Boolean booleanValue) {
            return booleanValue;
        }
        throw new IllegalArgumentException("Expected BOOLEAN value but got: " + value);
    }

    private int compare(Object left, Object right) {
        if (left == null || right == null) {
            return left == right ? 0 : (left == null ? -1 : 1);
        }
        if (left instanceof Number leftNumber && right instanceof Number rightNumber) {
            return Double.compare(leftNumber.doubleValue(), rightNumber.doubleValue());
        }
        if (left instanceof Comparable<?> comparable && left.getClass().isInstance(right)) {
            @SuppressWarnings("unchecked")
            var typedComparable = (Comparable<Object>) comparable;
            return typedComparable.compareTo(right);
        }
        return left.toString().compareTo(right.toString());
    }

    private Object add(Object left, Object right) {
        return numeric(left, right, NumericOp.ADD);
    }

    private Object subtract(Object left, Object right) {
        return numeric(left, right, NumericOp.SUBTRACT);
    }

    private Object multiply(Object left, Object right) {
        return numeric(left, right, NumericOp.MULTIPLY);
    }

    private Object divide(Object left, Object right) {
        return numeric(left, right, NumericOp.DIVIDE);
    }

    private Object numeric(Object left, Object right, NumericOp op) {
        if (!(left instanceof Number leftNumber) || !(right instanceof Number rightNumber)) {
            throw new IllegalArgumentException("Numeric operator requires number operands");
        }

        var useDouble = leftNumber instanceof Double || leftNumber instanceof Float || rightNumber instanceof Double || rightNumber instanceof Float || op == NumericOp.DIVIDE;
        if (useDouble) {
            return switch (op) {
                case ADD -> leftNumber.doubleValue() + rightNumber.doubleValue();
                case SUBTRACT -> leftNumber.doubleValue() - rightNumber.doubleValue();
                case MULTIPLY -> leftNumber.doubleValue() * rightNumber.doubleValue();
                case DIVIDE -> leftNumber.doubleValue() / rightNumber.doubleValue();
            };
        }

        return switch (op) {
            case ADD -> leftNumber.longValue() + rightNumber.longValue();
            case SUBTRACT -> leftNumber.longValue() - rightNumber.longValue();
            case MULTIPLY -> leftNumber.longValue() * rightNumber.longValue();
            case DIVIDE -> leftNumber.longValue() / rightNumber.longValue();
        };
    }

    private enum NumericOp {
        ADD,
        SUBTRACT,
        MULTIPLY,
        DIVIDE
    }

    private String typeName(Object value) {
        return value == null ? "null" : value.getClass().getSimpleName();
    }
}