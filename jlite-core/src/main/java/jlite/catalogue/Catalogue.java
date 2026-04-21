package jlite.catalogue;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
/**
 * In-memory catalogue.
 * TODO: persist to disk, expose system tables (jlite_tables, jlite_columns, jlite_indexes).
 */
public class Catalogue {
    private final ConcurrentHashMap<String, TableSchema> tables = new ConcurrentHashMap<>();

    public void createTable(TableSchema schema) {
        if (tables.putIfAbsent(schema.name(), schema) != null)
            throw new IllegalStateException("Table already exists: " + schema.name());
    }

    public void dropTable(String name) {
        if (tables.remove(name) == null) throw new IllegalStateException("Table not found: " + name);
    }

    public boolean hasTable(String name) {
        return tables.containsKey(name);
    }

    public void addColumn(String tableName, Column column) {
        var schema = tables.get(tableName);
        if (schema == null) {
            throw new IllegalStateException("Table not found: " + tableName);
        }
        var exists = schema.columns().stream().anyMatch(existing -> existing.name().equalsIgnoreCase(column.name()));
        if (exists) {
            throw new IllegalStateException("Column already exists: " + column.name());
        }
        var columns = new ArrayList<>(schema.columns());
        columns.add(column);
        tables.put(tableName, new TableSchema(schema.name(), List.copyOf(columns)));
    }

    public void dropColumn(String tableName, String columnName) {
        var schema = tables.get(tableName);
        if (schema == null) {
            throw new IllegalStateException("Table not found: " + tableName);
        }
        var columns = new ArrayList<Column>();
        var removed = false;
        for (var column : schema.columns()) {
            if (column.name().equalsIgnoreCase(columnName)) {
                removed = true;
            } else {
                columns.add(column);
            }
        }
        if (!removed) {
            throw new IllegalStateException("Column not found: " + columnName);
        }
        tables.put(tableName, new TableSchema(schema.name(), List.copyOf(columns)));
    }

    public Optional<TableSchema> getTable(String name) { return Optional.ofNullable(tables.get(name)); }
    public Collection<TableSchema> allTables() { return tables.values(); }
}
