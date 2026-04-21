package jlite.executor;

import jlite.catalogue.Catalogue;
import jlite.catalogue.Column;
import jlite.catalogue.TableSchema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

public class InMemoryTableStore implements TableStore {

    private final Catalogue catalogue;
    private final Map<String, List<Map<String, Object>>> rowsByTable = new ConcurrentHashMap<>();

    public InMemoryTableStore(Catalogue catalogue) {
        this.catalogue = catalogue;
    }

    @Override
    public void createTable(TableSchema schema) {
        catalogue.createTable(schema);
        rowsByTable.put(schema.name(), new ArrayList<>());
    }

    @Override
    public void dropTable(String tableName) {
        catalogue.dropTable(tableName);
        rowsByTable.remove(tableName);
    }

    @Override
    public void addColumn(String tableName, Column column) {
        catalogue.addColumn(tableName, column);
        var rows = rowsByTable.get(tableName);
        if (rows != null) {
            for (var row : rows) {
                row.put(column.name(), null);
            }
        }
    }

    @Override
    public void dropColumn(String tableName, String columnName) {
        catalogue.dropColumn(tableName, columnName);
        var rows = rowsByTable.get(tableName);
        if (rows != null) {
            for (var row : rows) {
                var keyToRemove = row.keySet().stream()
                    .filter(key -> key.equalsIgnoreCase(columnName))
                    .findFirst()
                    .orElse(null);
                if (keyToRemove != null) {
                    row.remove(keyToRemove);
                }
            }
        }
    }

    @Override
    public void insertRow(String tableName, Map<String, Object> values) {
        var schema = resolveSchema(tableName);
        var row = new LinkedHashMap<String, Object>();

        for (var column : schema.columns()) {
            row.put(column.name(), values.get(column.name()));
        }

        for (var valueColumn : values.keySet()) {
            if (schema.columns().stream().noneMatch(column -> column.name().equalsIgnoreCase(valueColumn))) {
                throw new IllegalArgumentException("Unknown column in insert: " + valueColumn);
            }
        }

        rowsByTable.get(tableName).add(row);
    }

    @Override
    public int updateRows(String tableName, Predicate<Map<String, Object>> predicate, UnaryOperator<Map<String, Object>> updater) {
        resolveSchema(tableName);
        var rows = rowsByTable.getOrDefault(tableName, List.of());
        var updated = 0;
        for (int i = 0; i < rows.size(); i++) {
            var row = rows.get(i);
            if (predicate.test(row)) {
                rows.set(i, updater.apply(new LinkedHashMap<>(row)));
                updated++;
            }
        }
        return updated;
    }

    @Override
    public int deleteRows(String tableName, Predicate<Map<String, Object>> predicate) {
        resolveSchema(tableName);
        var rows = rowsByTable.getOrDefault(tableName, List.of());
        var before = rows.size();
        rows.removeIf(predicate);
        return before - rows.size();
    }

    @Override
    public List<Map<String, Object>> scan(String tableName) {
        resolveSchema(tableName);
        var rows = rowsByTable.getOrDefault(tableName, List.of());
        var snapshot = new ArrayList<Map<String, Object>>(rows.size());
        for (var row : rows) {
            snapshot.add(Collections.unmodifiableMap(new LinkedHashMap<>(row)));
        }
        return List.copyOf(snapshot);
    }

    @Override
    public TableSchema resolveSchema(String tableName) {
        return catalogue.getTable(tableName)
            .orElseThrow(() -> new IllegalArgumentException("Unknown table: " + tableName));
    }

    @Override
    public List<Column> columns(String tableName) {
        return resolveSchema(tableName).columns();
    }
}