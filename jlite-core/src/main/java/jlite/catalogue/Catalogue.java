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
    public Optional<TableSchema> getTable(String name) { return Optional.ofNullable(tables.get(name)); }
    public Collection<TableSchema> allTables() { return tables.values(); }
}
