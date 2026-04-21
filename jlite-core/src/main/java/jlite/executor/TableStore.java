package jlite.executor;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import jlite.catalogue.Column;
import jlite.catalogue.TableSchema;

public interface TableStore {

    void createTable(TableSchema schema);

    void dropTable(String tableName);

    void addColumn(String tableName, Column column);

    void dropColumn(String tableName, String columnName);

    void insertRow(String tableName, Map<String, Object> values);

    int updateRows(String tableName, Predicate<Map<String, Object>> predicate, UnaryOperator<Map<String, Object>> updater);

    int deleteRows(String tableName, Predicate<Map<String, Object>> predicate);

    List<Map<String, Object>> scan(String tableName);

    TableSchema resolveSchema(String tableName);

    List<Column> columns(String tableName);
}