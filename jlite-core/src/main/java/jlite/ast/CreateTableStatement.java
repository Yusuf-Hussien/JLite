package jlite.ast;

import java.util.List;

import jlite.catalogue.DataType;

public record CreateTableStatement(String table, List<ColumnDef> columns) implements Statement {
    public record ColumnDef(String name, DataType type) {}
}