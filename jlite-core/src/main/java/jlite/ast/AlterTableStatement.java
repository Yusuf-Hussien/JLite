package jlite.ast;

import jlite.catalogue.DataType;

public record AlterTableStatement(String table, Action action, String column, DataType type) implements Statement {
    public enum Action {
        ADD_COLUMN,
        DROP_COLUMN
    }
}