package jlite.ast;
public record ColumnRef(String table, String column) implements Expression {}
