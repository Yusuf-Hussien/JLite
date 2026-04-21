package jlite.ast;

public record DropTableStatement(String table) implements Statement {}