package jlite.ast;
public record DeleteStatement(String table, Expression whereClause) implements Statement {}
