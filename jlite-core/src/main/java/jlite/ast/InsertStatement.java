package jlite.ast;
import java.util.List;
public record InsertStatement(String table, List<String> columns, List<List<Expression>> rows) implements Statement {}
