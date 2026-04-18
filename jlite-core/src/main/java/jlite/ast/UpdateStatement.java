package jlite.ast;
import java.util.Map;
public record UpdateStatement(String table, Map<String, Expression> assignments, Expression whereClause) implements Statement {}
