package jlite.ast;
import java.util.List;
/** TODO: add distinct, joins, groupBy, having, orderBy, limit, offset. */
public record SelectStatement(List<Expression> selectList, String fromTable, Expression whereClause) implements Statement {}
