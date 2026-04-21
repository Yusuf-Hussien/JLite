package jlite.executor;

import java.util.List;

public record QueryResult(List<String> columns, List<List<Object>> rows) {

	public static QueryResult resultSet(List<String> columns, List<List<Object>> rows) {
		return new QueryResult(columns, rows);
	}

	public static QueryResult affectedRows(int count) {
		return new QueryResult(List.of("affected_rows"), List.of(List.of(count)));
	}
}