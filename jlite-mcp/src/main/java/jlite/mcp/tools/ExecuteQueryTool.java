package jlite.mcp.tools;

import java.util.LinkedHashMap;
import java.util.Map;

import jlite.executor.QueryEngine;
import jlite.executor.QueryResult;

/**
 * MCP tool: execute_query
 *
 * Input:  { "sql": "..." }
 * Output: { "rows": [...], "rowCount": N, "columns": [...] }
 *
 * TODO: connect to running JLite engine instance.
 * TODO: timeout and row-limit guard for agent queries.
 */
public class ExecuteQueryTool {

    private final QueryEngine queryEngine;

    public ExecuteQueryTool(QueryEngine queryEngine, boolean allowMutations) {
        this.queryEngine = queryEngine;
    }

    public ExecuteQueryTool(QueryEngine queryEngine) {
        this(queryEngine, false);
    }

    public Object execute(String sql) {
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException("sql must not be blank");
        }

        QueryResult result = queryEngine.execute(sql);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("columns", result.columns());
        payload.put("rows", result.rows());
        payload.put("rowCount", result.rows().size());
        return payload;
    }
}
