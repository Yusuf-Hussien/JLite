package jlite.mcp.tools;

/**
 * MCP tool: execute_query
 *
 * Input:  { "sql": "SELECT ..." }
 * Output: { "rows": [...], "rowCount": N, "columns": [...] }
 *
 * TODO: connect to running JLite engine instance.
 * TODO: enforce query-only mode (no DDL) unless an elevated token is provided.
 * TODO: timeout and row-limit guard for agent queries.
 */
public class ExecuteQueryTool {

    public Object execute(String sql) {
        // TODO: implement
        throw new UnsupportedOperationException("ExecuteQueryTool not yet implemented");
    }
}
