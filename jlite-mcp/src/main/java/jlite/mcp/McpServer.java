package jlite.mcp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import jlite.config.StorageConfigResolver;
import jlite.executor.QueryEngine;
import jlite.mcp.tools.ExecuteQueryTool;

/**
 * MCP (Model Context Protocol) Server exposing JLite as AI-callable tools.
 *
 * Supports stdio transport (default) and SSE for remote agents.
 *
 * TODO: implement MCP handshake, tool registration, request dispatch.
 * TODO: authentication token validation.
 * TODO: expose each table as a browsable MCP resource.
 * TODO: SSE transport for remote connectivity.
 */
public class McpServer {

    private final ObjectMapper objectMapper;
    private final ExecuteQueryTool executeQueryTool;

    public McpServer() {
        this(StorageConfigResolver.resolveStorageDir().map(QueryEngine::new).orElseGet(QueryEngine::new));
    }

    public McpServer(QueryEngine queryEngine) {
        this.objectMapper = new ObjectMapper();
        this.executeQueryTool = new ExecuteQueryTool(queryEngine, true);
    }

    public void startStdio() {
        startStdio(System.in, System.out);
    }

    void startStdio(InputStream input, OutputStream output) {
        try (var reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
             var writer = new PrintWriter(output, true, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                writer.println(handleJsonLine(line));
            }
        } catch (IOException ex) {
            throw new RuntimeException("MCP stdio loop failed", ex);
        }
    }

    String handleJsonLine(String jsonLine) {
        Request request;
        try {
            request = objectMapper.readValue(jsonLine, Request.class);
        } catch (JsonProcessingException ex) {
            return toJson(Map.of(
                "id", null,
                "ok", false,
                "error", "Invalid JSON request: " + ex.getOriginalMessage()
            ));
        }

        try {
            return toJson(dispatch(request));
        } catch (RuntimeException ex) {
            return toJson(Map.of(
                "id", request.id(),
                "ok", false,
                "error", ex.getMessage()
            ));
        }
    }

    private Map<String, Object> dispatch(Request request) {
        return switch (request.method()) {
            case "ping" -> Map.of("id", request.id(), "ok", true, "result", Map.of("status", "pong"));
            case "initialize" -> initializeResult(request.id());
            case "tools/list" -> toolsListResult(request.id());
            case "tools/call" -> callToolResult(request.id(), request.params());
            default -> Map.of("id", request.id(), "ok", false, "error", "Unsupported method: " + request.method());
        };
    }

    private Map<String, Object> initializeResult(String id) {
        return Map.of(
            "id", id,
            "ok", true,
            "result", Map.of(
                "serverName", "jlite-mcp",
                "serverVersion", "0.1.0",
                "capabilities", Map.of("tools", true, "sqlStatements", List.of("SELECT", "INSERT", "UPDATE", "DELETE", "CREATE TABLE", "DROP TABLE", "ALTER TABLE"))
            )
        );
    }

    private Map<String, Object> toolsListResult(String id) {
        return Map.of(
            "id", id,
            "ok", true,
            "result", Map.of(
                "tools", List.of(Map.of(
                    "name", "execute_sql",
                    "description", "Execute any supported JLite SQL statement and return structured results",
                    "inputSchema", Map.of(
                        "type", "object",
                        "required", List.of("sql"),
                        "properties", Map.of("sql", Map.of("type", "string"))
                    )
                ), Map.of(
                    "name", "execute_query",
                    "description", "Backward-compatible alias for execute_sql",
                    "inputSchema", Map.of(
                        "type", "object",
                        "required", List.of("sql"),
                        "properties", Map.of("sql", Map.of("type", "string"))
                    )
                ))
            )
        );
    }

    private Map<String, Object> callToolResult(String id, Map<String, Object> params) {
        if (params == null) {
            return Map.of("id", id, "ok", false, "error", "Missing params");
        }

        var toolName = String.valueOf(params.getOrDefault("name", ""));
        if (!"execute_query".equals(toolName) && !"execute_sql".equals(toolName)) {
            return Map.of("id", id, "ok", false, "error", "Unknown tool: " + toolName);
        }

        var arguments = params.get("arguments");
        if (!(arguments instanceof Map<?, ?> rawArgs)) {
            return Map.of("id", id, "ok", false, "error", "tools/call requires arguments object");
        }

        var sqlValue = rawArgs.get("sql");
        if (!(sqlValue instanceof String sql) || sql.isBlank()) {
            return Map.of("id", id, "ok", false, "error", "execute_query requires non-empty sql");
        }

        var result = executeQueryTool.execute(sql);
        return Map.of("id", id, "ok", true, "result", result);
    }

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException ex) {
            throw new RuntimeException("Failed to encode response JSON", ex);
        }
    }

    public static void main(String[] args) {
        new McpServer().startStdio();
    }

    private record Request(String id, String method, Map<String, Object> params) {
    }
}
