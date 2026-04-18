package jlite.mcp;

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

    public void startStdio() {
        // TODO: implement stdio transport loop
        throw new UnsupportedOperationException("McpServer.startStdio() not yet implemented");
    }
}
