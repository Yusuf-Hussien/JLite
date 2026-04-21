package jlite.server.protocol;

public record ServerRequest(String type, String sql) {}