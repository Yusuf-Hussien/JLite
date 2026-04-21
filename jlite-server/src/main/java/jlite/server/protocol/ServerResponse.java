package jlite.server.protocol;

import java.util.List;

public record ServerResponse(String type, List<String> columns, List<List<Object>> rows, String message) {}