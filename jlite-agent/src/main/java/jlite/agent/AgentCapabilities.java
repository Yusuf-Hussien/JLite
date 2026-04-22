package jlite.agent;

import java.util.List;

final class AgentCapabilities {

    private static final List<String> SUPPORTED_STATEMENTS = List.of(
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
        "CREATE TABLE",
        "DROP TABLE",
        "ALTER TABLE"
    );

    private AgentCapabilities() {
    }

    static String promptContext() {
        return String.join("\n",
            "JLite capability summary:",
            "- Supported statements: " + String.join(", ", SUPPORTED_STATEMENTS),
            "- Single-table queries only",
            "- WHERE clauses support boolean logic and basic comparisons",
            "- Prefer SELECT when the user asks to inspect data",
            "- Use INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE, or ALTER TABLE only when the user explicitly requests a data or schema change"
        );
    }

    static String helpText() {
        return String.join("\n",
            "I can translate JLite SQL and execute it directly.",
            "Supported statements: " + String.join(", ", SUPPORTED_STATEMENTS) + ".",
            "Examples: show users, list all from users, SELECT id, name FROM users, INSERT INTO users VALUES (...), UPDATE users SET ..., DELETE FROM users WHERE ..., CREATE TABLE users (...)."
        );
    }
}