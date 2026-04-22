package jlite.agent;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Pattern;

import jlite.catalogue.Column;
import jlite.catalogue.DataType;
import jlite.catalogue.TableSchema;
import jlite.config.StorageConfigResolver;
import jlite.executor.QueryEngine;
import jlite.executor.QueryResult;

/**
 * Natural-language → SQL agent.
 *
 * Accepts an English question, builds a schema-aware prompt,
 * calls the Claude API, receives SQL, validates and executes it.
 *
 * TODO: inject live schema context from Catalogue.
 * TODO: call Anthropic SDK with system prompt + user question.
 * TODO: parse LLM response to extract the SQL block.
 * TODO: retry loop (max 3) feeding validation errors back to the LLM.
 * TODO: multi-turn conversation support via message history.
 * TODO: safety guardrails: refuse DROP/DELETE-without-WHERE unless confirmed.
 */
public class NlAgent {

    private final SchemaContextBuilder schemaContextBuilder;
    private final SqlValidator sqlValidator;
    private final QueryEngine queryEngine;
    private final SqlTranslator sqlTranslator;
    private final SqlTranslator fallbackTranslator;

    @FunctionalInterface
    public interface SqlTranslator {
        String translate(String naturalLanguageQuestion, String schemaContext);
    }

    public NlAgent(SchemaContextBuilder schemaContextBuilder, SqlValidator sqlValidator) {
        this(
            schemaContextBuilder,
            sqlValidator,
            new QueryEngine(),
            createDefaultTranslator()
        );
    }

    public NlAgent(
        SchemaContextBuilder schemaContextBuilder,
        SqlValidator sqlValidator,
        QueryEngine queryEngine,
        SqlTranslator sqlTranslator
    ) {
        this.schemaContextBuilder = schemaContextBuilder;
        this.sqlValidator = sqlValidator;
        this.queryEngine = queryEngine;
        this.sqlTranslator = sqlTranslator;
        this.fallbackTranslator = heuristicTranslator();
    }

    public String query(String naturalLanguageQuestion) {
        if (naturalLanguageQuestion == null || naturalLanguageQuestion.isBlank()) {
            throw new IllegalArgumentException("Question must not be empty");
        }

        var assistantResponse = maybeAssistantResponse(naturalLanguageQuestion);
        if (assistantResponse != null) {
            return assistantResponse;
        }

        var schemaContext = schemaContextBuilder.build();
        var capabilityContext = AgentCapabilities.promptContext();
        var promptContext = schemaContext.isBlank() ? capabilityContext : schemaContext + "\n\n" + capabilityContext;
        String sql;
        try {
            sql = sqlTranslator.translate(naturalLanguageQuestion, promptContext);
        } catch (RuntimeException ex) {
            sql = fallbackTranslator.translate(naturalLanguageQuestion, promptContext);
            if (sql == null || sql.isBlank()) {
                throw new IllegalArgumentException("Could not translate question to SQL", ex);
            }
            System.err.println("Gemini unavailable, used local fallback translation: " + ex.getMessage());
        }

        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException("Could not translate question to SQL");
        }

        sql = normalizeSql(sql);

        var validation = sqlValidator.validate(sql);
        if (!validation.ok()) {
            var repairedSql = attemptRemoteSqlRepair(naturalLanguageQuestion, promptContext, sql, validation.errorMessage());
            if (repairedSql != null) {
                sql = repairedSql;
            } else {
                // Remote repair failed. Keep local fallback as a safety net.
                var fallbackSql = fallbackTranslator.translate(naturalLanguageQuestion, promptContext);
                if (fallbackSql != null && !fallbackSql.isBlank()) {
                    fallbackSql = normalizeSql(fallbackSql);
                    var fallbackValidation = sqlValidator.validate(fallbackSql);
                    if (fallbackValidation.ok()) {
                        sql = fallbackSql;
                    } else {
                        throw new IllegalArgumentException("Generated SQL is invalid: " + validation.errorMessage());
                    }
                } else {
                    throw new IllegalArgumentException("Generated SQL is invalid: " + validation.errorMessage());
                }
            }
        }

        var intentNarrowedSql = attemptIntentAwareRemoteNarrowing(naturalLanguageQuestion, promptContext, sql);
        if (intentNarrowedSql != null) {
            sql = intentNarrowedSql;
        }

        var result = queryEngine.execute(sql);
        return renderAnswer(sql, result);
    }

    private String attemptRemoteSqlRepair(String originalQuestion, String promptContext, String invalidSql, String validationError) {
        try {
            var repaired = sqlTranslator.translate(buildRepairPrompt(originalQuestion, invalidSql, validationError), promptContext);
            if (repaired == null || repaired.isBlank()) {
                throw new IllegalArgumentException("Generated SQL is invalid: " + validationError);
            }
            repaired = normalizeSql(repaired);
            var repairedValidation = sqlValidator.validate(repaired);
            return repairedValidation.ok() ? repaired : null;
        } catch (RuntimeException ex) {
            return null;
        }
    }

    private static String buildRepairPrompt(String originalQuestion, String invalidSql, String validationError) {
        return String.join("\n",
            "Rewrite the SQL to be valid for JLite and return only one SQL statement.",
            "Do not use unsupported syntax like JOIN, GROUP BY, ORDER BY, LIMIT, DISTINCT, or subqueries.",
            "Original user request: " + originalQuestion,
            "Previous invalid SQL: " + invalidSql,
            "Validation error: " + validationError
        );
    }

    private String attemptIntentAwareRemoteNarrowing(String originalQuestion, String promptContext, String currentSql) {
        if (!asksForSingleFirstUser(originalQuestion) || !isBroadUsersSelect(currentSql)) {
            return null;
        }

        try {
            var repaired = sqlTranslator.translate(buildIntentRepairPrompt(originalQuestion, currentSql), promptContext);
            if (repaired == null || repaired.isBlank()) {
                return null;
            }
            repaired = normalizeSql(repaired);
            var repairedValidation = sqlValidator.validate(repaired);
            return repairedValidation.ok() ? repaired : null;
        } catch (RuntimeException ex) {
            return null;
        }
    }

    private static boolean asksForSingleFirstUser(String question) {
        var normalized = question.trim().toLowerCase(Locale.ROOT);
        return normalized.matches(".*\\b(first|1st|single|one)\\b.*\\buser\\b.*");
    }

    private static boolean isBroadUsersSelect(String sql) {
        var normalized = sql.trim().replaceAll("\\s+", " ").toLowerCase(Locale.ROOT);
        return normalized.equals("select * from users") || normalized.equals("select * from users;");
    }

    private static String buildIntentRepairPrompt(String originalQuestion, String currentSql) {
        return String.join("\n",
            "Rewrite the SQL to preserve the user intent and return only one SQL statement.",
            "JLite does not support ORDER BY or LIMIT.",
            "If the user asks for the first/single user and table users has an id column, prefer WHERE id = 1.",
            "Original user request: " + originalQuestion,
            "Current SQL that is too broad: " + currentSql
        );
    }

    public static void main(String[] args) {
        var engine = createEngine();
        seedDemoData(engine);
        var agent = createConfiguredAgent(engine);
        try (var scanner = new Scanner(System.in)) {
            System.out.println("JLite Agent ready. Type a question or \"quit\".");
            while (true) {
                System.out.print("you> ");
                if (!scanner.hasNextLine()) {
                    break;
                }
                var line = scanner.nextLine();
                if (line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit")) {
                    break;
                }
                if (line.equalsIgnoreCase("\\remote-check")) {
                    System.out.println(remoteCheck());
                    continue;
                }
                if (line.isBlank()) {
                    continue;
                }
                try {
                    System.out.println(agent.query(line));
                } catch (RuntimeException ex) {
                    System.out.println("error: " + ex.getMessage());
                }
            }
        }
    }

    private String renderAnswer(String sql, QueryResult result) {
        var output = new StringBuilder();
        output.append("SQL: ").append(sql).append("\n");
        output.append("Columns: ").append(result.columns()).append("\n");
        output.append("Row count: ").append(result.rows().size());
        if (!result.rows().isEmpty()) {
            output.append("\nRows:");
            for (var row : result.rows()) {
                output.append("\n- ").append(row);
            }
        }
        return output.toString();
    }

    private static SqlTranslator createDefaultTranslator() {
        var configured = GeminiConfigResolver.resolve();
        if (configured.isPresent()) {
            var config = configured.get();
            return new GeminiSqlTranslator(config.apiKey(), GeminiConfigResolver.candidateModels(config.model()));
        }

        return heuristicTranslator();
    }

    private static SqlTranslator heuristicTranslator() {
        return (question, schemaContext) -> {
            var extracted = extractSql(question);
            if (extracted != null) {
                return extracted;
            }

            var normalized = question.trim().toLowerCase(Locale.ROOT);
            var showAll = Pattern.compile("(?:show|list)\\s+(?:all\\s+)?(?:rows\\s+)?(?:from\\s+)?([a-zA-Z_][a-zA-Z0-9_]*)");
            var matcher = showAll.matcher(normalized);
            if (matcher.matches()) {
                return "SELECT * FROM " + matcher.group(1);
            }

            if (normalized.matches(".*\\b(first|1st)\\b.*\\buser\\b.*")) {
                return "SELECT * FROM users WHERE id = 1";
            }

            if (normalized.startsWith("select ")) {
                return question.trim();
            }

            return null;
        };
    }

    private static NlAgent createConfiguredAgent(QueryEngine queryEngine) {
        var schemaBuilder = new SchemaContextBuilder(queryEngine.catalogue());
        var validator = new SqlValidator(queryEngine.catalogue());
        var translator = createDefaultTranslator();
        return new NlAgent(schemaBuilder, validator, queryEngine, translator);
    }

    static String remoteCheck() {
        var configured = GeminiConfigResolver.resolve();
        if (configured.isEmpty()) {
            return "REMOTE_STATUS=ERROR\nDETAIL=GEMINI_API_KEY not configured";
        }

        var config = configured.get();
        RuntimeException lastFailure = null;

        for (var model : GeminiConfigResolver.candidateModels(config.model())) {
            try {
                var translator = new GeminiSqlTranslator(config.apiKey(), model);
                var sql = translator.translate("List all users", "users(id INT, name TEXT)");
                return "REMOTE_STATUS=OK\nMODEL=" + model + "\nSQL=" + sql;
            } catch (RuntimeException ex) {
                lastFailure = ex;
            }
        }

        return "REMOTE_STATUS=ERROR\nMODEL=" + config.model() + "\nDETAIL=" + (lastFailure == null ? "No Gemini models configured" : lastFailure.getMessage());
    }

    private String maybeAssistantResponse(String question) {
        var normalized = question.trim().toLowerCase(Locale.ROOT);
        if (normalized.matches("^(h+i+|hello+|hey+|yo+|hola+)$")) {
            return "Hi. Ask me about your data, for example: show users, list rows from users, or a SQL query like SELECT * FROM users.";
        }

        if (normalized.matches(".*\\bhow\\s+are\\s+(you|u)\\b.*")
            || normalized.matches(".*\\bwho\\s+are\\s+(you|u)\\b.*")
            || normalized.matches(".*\\bwhat('?s| is)\\s+your\\s+name\\b.*")
            || normalized.matches(".*\\bthank(s| you)?\\b.*")) {
            return "I am JLite Agent. I translate natural language to JLite SQL and execute it against your tables. Ask me about your data, for example: users with name Cara, active users, or help.";
        }

        if (normalized.equals("help")
            || normalized.contains("what can you do")
            || normalized.contains("what u can do")
            || normalized.contains("what u could do")
            || normalized.contains("capabilities")
            || normalized.contains("ability")) {
            return AgentCapabilities.helpText();
        }

        return null;
    }

    static String normalizeSql(String sql) {
        var trimmed = sql.trim().replaceFirst("(?i)^slect\\s+", "SELECT ");
        var lower = trimmed.toLowerCase(Locale.ROOT);
        var prefix = "select all from ";
        if (lower.startsWith(prefix) && trimmed.length() > prefix.length()) {
            return "SELECT * FROM " + trimmed.substring(prefix.length()).trim();
        }
        return trimmed;
    }

    private static QueryEngine createEngine() {
        return StorageConfigResolver.resolveStorageDir().map(QueryEngine::new).orElseGet(QueryEngine::new);
    }

    private static void seedDemoData(QueryEngine queryEngine) {
        if (queryEngine.catalogue().hasTable("users")) {
            return;
        }

        queryEngine.createTable(new TableSchema(
            "users",
            List.of(
                new Column("id", DataType.INT, false, true),
                new Column("name", DataType.TEXT, true, false),
                new Column("age", DataType.INT, true, false),
                new Column("active", DataType.BOOLEAN, true, false)
            )
        ));
        queryEngine.insertRow("users", Map.of("id", 1L, "name", "Alice", "age", 30L, "active", true));
        queryEngine.insertRow("users", Map.of("id", 2L, "name", "Bob", "age", 17L, "active", false));
        queryEngine.insertRow("users", Map.of("id", 3L, "name", "Cara", "age", 41L, "active", true));
    }

    static String extractSql(String text) {
        var fenced = Pattern.compile("```sql\\s*(.*?)\\s*```", Pattern.CASE_INSENSITIVE | Pattern.DOTALL).matcher(text);
        if (fenced.find()) {
            return fenced.group(1).trim();
        }

        var candidate = text.trim();
        var upper = candidate.toUpperCase(Locale.ROOT);
        if (upper.startsWith("SELECT ") || upper.startsWith("INSERT ") || upper.startsWith("UPDATE ") || upper.startsWith("DELETE ")
            || upper.startsWith("CREATE ") || upper.startsWith("DROP ") || upper.startsWith("ALTER ")) {
            return normalizeSql(candidate);
        }

        return null;
    }
}
