package jlite.cli;

import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import jlite.catalogue.Column;
import jlite.catalogue.DataType;
import jlite.catalogue.TableSchema;
import jlite.config.StorageConfigResolver;
import jlite.executor.QueryEngine;
import jlite.executor.QueryResult;

/**
 * Interactive REPL shell for JLite.
 *
 * TODO: JLine3 integration for history, arrow-key editing, syntax highlighting.
 * TODO: meta-commands: \tables, \describe <table>, \explain <sql>, \timing, \connect, \import.
 * TODO: multi-line statement accumulation (continue until semicolon).
 * TODO: connect to remote TCP server via \connect host:port.
 */
public class Repl {

    private static final String USER_PROMPT = "you> ";
    private static final String ASSISTANT_PREFIX = "jlite> ";

    private final QueryEngine queryEngine;
    private Theme theme;
    private boolean useColor;

    public Repl() {
        this(Theme.AUTO);
    }

    public Repl(Theme theme) {
        this.queryEngine = createConfiguredEngine();
        this.theme = theme == null ? Theme.AUTO : theme;
        seedDemoData();
    }

    public void run() {
        run(new InputStreamReader(System.in), System.out);
    }

    public void run(Reader input, PrintStream output) {
        this.useColor = shouldUseColor(output);
        printBanner(output);
        output.println(muted("Session started. Type SQL and press Enter."));
        output.println(muted("Commands: \\help, \\quit, \\q"));
        output.println(muted("Dataset: users (demo table loaded)"));
        var scanner = new Scanner(input);
        while (true) {
            output.print(userPrompt());
            output.flush();
            if (!scanner.hasNextLine()) {
                break;
            }

            var line = scanner.nextLine().trim();
            if (line.equalsIgnoreCase("\\quit") || line.equalsIgnoreCase("\\q")) break;
            if (line.isEmpty()) continue;
            if (line.equalsIgnoreCase("\\help")) {
                printHelp(output);
                continue;
            }
            if (line.toLowerCase().startsWith("\\theme")) {
                handleThemeCommand(output, line);
                continue;
            }

            try {
                QueryResult result = queryEngine.execute(line);
                printResult(output, result);
            } catch (RuntimeException ex) {
                output.println(error(ASSISTANT_PREFIX + "error: " + ex.getMessage()));
                output.println();
            }
        }

        output.println(muted("Session ended."));
    }

    private void printBanner(PrintStream output) {
        output.println("     _ _     _ _       ");
        output.println("    | | |   (_) |_ ___ ");
        output.println(" _  | | |   | | __/ _ \\");
        output.println("| |_| | |___| | ||  __/");
        output.println(" \\___/|_____|_|\\__\\___|");
        output.println(accent("-JLite Database Engine-"));
        output.println();
    }

    private void seedDemoData() {
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

    private QueryEngine createConfiguredEngine() {
        var configuredStorageDir = StorageConfigResolver.resolveStorageDir();
        return configuredStorageDir.map(QueryEngine::new).orElseGet(QueryEngine::new);
    }

    private void printResult(PrintStream output, QueryResult result) {
        output.println(assistant("result"));
        var columns = result.columns();
        var rows = result.rows();
        if (columns.isEmpty()) {
            output.println(muted("  (no columns)"));
            output.println(muted("  rows: " + rows.size()));
            output.println();
            return;
        }

        int[] widths = new int[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            widths[i] = columns.get(i).length();
        }
        for (var row : rows) {
            for (int i = 0; i < row.size() && i < widths.length; i++) {
                widths[i] = Math.max(widths[i], String.valueOf(row.get(i)).length());
            }
        }

        output.println(muted("  " + horizontalRule(widths)));
        output.println("  " + strong(formatRow(columns, widths)));
        output.println(muted("  " + horizontalRule(widths)));
        for (var row : rows) {
            output.println("  " + formatRow(row.stream().map(String::valueOf).toList(), widths));
        }
        output.println(muted("  " + horizontalRule(widths)));
        output.println(muted("  rows: " + rows.size()));
        output.println();
    }

    private void printHelp(PrintStream output) {
        output.println(assistant("help"));
        output.println(muted("  Enter SQL statements and press Enter to execute."));
        output.println(muted("  \\help   Show this help"));
        output.println(muted("  \\theme  Show or set theme"));
        output.println(muted("          Usage: \\theme [auto|mono|ocean|sunset|forest]"));
        output.println(muted("  \\quit   Exit CLI"));
        output.println(muted("  \\q      Exit CLI"));
        output.println();
    }

    private void handleThemeCommand(PrintStream output, String line) {
        var parts = line.trim().split("\\s+");
        if (parts.length == 1) {
            output.println(assistant("theme"));
            output.println(muted("  current: " + theme.value));
            output.println(muted("  available: auto, mono, ocean, sunset, forest"));
            output.println();
            return;
        }

        if (parts.length != 2) {
            output.println(error(ASSISTANT_PREFIX + "error: usage: \\theme [auto|mono|ocean|sunset|forest]"));
            output.println();
            return;
        }

        try {
            theme = Theme.fromValue(parts[1]);
            useColor = shouldUseColor(output);
            output.println(assistant("theme set to " + theme.value));
            output.println();
        } catch (IllegalArgumentException ex) {
            output.println(error(ASSISTANT_PREFIX + "error: " + ex.getMessage()));
            output.println();
        }
    }

    private String horizontalRule(int[] widths) {
        StringBuilder line = new StringBuilder("+");
        for (int width : widths) {
            line.append("-".repeat(width + 2)).append('+');
        }
        return line.toString();
    }

    private String formatRow(List<String> cells, int[] widths) {
        StringBuilder line = new StringBuilder("|");
        for (int i = 0; i < widths.length; i++) {
            String value = i < cells.size() ? cells.get(i) : "";
            line.append(' ').append(value);
            int padding = widths[i] - value.length();
            if (padding > 0) {
                line.append(" ".repeat(padding));
            }
            line.append(" |");
        }
        return line.toString();
    }

    private boolean shouldUseColor(PrintStream output) {
        if (theme == Theme.MONO) {
            return false;
        }

        // If the user explicitly selected a theme, honor it even when System.console()
        // is unavailable (common in IDE integrated terminals).
        if (theme != Theme.AUTO) {
            return output == System.out && System.getenv("NO_COLOR") == null;
        }

        if (output != System.out) {
            return false;
        }
        if (System.console() == null) {
            return false;
        }
        return System.getenv("NO_COLOR") == null;
    }

    private String userPrompt() {
        return color(USER_PROMPT, theme.user);
    }

    private String assistant(String message) {
        return color(ASSISTANT_PREFIX + message, theme.assistant);
    }

    private String muted(String text) {
        return color(text, theme.muted);
    }

    private String strong(String text) {
        return color(text, theme.strong);
    }

    private String accent(String text) {
        return color(text, theme.accent);
    }

    private String error(String text) {
        return color(text, theme.error);
    }

    private String color(String text, String code) {
        if (!useColor) {
            return text;
        }
        return code + text + Ansi.RESET;
    }

    private static final class Ansi {
        private static final String RESET = "\u001B[0m";
        private static final String BOLD = "\u001B[1m";
        private static final String RED = "\u001B[31m";
        private static final String YELLOW = "\u001B[33m";
        private static final String GREEN = "\u001B[32m";
        private static final String BLUE = "\u001B[34m";
        private static final String MAGENTA = "\u001B[35m";
        private static final String CYAN = "\u001B[36m";
        private static final String GRAY = "\u001B[90m";

        private Ansi() {
        }
    }

    public enum Theme {
        AUTO("auto", Ansi.CYAN, Ansi.GREEN, Ansi.GRAY, Ansi.BOLD, Ansi.BLUE, Ansi.RED),
        MONO("mono", "", "", "", "", "", ""),
        OCEAN("ocean", Ansi.CYAN, Ansi.BLUE, Ansi.GRAY, Ansi.BOLD, Ansi.GREEN, Ansi.RED),
        SUNSET("sunset", Ansi.MAGENTA, Ansi.YELLOW, Ansi.GRAY, Ansi.BOLD, Ansi.RED, Ansi.RED),
        FOREST("forest", Ansi.GREEN, Ansi.CYAN, Ansi.GRAY, Ansi.BOLD, Ansi.GREEN, Ansi.RED);

        private final String value;
        private final String user;
        private final String assistant;
        private final String muted;
        private final String strong;
        private final String accent;
        private final String error;

        Theme(String value, String user, String assistant, String muted, String strong, String accent, String error) {
            this.value = value;
            this.user = user;
            this.assistant = assistant;
            this.muted = muted;
            this.strong = strong;
            this.accent = accent;
            this.error = error;
        }

        static Theme fromValue(String value) {
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("theme value cannot be empty");
            }
            var normalized = value.trim().toLowerCase();
            for (var option : values()) {
                if (option.value.equals(normalized)) {
                    return option;
                }
            }
            throw new IllegalArgumentException("unknown theme '" + value + "'. expected: auto, mono, ocean, sunset, forest");
        }
    }

    public static void main(String[] args) {
        try {
            new Repl(resolveTheme(args)).run();
        } catch (IllegalArgumentException ex) {
            System.err.println("ERROR: " + ex.getMessage());
            System.err.println("Usage: Repl [--theme <auto|mono|ocean|sunset|forest>] [--theme=<...>] [--no-color]");
            System.exit(1);
        }
    }

    private static Theme resolveTheme(String[] args) {
        Theme selectedTheme = Theme.AUTO;
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if ("--no-color".equalsIgnoreCase(arg)) {
                selectedTheme = Theme.MONO;
                continue;
            }
            if ("--theme".equalsIgnoreCase(arg)) {
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException("missing value after --theme");
                }
                selectedTheme = Theme.fromValue(args[++i]);
                continue;
            }
            if (arg.startsWith("--theme=")) {
                selectedTheme = Theme.fromValue(arg.substring("--theme=".length()));
            }
        }
        return selectedTheme;
    }
}
