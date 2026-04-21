package jlite.cli;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.StringReader;

import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

class ReplTest {

    @Test
    void executesQueryAndPrintsResults() {
        var repl = new Repl();
        var output = new ByteArrayOutputStream();

        repl.run(new StringReader("SELECT name, age FROM users WHERE active = true AND age >= 18\n\\quit\n"), new PrintStream(output));

        var text = output.toString();
        assertTrue(text.contains("JLite Database Engine"));
        assertTrue(text.contains("you> "));
        assertTrue(text.contains("Session started. Type SQL and press Enter."));
        assertTrue(text.contains("Dataset: users (demo table loaded)"));
        assertTrue(text.contains("jlite> result"));
        assertTrue(text.contains("| name  | age |"));
        assertTrue(text.contains("| Alice | 30  |"));
        assertTrue(text.contains("| Cara  | 41  |"));
        assertTrue(text.contains("rows: 2"));
        assertTrue(text.contains("Session ended."));
    }

    @Test
    void executesDdlAndDmlThroughCli() {
        var repl = new Repl();
        var output = new ByteArrayOutputStream();

        var commands = String.join("\n",
            "CREATE TABLE projects (id INT, name TEXT)",
            "INSERT INTO projects VALUES (1, 'Core'), (2, 'Server')",
            "ALTER TABLE projects ADD COLUMN active BOOLEAN",
            "UPDATE projects SET active = true WHERE id = 1",
            "DELETE FROM projects WHERE id = 2",
            "SELECT id, name, active FROM projects",
            "DROP TABLE projects",
            "\\quit",
            ""
        );

        repl.run(new StringReader(commands), new PrintStream(output));

        var text = output.toString();
        assertTrue(text.contains("affected_rows"));
        assertTrue(text.contains("2"));
        assertTrue(text.contains("1"));
        assertTrue(text.contains("| id | name | active |"));
        assertTrue(text.contains("| 1  | Core | true   |"));
        assertTrue(!text.contains("jlite> error:"));
        assertTrue(text.contains("Session ended."));
    }

    @Test
    void switchesThemeAtRuntime() {
        var repl = new Repl();
        var output = new ByteArrayOutputStream();

        repl.run(new StringReader("\\theme sunset\n\\theme\n\\quit\n"), new PrintStream(output));

        var text = output.toString();
        assertTrue(text.contains("jlite> theme set to sunset"));
        assertTrue(text.contains("current: sunset"));
    }
}
