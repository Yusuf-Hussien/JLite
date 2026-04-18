package jlite.cli;

import java.util.Scanner;

/**
 * Interactive REPL shell for JLite.
 *
 * TODO: JLine3 integration for history, arrow-key editing, syntax highlighting.
 * TODO: meta-commands: \tables, \describe <table>, \explain <sql>, \timing, \connect, \import.
 * TODO: multi-line statement accumulation (continue until semicolon).
 * TODO: connect to remote TCP server via \connect host:port.
 */
public class Repl {

    public void run() {
        System.out.println("JLite — type SQL or \\quit to exit");
        var scanner = new Scanner(System.in);
        while (scanner.hasNextLine()) {
            var line = scanner.nextLine().trim();
            if (line.equalsIgnoreCase("\\quit") || line.equalsIgnoreCase("\\q")) break;
            if (line.isEmpty()) continue;
            // TODO: route to query engine or meta-command handler
            System.out.println("TODO: execute: " + line);
        }
    }

    public static void main(String[] args) {
        new Repl().run();
    }
}
