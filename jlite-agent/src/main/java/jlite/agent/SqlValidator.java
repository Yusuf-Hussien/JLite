package jlite.agent;

import jlite.analyser.Analyser;
import jlite.catalogue.Catalogue;
import jlite.lexer.Lexer;
import jlite.lexer.LexerException;
import jlite.parser.ParseException;
import jlite.parser.Parser;

/**
 * Validates LLM-generated SQL before execution.
 *
 * Runs the SQL through JLite's own parser and semantic analyser.
 * Returns a structured result so the agent can feed errors back to the LLM.
 *
 * TODO: implement syntax check via Parser.
 * TODO: implement semantic check (tables + columns exist in Catalogue).
 * TODO: return structured ValidationResult(ok, errorMessage).
 */
public class SqlValidator {

    private final Catalogue catalogue;

    public SqlValidator(Catalogue catalogue) {
        this.catalogue = catalogue;
    }

    public ValidationResult validate(String sql) {
        if (sql == null || sql.isBlank()) {
            return new ValidationResult(false, "SQL is empty");
        }

        try {
            var statement = new Parser(new Lexer(sql).tokenise()).parseStatement();
            new Analyser(catalogue).validate(statement);
            return new ValidationResult(true, null);
        } catch (LexerException | ParseException ex) {
            return new ValidationResult(false, "Syntax error: " + ex.getMessage());
        } catch (RuntimeException ex) {
            return new ValidationResult(false, "Semantic error: " + ex.getMessage());
        }
    }

    public record ValidationResult(boolean ok, String errorMessage) {}
}
