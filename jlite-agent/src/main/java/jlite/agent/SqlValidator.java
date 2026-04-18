package jlite.agent;

import jlite.catalogue.Catalogue;
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
        // TODO: implement
        throw new UnsupportedOperationException("SqlValidator.validate() not yet implemented");
    }

    public record ValidationResult(boolean ok, String errorMessage) {}
}
