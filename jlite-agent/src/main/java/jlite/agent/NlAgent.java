package jlite.agent;

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

    public NlAgent(SchemaContextBuilder schemaContextBuilder, SqlValidator sqlValidator) {
        this.schemaContextBuilder = schemaContextBuilder;
        this.sqlValidator = sqlValidator;
    }

    public String query(String naturalLanguageQuestion) {
        // TODO: implement full pipeline
        throw new UnsupportedOperationException("NlAgent.query() not yet implemented");
    }
}
