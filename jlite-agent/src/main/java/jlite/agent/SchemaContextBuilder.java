package jlite.agent;

import jlite.catalogue.Catalogue;

/**
 * Builds a compact schema description to inject into the LLM system prompt.
 *
 * Format (one line per table):
 *   users(id INT PK, name TEXT, email TEXT, created_at TIMESTAMP)
 *   orders(id INT PK, user_id INT FK->users.id, total FLOAT, status TEXT)
 *
 * TODO: include index hints so the LLM can write index-friendly predicates.
 * TODO: include row-count estimates to guide the LLM's query complexity.
 */
public class SchemaContextBuilder {

    private final Catalogue catalogue;

    public SchemaContextBuilder(Catalogue catalogue) {
        this.catalogue = catalogue;
    }

    public String build() {
        // TODO: implement
        throw new UnsupportedOperationException("SchemaContextBuilder.build() not yet implemented");
    }
}
