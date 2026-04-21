package jlite.agent;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import jlite.catalogue.Catalogue;
import jlite.catalogue.Column;
import jlite.catalogue.DataType;
import jlite.catalogue.TableSchema;

class SchemaContextBuilderTest {

    @Test
    void buildsSchemaLinesForAllTables() {
        var catalogue = new Catalogue();
        catalogue.createTable(new TableSchema("users", List.of(
            new Column("id", DataType.INT, false, true),
            new Column("name", DataType.TEXT, true, false)
        )));
        catalogue.createTable(new TableSchema("projects", List.of(
            new Column("id", DataType.INT, false, true),
            new Column("title", DataType.TEXT, false, false)
        )));

        var builder = new SchemaContextBuilder(catalogue);
        var context = builder.build();

        assertTrue(context.contains("users(id INT PK NOT_NULL, name TEXT)"));
        assertTrue(context.contains("projects(id INT PK NOT_NULL, title TEXT NOT_NULL)"));
    }
}
