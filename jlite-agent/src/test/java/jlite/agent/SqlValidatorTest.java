package jlite.agent;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

import jlite.catalogue.Catalogue;
import jlite.catalogue.Column;
import jlite.catalogue.DataType;
import jlite.catalogue.TableSchema;

class SqlValidatorTest {

    @Test
    void validatesGoodSqlAndRejectsBadSql() {
        var catalogue = new Catalogue();
        catalogue.createTable(new TableSchema("users", List.of(
            new Column("id", DataType.INT, false, true),
            new Column("name", DataType.TEXT, true, false)
        )));

        var validator = new SqlValidator(catalogue);

        var ok = validator.validate("SELECT id, name FROM users");
        assertEquals(true, ok.ok());

        var bad = validator.validate("SELECT missing FROM users");
        assertEquals(false, bad.ok());
    }
}
