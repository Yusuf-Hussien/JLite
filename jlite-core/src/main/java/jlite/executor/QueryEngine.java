package jlite.executor;

import java.nio.file.Path;
import java.util.Map;

import jlite.analyser.Analyser;
import jlite.catalogue.Catalogue;
import jlite.catalogue.TableSchema;
import jlite.lexer.Lexer;
import jlite.parser.Parser;

public class QueryEngine {

    private final Catalogue catalogue;
    private final TableStore tableStore;
    private final Analyser analyser;
    private final QueryExecutor executor;

    public QueryEngine() {
        var catalog = new Catalogue();
        this.catalogue = catalog;
        this.tableStore = new InMemoryTableStore(catalog);
        this.analyser = new Analyser(catalog);
        this.executor = new QueryExecutor(tableStore);
    }

    public QueryEngine(Path storageDir) {
        var catalog = new Catalogue();
        this.catalogue = catalog;
        this.tableStore = new PersistentTableStore(catalog, storageDir);
        this.analyser = new Analyser(catalog);
        this.executor = new QueryExecutor(tableStore);
    }

    public Catalogue catalogue() {
        return catalogue;
    }

    public void createTable(TableSchema schema) {
        tableStore.createTable(schema);
    }

    public void insertRow(String tableName, Map<String, Object> values) {
        tableStore.insertRow(tableName, values);
    }

    public QueryResult execute(String sql) {
        var statement = new Parser(new Lexer(sql).tokenise()).parseStatement();
        analyser.validate(statement);
        return executor.execute(statement);
    }
}