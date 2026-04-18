package jlite.catalogue;
import java.util.List;
/** TODO: add index list, FK constraints, statistics. */
public record TableSchema(String name, List<Column> columns) {}
