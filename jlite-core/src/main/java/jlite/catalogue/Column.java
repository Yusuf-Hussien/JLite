package jlite.catalogue;
public record Column(String name, DataType type, boolean nullable, boolean primaryKey) {}
