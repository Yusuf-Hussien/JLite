package jlite.ast;
/** TODO: add CreateTableStatement, DropTableStatement, AlterTable*, CreateIndex*, Transaction*. */
public sealed interface Statement extends Node
    permits SelectStatement, InsertStatement, UpdateStatement, DeleteStatement,
            CreateTableStatement, DropTableStatement, AlterTableStatement {}
