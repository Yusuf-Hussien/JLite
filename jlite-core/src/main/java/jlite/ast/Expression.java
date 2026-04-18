package jlite.ast;
/** TODO: add Literal, ColumnRef, BinaryOp, UnaryOp, FunctionCall, CaseExpr, CastExpr, SubqueryExpr. */
public sealed interface Expression extends Node permits Literal, ColumnRef, BinaryOp {}
