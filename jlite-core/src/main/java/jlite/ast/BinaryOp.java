package jlite.ast;
public record BinaryOp(Expression left, Op op, Expression right) implements Expression {
    public enum Op { EQ, NEQ, LT, GT, LTE, GTE, AND, OR, PLUS, MINUS, STAR, SLASH }
}
