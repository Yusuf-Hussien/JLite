package jlite.parser;
import jlite.ast.Statement;
import jlite.lexer.Token;
import java.util.List;
/**
 * Recursive-descent SQL parser.
 * TODO: implement parseStatement() dispatching to all statement types.
 * TODO: parseExpression(), parsePredicate(), parseLiteral().
 */
public class Parser {
    private final List<Token> tokens;
    private int pos = 0;
    public Parser(List<Token> tokens) { this.tokens = tokens; }
    public Statement parseStatement() {
        throw new UnsupportedOperationException("Parser.parseStatement() not yet implemented");
    }
}
