package jlite.lexer;
import java.util.List;
/**
 * JLite SQL Lexer.
 * TODO: implement tokenise() for all TokenType values.
 * TODO: track line/column for error messages.
 * TODO: strip -- and block comments.
 */
public class Lexer {
    private final String input;
    public Lexer(String input) { this.input = input; }
    public List<Token> tokenise() {
        throw new UnsupportedOperationException("Lexer.tokenise() not yet implemented");
    }
}
