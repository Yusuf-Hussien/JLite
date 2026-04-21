package jlite.lexer;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

class LexerTest {

    @Test
    void tokeniseSelectWhereQuery() {
        var lexer = new Lexer("SELECT name, age FROM users WHERE age >= 18 AND active = true;");

        List<Token> tokens = lexer.tokenise();

        assertEquals(TokenType.SELECT, tokens.get(0).type());
        assertEquals(TokenType.IDENTIFIER, tokens.get(1).type());
        assertEquals("name", tokens.get(1).value());
        assertEquals(TokenType.COMMA, tokens.get(2).type());
        assertEquals(TokenType.IDENTIFIER, tokens.get(3).type());
        assertEquals(TokenType.FROM, tokens.get(4).type());
        assertEquals(TokenType.IDENTIFIER, tokens.get(5).type());
        assertEquals("users", tokens.get(5).value());
        assertEquals(TokenType.WHERE, tokens.get(6).type());
        assertEquals(TokenType.IDENTIFIER, tokens.get(7).type());
        assertEquals(TokenType.GTE, tokens.get(8).type());
        assertEquals(TokenType.INTEGER_LITERAL, tokens.get(9).type());
        assertEquals("18", tokens.get(9).value());
        assertEquals(TokenType.AND, tokens.get(10).type());
        assertEquals(TokenType.IDENTIFIER, tokens.get(11).type());
        assertEquals(TokenType.EQ, tokens.get(12).type());
        assertEquals(TokenType.BOOLEAN_LITERAL, tokens.get(13).type());
        assertEquals("true", tokens.get(13).value());
        assertEquals(TokenType.SEMICOLON, tokens.get(14).type());
        assertEquals(TokenType.EOF, tokens.get(15).type());
    }

    @Test
    void tokeniseHandlesCommentsAndEscapedStrings() {
        var lexer = new Lexer("SELECT 'it''s' /* ignored */ FROM t -- comment\nWHERE id = 1");

        List<Token> tokens = lexer.tokenise();

        assertEquals(TokenType.SELECT, tokens.get(0).type());
        assertEquals(TokenType.STRING_LITERAL, tokens.get(1).type());
        assertEquals("it's", tokens.get(1).value());
        assertEquals(TokenType.FROM, tokens.get(2).type());
        assertEquals(TokenType.IDENTIFIER, tokens.get(3).type());
        assertEquals("t", tokens.get(3).value());
        assertEquals(TokenType.WHERE, tokens.get(4).type());
        assertEquals(TokenType.IDENTIFIER, tokens.get(5).type());
        assertEquals(TokenType.EQ, tokens.get(6).type());
        assertEquals(TokenType.INTEGER_LITERAL, tokens.get(7).type());
        assertEquals(TokenType.EOF, tokens.get(8).type());
    }

    @Test
    void tokeniseThrowsForUnterminatedString() {
        var lexer = new Lexer("SELECT 'oops");
        var ex = assertThrows(LexerException.class, () -> {
            lexer.tokenise();
        });
        assertEquals(true, ex.getMessage().contains("Unterminated string literal"));
    }
}
