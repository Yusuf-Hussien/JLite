package jlite.lexer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * JLite SQL Lexer.
 * TODO: implement tokenise() for all TokenType values.
 * TODO: track line/column for error messages.
 * TODO: strip -- and block comments.
 */
public class Lexer {
    private static final Map<String, TokenType> KEYWORDS = buildKeywords();

    private final String input;
    private int pos;
    private int line;
    private int column;

    public Lexer(String input) { this.input = input; }

    public List<Token> tokenise() {
        this.pos = 0;
        this.line = 1;
        this.column = 1;

        var tokens = new ArrayList<Token>();
        while (!isAtEnd()) {
            var c = peek();

            if (Character.isWhitespace(c)) {
                consumeWhitespace();
                continue;
            }

            if (Character.isLetter(c) || c == '_') {
                tokens.add(readWord());
                continue;
            }

            if (Character.isDigit(c)) {
                tokens.add(readNumber());
                continue;
            }

            var startLine = line;
            var startColumn = column;
            switch (c) {
                case '\'' -> tokens.add(readStringLiteral());
                case '=' -> {
                    advance();
                    tokens.add(new Token(TokenType.EQ, "=", startLine, startColumn));
                }
                case '!' -> {
                    advance();
                    if (match('=')) {
                        tokens.add(new Token(TokenType.NEQ, "!=", startLine, startColumn));
                    } else {
                        throw error("Unexpected character '!'");
                    }
                }
                case '<' -> {
                    advance();
                    if (match('=')) {
                        tokens.add(new Token(TokenType.LTE, "<=", startLine, startColumn));
                    } else if (match('>')) {
                        tokens.add(new Token(TokenType.NEQ, "<>", startLine, startColumn));
                    } else {
                        tokens.add(new Token(TokenType.LT, "<", startLine, startColumn));
                    }
                }
                case '>' -> {
                    advance();
                    if (match('=')) {
                        tokens.add(new Token(TokenType.GTE, ">=", startLine, startColumn));
                    } else {
                        tokens.add(new Token(TokenType.GT, ">", startLine, startColumn));
                    }
                }
                case '+' -> {
                    advance();
                    tokens.add(new Token(TokenType.PLUS, "+", startLine, startColumn));
                }
                case '-' -> {
                    if (peekNext() == '-') {
                        consumeLineComment();
                    } else {
                        advance();
                        tokens.add(new Token(TokenType.MINUS, "-", startLine, startColumn));
                    }
                }
                case '*' -> {
                    advance();
                    tokens.add(new Token(TokenType.STAR, "*", startLine, startColumn));
                }
                case '/' -> {
                    if (peekNext() == '*') {
                        consumeBlockComment();
                    } else {
                        advance();
                        tokens.add(new Token(TokenType.SLASH, "/", startLine, startColumn));
                    }
                }
                case '%' -> {
                    advance();
                    tokens.add(new Token(TokenType.PERCENT, "%", startLine, startColumn));
                }
                case '(' -> {
                    advance();
                    tokens.add(new Token(TokenType.LPAREN, "(", startLine, startColumn));
                }
                case ')' -> {
                    advance();
                    tokens.add(new Token(TokenType.RPAREN, ")", startLine, startColumn));
                }
                case ',' -> {
                    advance();
                    tokens.add(new Token(TokenType.COMMA, ",", startLine, startColumn));
                }
                case ';' -> {
                    advance();
                    tokens.add(new Token(TokenType.SEMICOLON, ";", startLine, startColumn));
                }
                case '.' -> {
                    advance();
                    tokens.add(new Token(TokenType.DOT, ".", startLine, startColumn));
                }
                default -> throw error("Unexpected character '" + c + "'");
            }
        }

        tokens.add(new Token(TokenType.EOF, "", line, column));
        return tokens;
    }

    private static Map<String, TokenType> buildKeywords() {
        var keywords = new HashMap<String, TokenType>();
        keywords.put("SELECT", TokenType.SELECT);
        keywords.put("FROM", TokenType.FROM);
        keywords.put("WHERE", TokenType.WHERE);
        keywords.put("INSERT", TokenType.INSERT);
        keywords.put("INTO", TokenType.INTO);
        keywords.put("VALUES", TokenType.VALUES);
        keywords.put("UPDATE", TokenType.UPDATE);
        keywords.put("SET", TokenType.SET);
        keywords.put("DELETE", TokenType.DELETE);
        keywords.put("CREATE", TokenType.CREATE);
        keywords.put("DROP", TokenType.DROP);
        keywords.put("ALTER", TokenType.ALTER);
        keywords.put("TABLE", TokenType.TABLE);
        keywords.put("INDEX", TokenType.INDEX);
        keywords.put("ON", TokenType.ON);
        keywords.put("JOIN", TokenType.JOIN);
        keywords.put("INNER", TokenType.INNER);
        keywords.put("LEFT", TokenType.LEFT);
        keywords.put("RIGHT", TokenType.RIGHT);
        keywords.put("OUTER", TokenType.OUTER);
        keywords.put("CROSS", TokenType.CROSS);
        keywords.put("GROUP", TokenType.GROUP);
        keywords.put("BY", TokenType.BY);
        keywords.put("HAVING", TokenType.HAVING);
        keywords.put("ORDER", TokenType.ORDER);
        keywords.put("ASC", TokenType.ASC);
        keywords.put("DESC", TokenType.DESC);
        keywords.put("LIMIT", TokenType.LIMIT);
        keywords.put("OFFSET", TokenType.OFFSET);
        keywords.put("DISTINCT", TokenType.DISTINCT);
        keywords.put("AS", TokenType.AS);
        keywords.put("AND", TokenType.AND);
        keywords.put("OR", TokenType.OR);
        keywords.put("NOT", TokenType.NOT);
        keywords.put("IN", TokenType.IN);
        keywords.put("BETWEEN", TokenType.BETWEEN);
        keywords.put("LIKE", TokenType.LIKE);
        keywords.put("IS", TokenType.IS);
        keywords.put("BEGIN", TokenType.BEGIN);
        keywords.put("COMMIT", TokenType.COMMIT);
        keywords.put("ROLLBACK", TokenType.ROLLBACK);
        keywords.put("SAVEPOINT", TokenType.SAVEPOINT);
        keywords.put("PRIMARY", TokenType.PRIMARY);
        keywords.put("KEY", TokenType.KEY);
        keywords.put("FOREIGN", TokenType.FOREIGN);
        keywords.put("REFERENCES", TokenType.REFERENCES);
        keywords.put("UNIQUE", TokenType.UNIQUE);
        keywords.put("DEFAULT", TokenType.DEFAULT);
        keywords.put("CHECK", TokenType.CHECK);
        keywords.put("ADD", TokenType.ADD);
        keywords.put("COLUMN", TokenType.COLUMN);
        keywords.put("RENAME", TokenType.RENAME);
        keywords.put("INT", TokenType.INT);
        keywords.put("BIGINT", TokenType.BIGINT);
        keywords.put("FLOAT", TokenType.FLOAT);
        keywords.put("DOUBLE", TokenType.DOUBLE);
        keywords.put("TEXT", TokenType.TEXT);
        keywords.put("VARCHAR", TokenType.VARCHAR);
        keywords.put("BOOLEAN", TokenType.BOOLEAN);
        keywords.put("DATE", TokenType.DATE);
        keywords.put("TIMESTAMP", TokenType.TIMESTAMP);
        return keywords;
    }

    private Token readWord() {
        var start = pos;
        var startLine = line;
        var startColumn = column;

        while (!isAtEnd() && (Character.isLetterOrDigit(peek()) || peek() == '_')) {
            advance();
        }

        var text = input.substring(start, pos);
        var upper = text.toUpperCase();
        if ("TRUE".equals(upper) || "FALSE".equals(upper)) {
            return new Token(TokenType.BOOLEAN_LITERAL, upper.toLowerCase(), startLine, startColumn);
        }
        if ("NULL".equals(upper)) {
            return new Token(TokenType.NULL_LITERAL, "null", startLine, startColumn);
        }

        var keywordType = KEYWORDS.get(upper);
        if (keywordType != null) {
            return new Token(keywordType, upper, startLine, startColumn);
        }
        return new Token(TokenType.IDENTIFIER, text, startLine, startColumn);
    }

    private Token readNumber() {
        var start = pos;
        var startLine = line;
        var startColumn = column;

        while (!isAtEnd() && Character.isDigit(peek())) {
            advance();
        }

        var isFloat = false;
        if (!isAtEnd() && peek() == '.' && Character.isDigit(peekNext())) {
            isFloat = true;
            advance();
            while (!isAtEnd() && Character.isDigit(peek())) {
                advance();
            }
        }

        var text = input.substring(start, pos);
        return new Token(isFloat ? TokenType.FLOAT_LITERAL : TokenType.INTEGER_LITERAL, text, startLine, startColumn);
    }

    private Token readStringLiteral() {
        var startLine = line;
        var startColumn = column;
        advance();

        var sb = new StringBuilder();
        while (!isAtEnd()) {
            var c = advance();
            if (c == '\'') {
                if (!isAtEnd() && peek() == '\'') {
                    advance();
                    sb.append('\'');
                    continue;
                }
                return new Token(TokenType.STRING_LITERAL, sb.toString(), startLine, startColumn);
            }
            sb.append(c);
        }

        throw new LexerException("Unterminated string literal at " + startLine + ":" + startColumn);
    }

    private void consumeWhitespace() {
        while (!isAtEnd() && Character.isWhitespace(peek())) {
            advance();
        }
    }

    private void consumeLineComment() {
        while (!isAtEnd() && peek() != '\n') {
            advance();
        }
    }

    private void consumeBlockComment() {
        advance();
        advance();
        while (!isAtEnd()) {
            if (peek() == '*' && peekNext() == '/') {
                advance();
                advance();
                return;
            }
            advance();
        }
        throw error("Unterminated block comment");
    }

    private char advance() {
        var c = input.charAt(pos++);
        if (c == '\n') {
            line++;
            column = 1;
        } else {
            column++;
        }
        return c;
    }

    private boolean match(char expected) {
        if (isAtEnd() || input.charAt(pos) != expected) {
            return false;
        }
        advance();
        return true;
    }

    private char peek() {
        return input.charAt(pos);
    }

    private char peekNext() {
        if (pos + 1 >= input.length()) {
            return '\0';
        }
        return input.charAt(pos + 1);
    }

    private boolean isAtEnd() {
        return pos >= input.length();
    }

    private LexerException error(String message) {
        return new LexerException(message + " at " + line + ":" + column);
    }
}
