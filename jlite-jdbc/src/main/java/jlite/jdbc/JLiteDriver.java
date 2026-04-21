package jlite.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import jlite.server.protocol.ServerRequest;
import jlite.server.protocol.ServerResponse;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * JLite JDBC 4.2 Driver.
 *
 * URL formats:
 *   jdbc:jlite://host:port/dbname  — remote via TCP server
 *   jdbc:jlite:file:/path/to/db   — embedded, direct file access
 *
 * TODO: implement connect() routing to TcpConnection or EmbeddedConnection.
 * TODO: implement PreparedStatement with ? binding.
 * TODO: implement ResultSetMetaData with column names and types.
 * TODO: implement batch inserts (addBatch / executeBatch).
 * TODO: implement Connection.setAutoCommit(false) for manual TX control.
 */
public class JLiteDriver implements Driver {

    public static final String URL_PREFIX_TCP      = "jdbc:jlite://";
    public static final String URL_PREFIX_EMBEDDED = "jdbc:jlite:file:";

    static {
        try {
            DriverManager.registerDriver(new JLiteDriver());
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) return null;
        if (url.startsWith(URL_PREFIX_EMBEDDED)) {
            throw new SQLFeatureNotSupportedException("Embedded URL mode is not implemented yet");
        }

        var hostPortAndDb = url.substring(URL_PREFIX_TCP.length());
        var slash = hostPortAndDb.indexOf('/');
        var hostPort = slash >= 0 ? hostPortAndDb.substring(0, slash) : hostPortAndDb;
        var parts = hostPort.split(":", 2);
        if (parts.length != 2 || parts[0].isBlank()) {
            throw new SQLException("Invalid JDBC URL. Expected jdbc:jlite://host:port/db");
        }

        var host = parts[0];
        final int port;
        try {
            port = Integer.parseInt(parts[1]);
        } catch (NumberFormatException ex) {
            throw new SQLException("Invalid port in JDBC URL: " + parts[1], ex);
        }

        return newConnectionProxy(new TcpClient(host, port));
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null &&
               (url.startsWith(URL_PREFIX_TCP) || url.startsWith(URL_PREFIX_EMBEDDED));
    }

    @Override public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) { return new DriverPropertyInfo[0]; }
    @Override public int getMajorVersion() { return 0; }
    @Override public int getMinorVersion() { return 1; }
    @Override public boolean jdbcCompliant() { return false; }
    @Override public Logger getParentLogger() { return Logger.getLogger("jlite.jdbc"); }

    private Connection newConnectionProxy(TcpClient client) {
        class ConnectionHandler implements java.lang.reflect.InvocationHandler {
            private boolean closed;

            @Override
            public Object invoke(Object proxy, java.lang.reflect.Method method, Object[] args) throws Throwable {
                var name = method.getName();
                return switch (name) {
                    case "createStatement" -> {
                        ensureOpen();
                        yield newStatementProxy(client, this::ensureOpen);
                    }
                    case "prepareStatement" -> {
                        ensureOpen();
                        var sql = (String) args[0];
                        yield newPreparedStatementProxy(client, this::ensureOpen, sql);
                    }
                    case "close" -> {
                        if (!closed) {
                            closed = true;
                            client.close();
                        }
                        yield null;
                    }
                    case "isClosed" -> closed;
                    case "setAutoCommit" -> null;
                    case "getAutoCommit" -> true;
                    case "commit", "rollback" -> null;
                    case "unwrap" -> {
                        var cls = (Class<?>) args[0];
                        if (cls.isInstance(proxy)) {
                            yield proxy;
                        }
                        throw new SQLException("Not a wrapper for " + cls.getName());
                    }
                    case "isWrapperFor" -> ((Class<?>) args[0]).isInstance(proxy);
                    case "toString" -> "JLiteConnection[closed=" + closed + "]";
                    default -> throw new SQLFeatureNotSupportedException("Connection method not supported: " + name);
                };
            }

            private void ensureOpen() throws SQLException {
                if (closed) {
                    throw new SQLException("Connection is closed");
                }
            }
        }

        return (Connection) Proxy.newProxyInstance(
            Connection.class.getClassLoader(),
            new Class<?>[]{Connection.class},
            new ConnectionHandler()
        );
    }

    private Statement newStatementProxy(TcpClient client, OpenCheck openCheck) {
        class StatementHandler implements java.lang.reflect.InvocationHandler {
            private boolean closed;

            @Override
            public Object invoke(Object proxy, java.lang.reflect.Method method, Object[] args) throws Throwable {
                var name = method.getName();
                return switch (name) {
                    case "executeQuery" -> {
                        ensureUsable();
                        var sql = (String) args[0];
                        var response = client.query(sql);
                        if (!isResultResponse(response) || isUpdateResponse(response)) {
                            throw new SQLException(response.message() == null ? "Unexpected response type: " + response.type() : response.message());
                        }
                        yield newResultSetProxy(response.columns(), response.rows());
                    }
                    case "execute" -> {
                        ensureUsable();
                        var sql = (String) args[0];
                        var response = client.query(sql);
                        if ("ERROR".equalsIgnoreCase(response.type())) {
                            throw new SQLException(response.message());
                        }
                        yield isResultResponse(response) && !isUpdateResponse(response);
                    }
                    case "executeUpdate" -> {
                        ensureUsable();
                        var sql = (String) args[0];
                        var response = client.query(sql);
                        if ("ERROR".equalsIgnoreCase(response.type())) {
                            throw new SQLException(response.message());
                        }
                        if (isUpdateResponse(response)) {
                            yield extractUpdateCount(response);
                        }
                        if (isResultResponse(response)) {
                            throw new SQLException("executeUpdate cannot be used for SELECT statements");
                        }
                        yield 0;
                    }
                    case "close" -> {
                        closed = true;
                        yield null;
                    }
                    case "isClosed" -> closed;
                    case "getUpdateCount" -> -1;
                    case "unwrap" -> {
                        var cls = (Class<?>) args[0];
                        if (cls.isInstance(proxy)) {
                            yield proxy;
                        }
                        throw new SQLException("Not a wrapper for " + cls.getName());
                    }
                    case "isWrapperFor" -> ((Class<?>) args[0]).isInstance(proxy);
                    case "toString" -> "JLiteStatement[closed=" + closed + "]";
                    default -> throw new SQLFeatureNotSupportedException("Statement method not supported: " + name);
                };
            }

            private void ensureUsable() throws SQLException {
                openCheck.ensureOpen();
                if (closed) {
                    throw new SQLException("Statement is closed");
                }
            }
        }

        return (Statement) Proxy.newProxyInstance(
            Statement.class.getClassLoader(),
            new Class<?>[]{Statement.class},
            new StatementHandler()
        );
    }

    private PreparedStatement newPreparedStatementProxy(TcpClient client, OpenCheck openCheck, String sqlTemplate) {
        class PreparedStatementHandler implements java.lang.reflect.InvocationHandler {
            private boolean closed;
            private final Map<Integer, Object> params = new HashMap<>();

            @Override
            public Object invoke(Object proxy, java.lang.reflect.Method method, Object[] args) throws Throwable {
                var name = method.getName();
                return switch (name) {
                    case "setString", "setInt", "setLong", "setBoolean", "setObject",
                         "setDouble", "setFloat", "setShort", "setByte", "setDate", "setTimestamp" -> {
                        ensureUsable();
                        params.put((Integer) args[0], args[1]);
                        yield null;
                    }
                    case "setNull" -> {
                        ensureUsable();
                        params.put((Integer) args[0], null);
                        yield null;
                    }
                    case "clearParameters" -> {
                        ensureUsable();
                        params.clear();
                        yield null;
                    }
                    case "executeQuery" -> {
                        ensureUsable();
                        var sql = renderSql();
                        var response = client.query(sql);
                        if (!isResultResponse(response) || isUpdateResponse(response)) {
                            throw new SQLException(response.message() == null ? "Unexpected response type: " + response.type() : response.message());
                        }
                        yield newResultSetProxy(response.columns(), response.rows());
                    }
                    case "execute" -> {
                        ensureUsable();
                        var response = client.query(renderSql());
                        if ("ERROR".equalsIgnoreCase(response.type())) {
                            throw new SQLException(response.message());
                        }
                        yield isResultResponse(response) && !isUpdateResponse(response);
                    }
                    case "executeUpdate" -> {
                        ensureUsable();
                        var response = client.query(renderSql());
                        if ("ERROR".equalsIgnoreCase(response.type())) {
                            throw new SQLException(response.message());
                        }
                        if (isUpdateResponse(response)) {
                            yield extractUpdateCount(response);
                        }
                        if (isResultResponse(response)) {
                            throw new SQLException("executeUpdate cannot be used for SELECT statements");
                        }
                        yield 0;
                    }
                    case "close" -> {
                        closed = true;
                        yield null;
                    }
                    case "isClosed" -> closed;
                    case "getUpdateCount" -> -1;
                    case "unwrap" -> {
                        var cls = (Class<?>) args[0];
                        if (cls.isInstance(proxy)) {
                            yield proxy;
                        }
                        throw new SQLException("Not a wrapper for " + cls.getName());
                    }
                    case "isWrapperFor" -> ((Class<?>) args[0]).isInstance(proxy);
                    case "toString" -> "JLitePreparedStatement[closed=" + closed + "]";
                    default -> throw new SQLFeatureNotSupportedException("PreparedStatement method not supported: " + name);
                };
            }

            private void ensureUsable() throws SQLException {
                openCheck.ensureOpen();
                if (closed) {
                    throw new SQLException("PreparedStatement is closed");
                }
            }

            private String renderSql() throws SQLException {
                var out = new StringBuilder();
                var inString = false;
                var paramIndex = 1;

                for (int i = 0; i < sqlTemplate.length(); i++) {
                    var ch = sqlTemplate.charAt(i);
                    if (ch == '\'') {
                        out.append(ch);
                        if (inString && i + 1 < sqlTemplate.length() && sqlTemplate.charAt(i + 1) == '\'') {
                            out.append(sqlTemplate.charAt(i + 1));
                            i++;
                        } else {
                            inString = !inString;
                        }
                        continue;
                    }

                    if (ch == '?' && !inString) {
                        if (!params.containsKey(paramIndex)) {
                            throw new SQLException("Missing value for parameter index " + paramIndex);
                        }
                        out.append(toSqlLiteral(params.get(paramIndex)));
                        paramIndex++;
                    } else {
                        out.append(ch);
                    }
                }

                if (params.size() >= paramIndex) {
                    for (var key : params.keySet()) {
                        if (key >= paramIndex) {
                            throw new SQLException("Parameter index out of range: " + key);
                        }
                    }
                }
                return out.toString();
            }

            private String toSqlLiteral(Object value) {
                if (value == null) {
                    return "NULL";
                }
                if (value instanceof String stringValue) {
                    return "'" + stringValue.replace("'", "''") + "'";
                }
                if (value instanceof Boolean boolValue) {
                    return boolValue ? "true" : "false";
                }
                if (value instanceof Number) {
                    return value.toString();
                }
                if (value instanceof java.sql.Timestamp timestamp) {
                    return "'" + timestamp.toString().replace("'", "''") + "'";
                }
                if (value instanceof java.sql.Date date) {
                    return "'" + date.toString().replace("'", "''") + "'";
                }
                return "'" + value.toString().replace("'", "''") + "'";
            }
        }

        return (PreparedStatement) Proxy.newProxyInstance(
            PreparedStatement.class.getClassLoader(),
            new Class<?>[]{PreparedStatement.class},
            new PreparedStatementHandler()
        );
    }

    private ResultSet newResultSetProxy(List<String> columns, List<List<Object>> rows) {
        var indexByName = new HashMap<String, Integer>();
        for (int i = 0; i < columns.size(); i++) {
            indexByName.put(columns.get(i).toLowerCase(), i + 1);
        }

        class ResultSetHandler implements java.lang.reflect.InvocationHandler {
            private int cursor = -1;
            private boolean closed;
            private Object lastValue;

            @Override
            public Object invoke(Object proxy, java.lang.reflect.Method method, Object[] args) throws Throwable {
                var name = method.getName();
                return switch (name) {
                    case "next" -> {
                        ensureOpen();
                        if (cursor + 1 < rows.size()) {
                            cursor++;
                            yield true;
                        }
                        yield false;
                    }
                    case "getObject" -> {
                        ensurePositioned();
                        var value = getByArg(args[0], rows.get(cursor), indexByName);
                        lastValue = value;
                        yield value;
                    }
                    case "getString" -> {
                        ensurePositioned();
                        var value = getByArg(args[0], rows.get(cursor), indexByName);
                        lastValue = value;
                        yield value == null ? null : value.toString();
                    }
                    case "getInt" -> {
                        ensurePositioned();
                        var value = getByArg(args[0], rows.get(cursor), indexByName);
                        lastValue = value;
                        yield value == null ? 0 : ((Number) value).intValue();
                    }
                    case "getLong" -> {
                        ensurePositioned();
                        var value = getByArg(args[0], rows.get(cursor), indexByName);
                        lastValue = value;
                        yield value == null ? 0L : ((Number) value).longValue();
                    }
                    case "getBoolean" -> {
                        ensurePositioned();
                        var value = getByArg(args[0], rows.get(cursor), indexByName);
                        lastValue = value;
                        yield value != null && (Boolean) value;
                    }
                    case "wasNull" -> lastValue == null;
                    case "findColumn" -> {
                        var label = ((String) args[0]).toLowerCase();
                        var found = indexByName.get(label);
                        if (found == null) {
                            throw new SQLException("Unknown column: " + args[0]);
                        }
                        yield found;
                    }
                    case "getMetaData" -> newResultSetMetaDataProxy(columns);
                    case "close" -> {
                        closed = true;
                        yield null;
                    }
                    case "isClosed" -> closed;
                    case "unwrap" -> {
                        var cls = (Class<?>) args[0];
                        if (cls.isInstance(proxy)) {
                            yield proxy;
                        }
                        throw new SQLException("Not a wrapper for " + cls.getName());
                    }
                    case "isWrapperFor" -> ((Class<?>) args[0]).isInstance(proxy);
                    default -> throw new SQLFeatureNotSupportedException("ResultSet method not supported: " + name);
                };
            }

            private void ensureOpen() throws SQLException {
                if (closed) {
                    throw new SQLException("ResultSet is closed");
                }
            }

            private void ensurePositioned() throws SQLException {
                ensureOpen();
                if (cursor < 0 || cursor >= rows.size()) {
                    throw new SQLException("Cursor is not positioned on a row");
                }
            }
        }

        return (ResultSet) Proxy.newProxyInstance(
            ResultSet.class.getClassLoader(),
            new Class<?>[]{ResultSet.class},
            new ResultSetHandler()
        );
    }

    private ResultSetMetaData newResultSetMetaDataProxy(List<String> columns) {
        class MetaDataHandler implements java.lang.reflect.InvocationHandler {
            @Override
            public Object invoke(Object proxy, java.lang.reflect.Method method, Object[] args) throws Throwable {
                var name = method.getName();
                return switch (name) {
                    case "getColumnCount" -> columns.size();
                    case "getColumnLabel", "getColumnName" -> columns.get(((Integer) args[0]) - 1);
                    case "getColumnType" -> Types.VARCHAR;
                    case "getColumnTypeName" -> "VARCHAR";
                    case "isNullable" -> ResultSetMetaData.columnNullable;
                    case "isAutoIncrement" -> false;
                    case "isCaseSensitive" -> true;
                    case "isSearchable" -> true;
                    case "isCurrency" -> false;
                    case "isSigned" -> true;
                    case "getColumnDisplaySize" -> 255;
                    case "getSchemaName", "getTableName", "getCatalogName" -> "";
                    case "getPrecision", "getScale" -> 0;
                    case "isReadOnly" -> true;
                    case "isWritable", "isDefinitelyWritable" -> false;
                    case "getColumnClassName" -> String.class.getName();
                    case "unwrap" -> {
                        var cls = (Class<?>) args[0];
                        if (cls.isInstance(proxy)) {
                            yield proxy;
                        }
                        throw new SQLException("Not a wrapper for " + cls.getName());
                    }
                    case "isWrapperFor" -> ((Class<?>) args[0]).isInstance(proxy);
                    default -> throw new SQLFeatureNotSupportedException("ResultSetMetaData method not supported: " + name);
                };
            }
        }

        return (ResultSetMetaData) Proxy.newProxyInstance(
            ResultSetMetaData.class.getClassLoader(),
            new Class<?>[]{ResultSetMetaData.class},
            new MetaDataHandler()
        );
    }

    private Object getByArg(Object arg, List<Object> row, Map<String, Integer> indexByName) throws SQLException {
        if (arg instanceof Integer idx) {
            if (idx < 1 || idx > row.size()) {
                throw new SQLException("Column index out of bounds: " + idx);
            }
            return row.get(idx - 1);
        }

        if (arg instanceof String label) {
            var idx = indexByName.get(label.toLowerCase());
            if (idx == null) {
                throw new SQLException("Unknown column: " + label);
            }
            return row.get(idx - 1);
        }

        throw new SQLException("Unsupported column accessor argument: " + arg);
    }

    private boolean isResultResponse(ServerResponse response) {
        return "RESULT".equalsIgnoreCase(response.type());
    }

    private boolean isUpdateResponse(ServerResponse response) {
        if (!isResultResponse(response)) {
            return false;
        }
        return response.columns() != null
            && response.columns().size() == 1
            && "affected_rows".equalsIgnoreCase(response.columns().get(0));
    }

    private int extractUpdateCount(ServerResponse response) throws SQLException {
        if (response.rows() == null || response.rows().isEmpty() || response.rows().get(0).isEmpty()) {
            return 0;
        }
        var value = response.rows().get(0).get(0);
        if (!(value instanceof Number number)) {
            throw new SQLException("Invalid affected_rows payload");
        }
        return number.intValue();
    }

    @FunctionalInterface
    private interface OpenCheck {
        void ensureOpen() throws SQLException;
    }

    private static final class TcpClient {
        private final Socket socket;
        private final DataInputStream input;
        private final DataOutputStream output;
        private final ObjectMapper objectMapper;
        private boolean closed;

        private TcpClient(String host, int port) throws SQLException {
            try {
                this.socket = new Socket(host, port);
                this.input = new DataInputStream(socket.getInputStream());
                this.output = new DataOutputStream(socket.getOutputStream());
                this.objectMapper = new ObjectMapper();
            } catch (IOException ex) {
                throw new SQLException("Failed to connect to " + host + ":" + port, ex);
            }
        }

        private synchronized ServerResponse query(String sql) throws SQLException {
            ensureOpen();
            return sendRequest(new ServerRequest("QUERY", sql));
        }

        private synchronized void close() throws SQLException {
            if (closed) {
                return;
            }
            try {
                sendRequest(new ServerRequest("CLOSE", null));
            } catch (SQLException ignored) {
                // best-effort close handshake
            }
            try {
                socket.close();
            } catch (IOException ex) {
                throw new SQLException("Failed closing socket", ex);
            } finally {
                closed = true;
            }
        }

        private ServerResponse sendRequest(ServerRequest request) throws SQLException {
            try {
                var payload = objectMapper.writeValueAsString(request).getBytes(StandardCharsets.UTF_8);
                output.writeInt(payload.length);
                output.write(payload);
                output.flush();

                var length = input.readInt();
                var responseBytes = new byte[length];
                input.readFully(responseBytes);
                return objectMapper.readValue(new String(responseBytes, StandardCharsets.UTF_8), ServerResponse.class);
            } catch (IOException ex) {
                throw new SQLException("Failed TCP request", ex);
            }
        }

        private void ensureOpen() throws SQLException {
            if (closed || socket.isClosed()) {
                throw new SQLException("Connection is closed");
            }
        }
    }
}
