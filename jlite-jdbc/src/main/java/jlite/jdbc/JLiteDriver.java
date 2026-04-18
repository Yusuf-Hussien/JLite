package jlite.jdbc;

import java.sql.*;
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
        // TODO: parse url, instantiate TcpConnection or EmbeddedConnection
        throw new SQLFeatureNotSupportedException("JLiteDriver.connect() not yet implemented");
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
}
