package jlite.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jlite.catalogue.Column;
import jlite.catalogue.DataType;
import jlite.catalogue.TableSchema;
import jlite.executor.QueryEngine;
import jlite.server.TcpServer;
import java.sql.DriverManager;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JLiteDriverTest {

    @Test
    void executesQueryThroughJdbcDriverOverTcp() throws Exception {
        var engine = new QueryEngine();
        engine.createTable(new TableSchema(
            "users",
            List.of(
                new Column("id", DataType.INT, false, true),
                new Column("name", DataType.TEXT, true, false),
                new Column("age", DataType.INT, true, false),
                new Column("active", DataType.BOOLEAN, true, false)
            )
        ));
        engine.insertRow("users", Map.of("id", 1L, "name", "Alice", "age", 30L, "active", true));
        engine.insertRow("users", Map.of("id", 2L, "name", "Bob", "age", 17L, "active", false));
        engine.insertRow("users", Map.of("id", 3L, "name", "Cara", "age", 41L, "active", true));

        var server = new TcpServer(0, engine);
        var serverThread = Thread.ofVirtual().start(() -> {
            try {
                server.start();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });

        try {
            server.awaitStarted();
            var jdbcUrl = "jdbc:jlite://127.0.0.1:" + server.getPort() + "/default";

            try (var connection = DriverManager.getConnection(jdbcUrl);
                 var statement = connection.createStatement();
                 var resultSet = statement.executeQuery("SELECT name, age FROM users WHERE active = true AND age >= 18")) {

                assertTrue(resultSet.next());
                assertEquals("Alice", resultSet.getString("name"));
                assertEquals(30, resultSet.getInt("age"));

                assertTrue(resultSet.next());
                assertEquals("Cara", resultSet.getString(1));
                assertEquals(41L, resultSet.getLong(2));

                assertEquals(2, resultSet.getMetaData().getColumnCount());
                assertEquals("name", resultSet.getMetaData().getColumnLabel(1));
                assertEquals("age", resultSet.getMetaData().getColumnLabel(2));
                assertTrue(!resultSet.next());
            }
        } finally {
            server.stop();
            serverThread.join(3000);
        }
    }

    @Test
    void executesPreparedStatementWithBoundParameters() throws Exception {
        var engine = new QueryEngine();
        engine.createTable(new TableSchema(
            "users",
            List.of(
                new Column("id", DataType.INT, false, true),
                new Column("name", DataType.TEXT, true, false),
                new Column("age", DataType.INT, true, false),
                new Column("active", DataType.BOOLEAN, true, false)
            )
        ));
        engine.insertRow("users", Map.of("id", 1L, "name", "Alice", "age", 30L, "active", true));
        engine.insertRow("users", Map.of("id", 2L, "name", "Bob", "age", 17L, "active", false));
        engine.insertRow("users", Map.of("id", 3L, "name", "Cara", "age", 41L, "active", true));

        var server = new TcpServer(0, engine);
        var serverThread = Thread.ofVirtual().start(() -> {
            try {
                server.start();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });

        try {
            server.awaitStarted();
            var jdbcUrl = "jdbc:jlite://127.0.0.1:" + server.getPort() + "/default";

            try (var connection = DriverManager.getConnection(jdbcUrl);
                 var statement = connection.prepareStatement("SELECT name, age FROM users WHERE active = ? AND age >= ?")) {
                statement.setBoolean(1, true);
                statement.setInt(2, 18);

                try (var resultSet = statement.executeQuery()) {
                    assertTrue(resultSet.next());
                    assertEquals("Alice", resultSet.getString("name"));
                    assertEquals(30, resultSet.getInt("age"));

                    assertTrue(resultSet.next());
                    assertEquals("Cara", resultSet.getString("name"));
                    assertEquals(41, resultSet.getInt("age"));
                    assertTrue(!resultSet.next());
                }
            }
        } finally {
            server.stop();
            serverThread.join(3000);
        }
    }

    @Test
    void preparedStatementFailsWhenParameterMissing() throws Exception {
        var engine = new QueryEngine();
        engine.createTable(new TableSchema(
            "users",
            List.of(
                new Column("id", DataType.INT, false, true),
                new Column("name", DataType.TEXT, true, false),
                new Column("age", DataType.INT, true, false),
                new Column("active", DataType.BOOLEAN, true, false)
            )
        ));
        engine.insertRow("users", Map.of("id", 1L, "name", "Alice", "age", 30L, "active", true));

        var server = new TcpServer(0, engine);
        var serverThread = Thread.ofVirtual().start(() -> {
            try {
                server.start();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });

        try {
            server.awaitStarted();
            var jdbcUrl = "jdbc:jlite://127.0.0.1:" + server.getPort() + "/default";

            try (var connection = DriverManager.getConnection(jdbcUrl);
                 var statement = connection.prepareStatement("SELECT name FROM users WHERE active = ? AND age >= ?")) {
                statement.setBoolean(1, true);

                var ex = assertThrows(java.sql.SQLException.class, statement::executeQuery);
                assertTrue(ex.getMessage().contains("Missing value for parameter index 2"));
            }
        } finally {
            server.stop();
            serverThread.join(3000);
        }
    }

    @Test
    void preparedStatementSupportsStringAndLongBinding() throws Exception {
        var engine = new QueryEngine();
        engine.createTable(new TableSchema(
            "users",
            List.of(
                new Column("id", DataType.INT, false, true),
                new Column("name", DataType.TEXT, true, false),
                new Column("age", DataType.INT, true, false),
                new Column("active", DataType.BOOLEAN, true, false)
            )
        ));
        engine.insertRow("users", Map.of("id", 1L, "name", "Alice", "age", 30L, "active", true));
        engine.insertRow("users", Map.of("id", 2L, "name", "Bob", "age", 17L, "active", false));

        var server = new TcpServer(0, engine);
        var serverThread = Thread.ofVirtual().start(() -> {
            try {
                server.start();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });

        try {
            server.awaitStarted();
            var jdbcUrl = "jdbc:jlite://127.0.0.1:" + server.getPort() + "/default";

            try (var connection = DriverManager.getConnection(jdbcUrl);
                 var statement = connection.prepareStatement("SELECT id, name FROM users WHERE name = ? AND id >= ?")) {
                statement.setString(1, "Alice");
                statement.setLong(2, 1L);

                try (var resultSet = statement.executeQuery()) {
                    assertTrue(resultSet.next());
                    assertEquals(1L, resultSet.getLong("id"));
                    assertEquals("Alice", resultSet.getString("name"));
                    assertTrue(!resultSet.next());
                }
            }
        } finally {
            server.stop();
            serverThread.join(3000);
        }
    }

    @Test
    void executeUpdateFailsForSelectStatements() throws Exception {
        var engine = new QueryEngine();
        engine.createTable(new TableSchema(
            "users",
            List.of(new Column("id", DataType.INT, false, true))
        ));
        engine.insertRow("users", Map.of("id", 1L));

        var server = new TcpServer(0, engine);
        var serverThread = Thread.ofVirtual().start(() -> {
            try {
                server.start();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });

        try {
            server.awaitStarted();
            var jdbcUrl = "jdbc:jlite://127.0.0.1:" + server.getPort() + "/default";

            try (var connection = DriverManager.getConnection(jdbcUrl);
                 var statement = connection.createStatement()) {
                var ex = assertThrows(SQLException.class, () -> statement.executeUpdate("SELECT id FROM users"));
                assertTrue(ex.getMessage().contains("executeUpdate cannot be used for SELECT statements"));
            }

            try (var connection = DriverManager.getConnection(jdbcUrl);
                 var statement = connection.prepareStatement("SELECT id FROM users WHERE id >= ?")) {
                statement.setInt(1, 1);
                var ex = assertThrows(SQLException.class, statement::executeUpdate);
                assertTrue(ex.getMessage().contains("executeUpdate cannot be used for SELECT statements"));
            }
        } finally {
            server.stop();
            serverThread.join(3000);
        }
    }

    @Test
    void executeUpdateReturnsAffectedRowsForDmlAndDdl() throws Exception {
        var engine = new QueryEngine();
        var server = new TcpServer(0, engine);
        var serverThread = Thread.ofVirtual().start(() -> {
            try {
                server.start();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });

        try {
            server.awaitStarted();
            var jdbcUrl = "jdbc:jlite://127.0.0.1:" + server.getPort() + "/default";

            try (var connection = DriverManager.getConnection(jdbcUrl);
                 var statement = connection.createStatement()) {
                assertEquals(0, statement.executeUpdate("CREATE TABLE users (id INT, name TEXT)"));
                assertEquals(2, statement.executeUpdate("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')"));
                assertEquals(1, statement.executeUpdate("UPDATE users SET name = 'Bobby' WHERE id = 2"));
                assertEquals(1, statement.executeUpdate("DELETE FROM users WHERE id = 1"));
                assertEquals(0, statement.executeUpdate("DROP TABLE users"));
            }
        } finally {
            server.stop();
            serverThread.join(3000);
        }
    }

    @Test
    void rejectsEmbeddedUrlForNow() {
        var driver = new JLiteDriver();
        var ex = assertThrows(SQLFeatureNotSupportedException.class, () -> {
            driver.connect("jdbc:jlite:file:/tmp/db", null);
        });
        assertTrue(ex.getMessage().contains("Embedded URL mode is not implemented"));
    }
}
