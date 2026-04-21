package jlite.executor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import jlite.storage.Page;

class QueryEnginePersistentStorageTest {

    @Test
    void persistsTablesAndRowsAcrossEngineRestart() throws IOException {
        var storageDir = Files.createTempDirectory("jlite-storage-");
        try {
            var engine1 = new QueryEngine(storageDir);
            engine1.execute("CREATE TABLE projects (id INT, name TEXT, active BOOLEAN)");
            engine1.execute("INSERT INTO projects VALUES (1, 'Core', true), (2, 'Server', false)");
            engine1.execute("UPDATE projects SET active = true WHERE id = 2");

            var engine2 = new QueryEngine(storageDir);
            var result = engine2.execute("SELECT id, name, active FROM projects WHERE active = true");

            assertEquals(java.util.List.of("id", "name", "active"), result.columns());
            assertEquals(2, result.rows().size());
            assertEquals(java.util.List.of(1L, "Core", true), result.rows().get(0));
            assertEquals(java.util.List.of(2L, "Server", true), result.rows().get(1));
        } finally {
            deleteDirectory(storageDir);
        }
    }

    @Test
    void handlesConcurrentWritesSafelyAndPersistsThem() throws Exception {
        var storageDir = Files.createTempDirectory("jlite-storage-");
        try {
            var engine = new QueryEngine(storageDir);
            engine.execute("CREATE TABLE events (id INT, name TEXT)");

            var executorService = Executors.newFixedThreadPool(4);
            var ready = new CountDownLatch(1);
            var completed = new CountDownLatch(20);

            for (int i = 0; i < 20; i++) {
                var id = i;
                executorService.submit(() -> {
                    try {
                        ready.await();
                        engine.execute("INSERT INTO events VALUES (" + id + ", 'event-" + id + "')");
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(ex);
                    } catch (RuntimeException ex) {
                        throw new RuntimeException(ex);
                    } finally {
                        completed.countDown();
                    }
                });
            }

            ready.countDown();
            assertEquals(true, completed.await(10, TimeUnit.SECONDS));
            executorService.shutdownNow();

            var reloadedEngine = new QueryEngine(storageDir);
            var result = reloadedEngine.execute("SELECT id, name FROM events");

            assertEquals(20, result.rows().size());
            assertTrue(containsRow(result.rows(), 0L, "event-0"));
            assertTrue(containsRow(result.rows(), 19L, "event-19"));
        } finally {
            deleteDirectory(storageDir);
        }
    }

    @Test
    void persistsTablesAsFixedSizePages() throws IOException {
        var storageDir = Files.createTempDirectory("jlite-storage-");
        try {
            var engine = new QueryEngine(storageDir);
            engine.execute("CREATE TABLE logs (id INT, message TEXT)");
            engine.execute("INSERT INTO logs VALUES (1, 'hello page storage')");

            var tableFile = storageDir.resolve("logs.tbl");
            assertTrue(Files.exists(tableFile));

            var fileSize = Files.size(tableFile);
            assertEquals(0L, fileSize % Page.PAGE_SIZE);
            assertTrue(fileSize >= Page.PAGE_SIZE);
        } finally {
            deleteDirectory(storageDir);
        }
    }

    @Test
    void persistsRowsAcrossMultipleSlottedPages() throws IOException {
        var storageDir = Files.createTempDirectory("jlite-storage-");
        try {
            var engine = new QueryEngine(storageDir);
            engine.execute("CREATE TABLE metrics (id INT, payload TEXT)");

            for (int i = 0; i < 300; i++) {
                var payload = "payload-" + i + "-" + "x".repeat(48);
                engine.execute("INSERT INTO metrics VALUES (" + i + ", '" + payload + "')");
            }

            var tableFile = storageDir.resolve("metrics.tbl");
            var fileSize = Files.size(tableFile);
            assertEquals(0L, fileSize % Page.PAGE_SIZE);
            assertTrue(fileSize >= 2L * Page.PAGE_SIZE);

            var reloadedEngine = new QueryEngine(storageDir);
            var result = reloadedEngine.execute("SELECT id, payload FROM metrics");
            assertEquals(300, result.rows().size());
            assertTrue(containsRow(result.rows(), 0L, "payload-0-" + "x".repeat(48)));
            assertTrue(containsRow(result.rows(), 299L, "payload-299-" + "x".repeat(48)));
        } finally {
            deleteDirectory(storageDir);
        }
    }

    @Test
    void updatesAndDeletesInPlaceWithoutGrowingFile() throws IOException {
        var storageDir = Files.createTempDirectory("jlite-storage-");
        try {
            var engine = new QueryEngine(storageDir);
            engine.execute("CREATE TABLE users (id INT, name TEXT, active BOOLEAN)");
            engine.execute("INSERT INTO users VALUES (1, 'Alice', true), (2, 'Bobby', true), (3, 'Cara', false)");

            var tableFile = storageDir.resolve("users.tbl");
            var sizeBefore = Files.size(tableFile);

            engine.execute("UPDATE users SET name = 'Boby' WHERE id = 2");
            engine.execute("DELETE FROM users WHERE id = 3");

            var sizeAfter = Files.size(tableFile);
            assertEquals(sizeBefore, sizeAfter);

            var reloadedEngine = new QueryEngine(storageDir);
            var result = reloadedEngine.execute("SELECT id, name, active FROM users");
            assertEquals(2, result.rows().size());
            assertTrue(containsRow(result.rows(), 1L, "Alice"));
            assertTrue(containsRow(result.rows(), 2L, "Boby"));
            assertTrue(result.rows().stream().noneMatch(row -> java.util.Objects.equals(row.get(0), 3L)));
        } finally {
            deleteDirectory(storageDir);
        }
    }

    private boolean containsRow(java.util.List<java.util.List<Object>> rows, long id, String name) {
        return rows.stream().anyMatch(row -> row.size() >= 2 && java.util.Objects.equals(row.get(0), id) && java.util.Objects.equals(row.get(1), name));
    }

    private void deleteDirectory(Path dir) throws IOException {
        try (var stream = Files.walk(dir)) {
            stream
                .sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                });
        } catch (RuntimeException ex) {
            if (ex.getCause() instanceof IOException ioException) {
                throw ioException;
            }
            throw ex;
        }
    }
}