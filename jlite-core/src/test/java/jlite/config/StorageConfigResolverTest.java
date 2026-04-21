package jlite.config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

class StorageConfigResolverTest {

    @Test
    void resolvesFromSystemPropertyFirst() throws IOException {
        var baseDir = Files.createTempDirectory("jlite-config-");
        try {
            Files.writeString(baseDir.resolve("jlite.properties"), "jlite.storage.dir=./from-properties");
            Files.writeString(baseDir.resolve(".env"), "JLITE_STORAGE_DIR=./from-dotenv\n");

            var systemProperties = new Properties();
            systemProperties.setProperty(StorageConfigResolver.STORAGE_DIR_PROPERTY, "./from-system");
            var resolved = StorageConfigResolver.resolveStorageDir(baseDir, systemProperties, Map.of(StorageConfigResolver.STORAGE_DIR_ENV, "./from-env"));

            assertEquals(baseDir.resolve("from-system").toAbsolutePath().normalize(), resolved.orElseThrow());
        } finally {
            deleteDirectory(baseDir);
        }
    }

    @Test
    void resolvesFromEnvironmentWhenSystemPropertyMissing() throws IOException {
        var baseDir = Files.createTempDirectory("jlite-config-");
        try {
            var systemProperties = new Properties();
            var resolved = StorageConfigResolver.resolveStorageDir(baseDir, systemProperties, Map.of(StorageConfigResolver.STORAGE_DIR_ENV, "./from-env"));
            assertEquals(baseDir.resolve("from-env").toAbsolutePath().normalize(), resolved.orElseThrow());
        } finally {
            deleteDirectory(baseDir);
        }
    }

    @Test
    void resolvesFromPropertiesFileWhenEnvIsMissing() throws IOException {
        var baseDir = Files.createTempDirectory("jlite-config-");
        try {
            Files.writeString(baseDir.resolve("jlite.properties"), "jlite.storage.dir=./from-properties\n");

            var systemProperties = new Properties();
            var resolved = StorageConfigResolver.resolveStorageDir(baseDir, systemProperties, Map.of());
            assertEquals(baseDir.resolve("from-properties").toAbsolutePath().normalize(), resolved.orElseThrow());
        } finally {
            deleteDirectory(baseDir);
        }
    }

    @Test
    void resolvesFromDotEnvWhenNoOtherSourcesExist() throws IOException {
        var baseDir = Files.createTempDirectory("jlite-config-");
        try {
            Files.writeString(baseDir.resolve(".env"), "# comment\nexport JLITE_STORAGE_DIR=\"./from-dotenv\"\n");

            var systemProperties = new Properties();
            var resolved = StorageConfigResolver.resolveStorageDir(baseDir, systemProperties, Map.of());
            assertEquals(baseDir.resolve("from-dotenv").toAbsolutePath().normalize(), resolved.orElseThrow());
        } finally {
            deleteDirectory(baseDir);
        }
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