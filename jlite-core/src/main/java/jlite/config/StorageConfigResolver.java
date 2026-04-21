package jlite.config;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public final class StorageConfigResolver {

    public static final String STORAGE_DIR_PROPERTY = "jlite.storage.dir";
    public static final String STORAGE_DIR_ENV = "JLITE_STORAGE_DIR";

    private StorageConfigResolver() {
    }

    public static Optional<Path> resolveStorageDir() {
        var baseDir = Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize();
        return resolveStorageDir(baseDir, System.getProperties(), System.getenv());
    }

    static Optional<Path> resolveStorageDir(Path baseDir, Properties systemProperties, Map<String, String> environment) {
        var fromProperty = normalizePath(systemProperties.getProperty(STORAGE_DIR_PROPERTY));
        if (fromProperty != null) {
            return Optional.of(baseDir.resolve(fromProperty).toAbsolutePath().normalize());
        }

        var fromEnv = normalizePath(environment.get(STORAGE_DIR_ENV));
        if (fromEnv != null) {
            return Optional.of(baseDir.resolve(fromEnv).toAbsolutePath().normalize());
        }

        for (var fileName : List.of("jlite.properties", "application.properties")) {
            var fromPropertiesFile = loadFromPropertiesFile(baseDir.resolve(fileName));
            if (fromPropertiesFile != null) {
                return Optional.of(baseDir.resolve(fromPropertiesFile).toAbsolutePath().normalize());
            }
        }

        var fromDotEnv = loadFromDotEnv(baseDir.resolve(".env"));
        if (fromDotEnv != null) {
            return Optional.of(baseDir.resolve(fromDotEnv).toAbsolutePath().normalize());
        }

        return Optional.empty();
    }

    private static String loadFromPropertiesFile(Path file) {
        if (!Files.isRegularFile(file)) {
            return null;
        }

        var properties = new Properties();
        try (var reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            properties.load(reader);
        } catch (IOException ex) {
            throw new UncheckedIOException("Failed to read properties file: " + file, ex);
        }

        return normalizePath(properties.getProperty(STORAGE_DIR_PROPERTY));
    }

    private static String loadFromDotEnv(Path file) {
        if (!Files.isRegularFile(file)) {
            return null;
        }

        List<String> lines;
        try {
            lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        } catch (IOException ex) {
            throw new UncheckedIOException("Failed to read .env file: " + file, ex);
        }

        for (var rawLine : lines) {
            var line = rawLine.trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }

            if (line.startsWith("export ")) {
                line = line.substring("export ".length()).trim();
            }

            var equalsIndex = line.indexOf('=');
            if (equalsIndex <= 0) {
                continue;
            }

            var key = line.substring(0, equalsIndex).trim();
            if (!STORAGE_DIR_ENV.equals(key)) {
                continue;
            }

            var value = line.substring(equalsIndex + 1).trim();
            if ((value.startsWith("\"") && value.endsWith("\"")) || (value.startsWith("'") && value.endsWith("'"))) {
                value = value.substring(1, value.length() - 1);
            }
            return normalizePath(value);
        }

        return null;
    }

    private static String normalizePath(String rawPath) {
        if (rawPath == null || rawPath.isBlank()) {
            return null;
        }
        return rawPath.trim();
    }
}