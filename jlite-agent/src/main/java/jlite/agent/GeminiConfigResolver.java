package jlite.agent;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public final class GeminiConfigResolver {

    public static final String GEMINI_API_KEY_PROPERTY = "GEMINI_API_KEY";
    public static final String GEMINI_MODEL_PROPERTY = "GEMINI_MODEL";
    public static final String DEFAULT_GEMINI_MODEL = "gemini-2.0-flash-lite";
    private static final List<String> DEFAULT_GEMINI_MODEL_CANDIDATES = List.of(
        "gemini-2.0-flash-lite",
        "gemini-2.0-flash",
        "gemini-flash-latest",
        "gemini-flash-lite-latest"
    );

    private GeminiConfigResolver() {
    }

    public static Optional<GeminiConfig> resolve() {
        var baseDir = Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize();
        return resolve(baseDir, System.getProperties(), System.getenv());
    }

    static Optional<GeminiConfig> resolve(Path baseDir, Properties systemProperties, Map<String, String> environment) {
        var apiKey = firstNonBlank(
            systemProperties.getProperty(GEMINI_API_KEY_PROPERTY),
            environment.get(GEMINI_API_KEY_PROPERTY),
            loadFromPropertiesFile(baseDir.resolve("jlite.properties"), GEMINI_API_KEY_PROPERTY),
            loadFromPropertiesFile(baseDir.resolve("application.properties"), GEMINI_API_KEY_PROPERTY),
            loadFromDotEnv(baseDir.resolve(".env"), GEMINI_API_KEY_PROPERTY)
        );

        if (apiKey == null) {
            return Optional.empty();
        }

        var model = firstNonBlank(
            systemProperties.getProperty(GEMINI_MODEL_PROPERTY),
            environment.get(GEMINI_MODEL_PROPERTY),
            loadFromPropertiesFile(baseDir.resolve("jlite.properties"), GEMINI_MODEL_PROPERTY),
            loadFromPropertiesFile(baseDir.resolve("application.properties"), GEMINI_MODEL_PROPERTY),
            loadFromDotEnv(baseDir.resolve(".env"), GEMINI_MODEL_PROPERTY)
        );

        return Optional.of(new GeminiConfig(apiKey, model == null ? DEFAULT_GEMINI_MODEL : model));
    }

    static List<String> candidateModels(String configuredModel) {
        var ordered = new java.util.LinkedHashSet<String>();
        var normalizedConfiguredModel = normalize(configuredModel);
        if (normalizedConfiguredModel != null) {
            ordered.add(normalizedConfiguredModel);
        }
        ordered.addAll(DEFAULT_GEMINI_MODEL_CANDIDATES);
        return List.copyOf(ordered);
    }

    private static String loadFromPropertiesFile(Path file, String key) {
        if (!Files.isRegularFile(file)) {
            return null;
        }

        var properties = new Properties();
        try (var reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            properties.load(reader);
        } catch (IOException ex) {
            throw new UncheckedIOException("Failed to read properties file: " + file, ex);
        }

        return normalize(properties.getProperty(key));
    }

    private static String loadFromDotEnv(Path file, String key) {
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

            var candidateKey = line.substring(0, equalsIndex).trim();
            if (!key.equals(candidateKey)) {
                continue;
            }

            var value = line.substring(equalsIndex + 1).trim();
            if ((value.startsWith("\"") && value.endsWith("\"")) || (value.startsWith("'") && value.endsWith("'"))) {
                value = value.substring(1, value.length() - 1);
            }
            return normalize(value);
        }

        return null;
    }

    private static String firstNonBlank(String... values) {
        for (var value : values) {
            var normalized = normalize(value);
            if (normalized != null) {
                return normalized;
            }
        }
        return null;
    }

    private static String normalize(String value) {
        if (value == null) {
            return null;
        }
        var trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    public record GeminiConfig(String apiKey, String model) {
    }
}