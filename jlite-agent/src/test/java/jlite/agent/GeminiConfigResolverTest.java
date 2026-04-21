package jlite.agent;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

class GeminiConfigResolverTest {

    @Test
    void resolvesApiKeyFromPropertiesFileBeforeDotEnv() throws IOException {
        var baseDir = Files.createTempDirectory("jlite-gemini-");
        try {
            Files.writeString(baseDir.resolve("jlite.properties"), "GEMINI_API_KEY=from-properties\nGEMINI_MODEL=gemini-pro\n");
            Files.writeString(baseDir.resolve(".env"), "GEMINI_API_KEY=from-dotenv\nGEMINI_MODEL=gemini-2.0-flash\n");

            var resolved = GeminiConfigResolver.resolve(baseDir, new Properties(), Map.of());

            assertTrue(resolved.isPresent());
            assertEquals("from-properties", resolved.orElseThrow().apiKey());
            assertEquals("gemini-pro", resolved.orElseThrow().model());
        } finally {
            deleteDirectory(baseDir);
        }
    }

    @Test
    void defaultsToFreeGeminiModelWhenModelIsMissing() throws IOException {
        var baseDir = Files.createTempDirectory("jlite-gemini-");
        try {
            var props = new Properties();
            props.setProperty(GeminiConfigResolver.GEMINI_API_KEY_PROPERTY, "from-system");

            var resolved = GeminiConfigResolver.resolve(baseDir, props, Map.of());

            assertTrue(resolved.isPresent());
            assertEquals(GeminiConfigResolver.DEFAULT_GEMINI_MODEL, resolved.orElseThrow().model());
        } finally {
            deleteDirectory(baseDir);
        }
    }

    @Test
    void configuredModelIsTriedBeforeFallbackCandidates() {
        var candidates = GeminiConfigResolver.candidateModels("gemini-custom");

        assertEquals("gemini-custom", candidates.get(0));
        assertTrue(candidates.containsAll(List.of(
            "gemini-2.0-flash-lite",
            "gemini-2.0-flash",
            "gemini-flash-latest",
            "gemini-flash-lite-latest"
        )));
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