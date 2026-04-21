package jlite.agent;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class GeminiSqlTranslator implements NlAgent.SqlTranslator {

    private static final URI API_ROOT = URI.create("https://generativelanguage.googleapis.com/v1beta/models/");

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final String apiKey;
    private final List<String> models;

    public GeminiSqlTranslator(String apiKey, String model) {
        this(HttpClient.newHttpClient(), new ObjectMapper(), apiKey, List.of(model));
    }

    GeminiSqlTranslator(String apiKey, List<String> models) {
        this(HttpClient.newHttpClient(), new ObjectMapper(), apiKey, models);
    }

    GeminiSqlTranslator(HttpClient httpClient, ObjectMapper objectMapper, String apiKey, List<String> models) {
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
        this.apiKey = apiKey;
        this.models = models;
    }

    @Override
    public String translate(String naturalLanguageQuestion, String schemaContext) {
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalStateException("GEMINI_API_KEY is missing");
        }

        var prompt = buildPrompt(naturalLanguageQuestion, schemaContext);
        RuntimeException lastFailure = null;

        for (var model : models) {
            if (model == null || model.isBlank()) {
                continue;
            }

            try {
                return translateWithModel(prompt, model);
            } catch (RuntimeException ex) {
                if (ex.getCause() instanceof InterruptedException) {
                    throw ex;
                }
                lastFailure = ex;
            }
        }

        if (lastFailure != null) {
            throw lastFailure;
        }

        throw new IllegalStateException("No Gemini models configured");
    }

    private String translateWithModel(String prompt, String model) {
        var body = buildRequestBody(prompt);
        var request = HttpRequest.newBuilder()
            .uri(URI.create(API_ROOT + model + ":generateContent?key=" + apiKey))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();

        try {
            var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IllegalStateException("Gemini request failed for model " + model + ": HTTP " + response.statusCode() + " - " + response.body());
            }
            return extractText(response.body());
        } catch (IOException ex) {
            throw new UncheckedIOException("Failed to call Gemini API for model " + model, ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Gemini API call interrupted", ex);
        }
    }

    private String buildPrompt(String naturalLanguageQuestion, String schemaContext) {
        return String.join("\n",
            "You are a SQL generator for JLite.",
            "Return only one SQL statement inside a fenced ```sql block.",
            "Prefer SELECT queries unless the user explicitly asks to modify data.",
            "Do not explain your answer.",
            "Schema:",
            schemaContext == null ? "" : schemaContext,
            "Question:",
            naturalLanguageQuestion
        );
    }

    private String buildRequestBody(String prompt) {
        try {
            var payload = java.util.Map.of(
                "contents", java.util.List.of(
                    java.util.Map.of(
                        "role", "user",
                        "parts", java.util.List.of(java.util.Map.of("text", prompt))
                    )
                ),
                "generationConfig", java.util.Map.of(
                    "temperature", 0.0,
                    "maxOutputTokens", 512
                )
            );
            return objectMapper.writeValueAsString(payload);
        } catch (IOException ex) {
            throw new UncheckedIOException("Failed to build Gemini request body", ex);
        }
    }

    private String extractText(String responseBody) {
        try {
            JsonNode root = objectMapper.readTree(responseBody);
            var candidates = root.path("candidates");
            if (!candidates.isArray() || candidates.isEmpty()) {
                throw new IllegalStateException("Gemini response missing candidates");
            }

            var content = candidates.get(0).path("content");
            var parts = content.path("parts");
            if (!parts.isArray()) {
                throw new IllegalStateException("Gemini response missing parts");
            }

            var builder = new StringBuilder();
            for (var part : parts) {
                var text = part.path("text").asText(null);
                if (text != null) {
                    builder.append(text);
                }
            }

            var extracted = NlAgent.extractSql(builder.toString());
            if (extracted != null) {
                return extracted;
            }

            return builder.toString().trim();
        } catch (IOException ex) {
            throw new UncheckedIOException("Failed to parse Gemini response", ex);
        }
    }
}