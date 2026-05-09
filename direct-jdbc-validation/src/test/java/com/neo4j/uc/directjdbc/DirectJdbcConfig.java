package com.neo4j.uc.directjdbc;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

final class DirectJdbcConfig {

    private static final String DEFAULT_DATABASE = "neo4j";

    private final String jdbcUrl;
    private final String cypherJdbcUrl;
    private final String username;
    private final String password;

    private DirectJdbcConfig(String jdbcUrl, String cypherJdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.cypherJdbcUrl = cypherJdbcUrl;
        this.username = username;
        this.password = password;
    }

    static DirectJdbcConfig load() {
        Map<String, String> fileValues = loadDotEnv(Path.of(".env"));

        String uri = value("NEO4J_URI", fileValues);
        String host = value("NEO4J_HOST", fileValues);
        String username = value("NEO4J_USERNAME", fileValues);
        String password = value("NEO4J_PASSWORD", fileValues);
        String database = value("NEO4J_DATABASE", fileValues);

        if (isBlank(database)) {
            database = DEFAULT_DATABASE;
        }
        if (isBlank(username)) {
            username = "neo4j";
        }
        if (isBlank(password)) {
            fail("Missing NEO4J_PASSWORD. Set it in direct-jdbc-validation/.env or the environment.");
        }

        String jdbcBaseUrl = "%s/%s".formatted(toJdbcUri(uri, host), database);
        String jdbcUrl = jdbcBaseUrl + "?enableSQLTranslation=true";

        return new DirectJdbcConfig(jdbcUrl, jdbcBaseUrl, username, password);
    }

    Connection connect() throws SQLException {
        return connect(jdbcUrl);
    }

    Connection connectCypher() throws SQLException {
        return connect(cypherJdbcUrl);
    }

    private Connection connect(String url) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", username);
        props.setProperty("password", password);
        return DriverManager.getConnection(url, props);
    }

    String jdbcUrl() {
        return jdbcUrl;
    }

    private static String toJdbcUri(String uri, String host) {
        if (!isBlank(uri)) {
            return toJdbcUri(uri.trim());
        }
        if (!isBlank(host)) {
            String cleanHost = host.trim()
                    .replaceFirst("^neo4j\\+s://", "")
                    .replaceFirst("^neo4j\\+ssc://", "")
                    .replaceFirst("^neo4j://", "");
            return toJdbcUri("neo4j+s://%s".formatted(stripTrailingSlash(cleanHost)));
        }
        fail("Missing NEO4J_URI or NEO4J_HOST. Set one in direct-jdbc-validation/.env or the environment.");
        throw new IllegalStateException("unreachable");
    }

    private static String toJdbcUri(String value) {
        String neo4jUri = stripTrailingSlash(value);
        if (neo4jUri.startsWith("jdbc:neo4j")) {
            return stripPathAndQuery(neo4jUri);
        }
        if (neo4jUri.startsWith("neo4j+s://")) {
            return "jdbc:" + ensurePort(stripPathAndQuery(neo4jUri), 7687);
        }
        if (neo4jUri.startsWith("neo4j+ssc://")) {
            return "jdbc:" + ensurePort(stripPathAndQuery(neo4jUri), 7687);
        }
        if (neo4jUri.startsWith("neo4j://")) {
            return "jdbc:" + ensurePort(stripPathAndQuery(neo4jUri), 7687);
        }
        fail("NEO4J_URI must start with neo4j://, neo4j+s://, neo4j+ssc://, or jdbc:neo4j.");
        throw new IllegalStateException("unreachable");
    }

    private static String stripPathAndQuery(String value) {
        String parseable = value.startsWith("jdbc:") ? value.substring("jdbc:".length()) : value;
        try {
            URI uri = new URI(parseable);
            String host = uri.getHost();
            if (host == null) {
                return value;
            }
            StringBuilder result = new StringBuilder(uri.getScheme()).append("://").append(host);
            if (uri.getPort() > 0) {
                result.append(':').append(uri.getPort());
            }
            return value.startsWith("jdbc:") ? "jdbc:" + result : result.toString();
        } catch (URISyntaxException e) {
            return value;
        }
    }

    private static String ensurePort(String uri, int defaultPort) {
        String schemeSuffix = "://";
        int schemeEnd = uri.indexOf(schemeSuffix);
        String prefix = uri.substring(0, schemeEnd + schemeSuffix.length());
        String rest = uri.substring(schemeEnd + schemeSuffix.length());
        if (rest.contains(":")) {
            return uri;
        }
        return prefix + rest + ":" + defaultPort;
    }

    private static String stripTrailingSlash(String value) {
        String result = value;
        while (result.endsWith("/")) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }

    private static String value(String key, Map<String, String> fileValues) {
        String envValue = System.getenv(key);
        if (!isBlank(envValue)) {
            return envValue;
        }
        return fileValues.get(key);
    }

    private static Map<String, String> loadDotEnv(Path path) {
        Map<String, String> values = new HashMap<>();
        if (!Files.exists(path)) {
            return values;
        }

        try {
            for (String rawLine : Files.readAllLines(path)) {
                String line = rawLine.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                int separator = line.indexOf('=');
                if (separator <= 0) {
                    continue;
                }
                String key = line.substring(0, separator).trim();
                String value = line.substring(separator + 1).trim();
                values.put(key, unquote(value));
            }
        } catch (IOException e) {
            fail("Failed to read " + path + ": " + e.getMessage());
        }
        return values;
    }

    private static String unquote(String value) {
        if (value.length() >= 2) {
            char first = value.charAt(0);
            char last = value.charAt(value.length() - 1);
            if ((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
                return value.substring(1, value.length() - 1);
            }
        }
        return value;
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
