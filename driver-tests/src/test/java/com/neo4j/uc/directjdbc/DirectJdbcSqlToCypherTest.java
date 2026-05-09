package com.neo4j.uc.directjdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
final class DirectJdbcSqlToCypherTest {

    private final DirectJdbcConfig config = DirectJdbcConfig.load();

    // validation/run_01 Section 2: Neo4j Python Driver

    @Test
    void run01PythonDriverReturnOne() throws SQLException {
        try (Connection connection = config.connectCypher()) {
            int value = queryInt(connection, "RETURN 1 AS test", "test");
            assertEquals(1, value);
        }
    }

    // validation/run_01 Section 3: Neo4j Spark Connector

    @Test
    void run01SparkConnectorReturnOk() throws SQLException {
        try (Connection connection = config.connectCypher();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("RETURN 'ok' AS message, 1 AS value")) {
            assertTrue(resultSet.next());
            assertEquals("ok", resultSet.getString("message"));
            assertEquals(1, resultSet.getInt("value"));
        }
    }

    // validation/run_01 Section 4: Direct JDBC Tests

    @Test
    void run01DirectJdbcAircraftTableRead() throws SQLException {
        try (Connection connection = config.connect();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("""
                        SELECT aircraftId, tail_number, icao24, model, operator, manufacturer
                        FROM Aircraft
                        """)) {
            assertTrue(resultSet.next(), "Expected Aircraft table query to return rows from " + config.jdbcUrl());
            assertFalse(resultSet.getString("aircraftId").isBlank());
        }
    }

    @Test
    void run01DirectJdbcSqlTranslationSelectOne() throws SQLException {
        try (Connection connection = config.connect()) {
            int value = queryInt(connection, "SELECT 1 AS value", "value");
            assertEquals(1, value);
        }
    }

    @Test
    void run01DirectJdbcCountAggregate() throws SQLException {
        try (Connection connection = config.connect()) {
            long count = queryLong(connection, "SELECT COUNT(*) AS flight_count FROM Flight", "flight_count");
            assertEquals(800L, count);
        }
    }

    @Test
    void run01DirectJdbcNaturalJoin() throws SQLException {
        try (Connection connection = config.connect()) {
            long count = queryLong(connection, """
                    SELECT COUNT(*) AS cnt
                    FROM Flight f
                    NATURAL JOIN DEPARTS_FROM r
                    NATURAL JOIN Airport a
                    """, "cnt");
            assertEquals(800L, count);
        }
    }

    // validation/run_01 Section 6: UC JDBC Queries, executed here as direct JDBC.

    @Test
    void run01UcJdbcBasicQuerySelectOne() throws SQLException {
        try (Connection connection = config.connect()) {
            int value = queryInt(connection, "SELECT 1 AS test", "test");
            assertEquals(1, value);
        }
    }

    @Test
    void run01UcRemoteQuerySelectOne() throws SQLException {
        try (Connection connection = config.connect()) {
            int value = queryInt(connection, "SELECT 1 AS test", "test");
            assertEquals(1, value);
        }
    }

    @Test
    void run01UcJdbcCount() throws SQLException {
        try (Connection connection = config.connect()) {
            long count = queryLong(connection, "SELECT COUNT(*) AS flight_count FROM Flight", "flight_count");
            assertEquals(800L, count);
        }
    }

    @Test
    void run01UcJdbcJoinAggregate() throws SQLException {
        try (Connection connection = config.connect()) {
            long count = queryLong(connection, """
                    SELECT COUNT(*) AS relationship_count
                    FROM Flight f
                    NATURAL JOIN DEPARTS_FROM r
                    NATURAL JOIN Airport a
                    """, "relationship_count");
            assertEquals(800L, count);
        }
    }

    @Test
    void run01UcJdbcWhereAggregate() throws SQLException {
        try (Connection connection = config.connect()) {
            long count = queryLong(connection,
                    "SELECT COUNT(*) AS boeing_count FROM Aircraft WHERE manufacturer = 'Boeing'",
                    "boeing_count");
            assertEquals(5L, count);
        }
    }

    @Test
    void run01UcJdbcMultipleAggregatesOriginalQuery() throws SQLException {
        try (Connection connection = config.connect();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("""
                        SELECT COUNT(*) AS total,
                               MIN(aircraftId) AS first_id,
                               MAX(aircraftId) AS last_id
                        FROM Aircraft
                        """)) {
            assertTrue(resultSet.next());
            assertEquals(20L, resultSet.getLong("total"));
        }
    }

    @Test
    void run01UcJdbcMultipleAggregatesCanonicalPropertyQuery() throws SQLException {
        try (Connection connection = config.connect();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("""
                        SELECT COUNT(*) AS total,
                               MIN(aircraftId) AS first_id,
                               MAX(aircraftId) AS last_id
                        FROM Aircraft
                        """)) {
            assertTrue(resultSet.next());
            assertEquals(20L, resultSet.getLong("total"));
            assertEquals("AC1001", resultSet.getString("first_id"));
            assertEquals("AC1020", resultSet.getString("last_id"));
        }
    }

    @Test
    void run01UcJdbcCountDistinct() throws SQLException {
        try (Connection connection = config.connect()) {
            long count = queryLong(connection,
                    "SELECT COUNT(DISTINCT manufacturer) AS unique_manufacturers FROM Aircraft",
                    "unique_manufacturers");
            assertEquals(3L, count);
        }
    }

    // validation/run_02 Section 2: Verify Neo4j via UC JDBC.

    @Test
    void run02Neo4jCountAircraft() throws SQLException {
        try (Connection connection = config.connect()) {
            long count = queryLong(connection, "SELECT COUNT(*) AS cnt FROM Aircraft", "cnt");
            assertEquals(20L, count);
        }
    }

    @Test
    void run02Neo4jCountMaintenanceEvent() throws SQLException {
        try (Connection connection = config.connect()) {
            long count = queryLong(connection, "SELECT COUNT(*) AS cnt FROM MaintenanceEvent", "cnt");
            assertEquals(300L, count);
        }
    }

    @Test
    void run02Neo4jCountFlight() throws SQLException {
        try (Connection connection = config.connect()) {
            long count = queryLong(connection, "SELECT COUNT(*) AS cnt FROM Flight", "cnt");
            assertEquals(800L, count);
        }
    }

    @Test
    void run02Neo4jTraversalFlightToAirport() throws SQLException {
        try (Connection connection = config.connect()) {
            long count = queryLong(connection,
                    "SELECT COUNT(*) AS cnt FROM Flight f NATURAL JOIN DEPARTS_FROM r NATURAL JOIN Airport a",
                    "cnt");
            assertEquals(800L, count);
        }
    }

    // validation/run_02 Section 3: Neo4j remote_query components.

    @Test
    void run02FleetSummaryTotalMaintenanceEvents() throws SQLException {
        try (Connection connection = config.connect()) {
            long count = queryLong(connection, "SELECT COUNT(*) AS cnt FROM MaintenanceEvent", "cnt");
            assertEquals(300L, count);
        }
    }

    @Test
    void run02FleetSummaryCriticalMaintenanceEvents() throws SQLException {
        try (Connection connection = config.connect()) {
            long count = queryLong(connection,
                    "SELECT COUNT(*) AS cnt FROM MaintenanceEvent WHERE severity = 'CRITICAL'",
                    "cnt");
            assertTrue(count > 0L, "Expected at least one critical maintenance event");
        }
    }

    @Test
    void run02FleetSummaryTotalFlights() throws SQLException {
        try (Connection connection = config.connect()) {
            long count = queryLong(connection, "SELECT COUNT(*) AS cnt FROM Flight", "cnt");
            assertEquals(800L, count);
        }
    }

    @Test
    void run02FleetSummaryFlightAirportConnections() throws SQLException {
        try (Connection connection = config.connect()) {
            long count = queryLong(connection,
                    "SELECT COUNT(*) AS cnt FROM Flight f NATURAL JOIN DEPARTS_FROM r NATURAL JOIN Airport a",
                    "cnt");
            assertEquals(800L, count);
        }
    }

    // validation/run_02 Section 4: Spark Connector label loads, expressed as SQL label reads.

    @Test
    void run02SparkConnectorMaintenanceEventLabelLoad() throws SQLException {
        try (Connection connection = config.connect()) {
            long count = queryLong(connection, "SELECT COUNT(*) AS cnt FROM MaintenanceEvent", "cnt");
            assertEquals(300L, count);
        }
    }

    @Test
    void run02SparkConnectorFlightLabelLoad() throws SQLException {
        try (Connection connection = config.connect()) {
            long count = queryLong(connection, "SELECT COUNT(*) AS cnt FROM Flight", "cnt");
            assertEquals(800L, count);
        }
    }

    // validation/run_02 Section 7: Fleet dashboard remote_query component.

    @Test
    void run02FleetDashboardDepartureTraversal() throws SQLException {
        try (Connection connection = config.connect()) {
            long count = queryLong(connection,
                    "SELECT COUNT(*) AS cnt FROM Flight f NATURAL JOIN DEPARTS_FROM r NATURAL JOIN Airport a",
                    "cnt");
            assertEquals(800L, count);
        }
    }

    // Additional direct-JDBC equivalents for Neo4j label aggregates used after Spark Connector loads in run_02.

    @Test
    void run02MaintenanceSummaryByAircraft() throws SQLException {
        try (Connection connection = config.connect();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("""
                        SELECT aircraftId,
                               COUNT(*) AS total_events,
                               SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) AS critical,
                               SUM(CASE WHEN severity = 'MAJOR' THEN 1 ELSE 0 END) AS major,
                               SUM(CASE WHEN severity = 'MINOR' THEN 1 ELSE 0 END) AS minor
                        FROM MaintenanceEvent
                        GROUP BY aircraftId
                        """)) {
            assertTrue(resultSet.next(), "Expected maintenance summary rows");
            assertTrue(resultSet.getLong("total_events") > 0L);
        }
    }

    @Test
    void run02FlightActivityByAircraft() throws SQLException {
        try (Connection connection = config.connect();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("""
                        SELECT aircraftId,
                               COUNT(*) AS total_flights,
                               COUNT(DISTINCT origin) AS unique_origins,
                               COUNT(DISTINCT destination) AS unique_destinations
                        FROM Flight
                        GROUP BY aircraftId
                        """)) {
            assertTrue(resultSet.next(), "Expected flight activity rows");
            assertTrue(resultSet.getLong("total_flights") > 0L);
        }
    }

    @Test
    void run02FleetDashboardMaintenanceAggregate() throws SQLException {
        try (Connection connection = config.connect();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("""
                        SELECT aircraftId,
                               COUNT(*) AS events,
                               SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) AS critical
                        FROM MaintenanceEvent
                        GROUP BY aircraftId
                        """)) {
            assertTrue(resultSet.next(), "Expected fleet dashboard maintenance rows");
            assertTrue(resultSet.getLong("events") > 0L);
        }
    }

    @Test
    void run02FleetDashboardFlightAggregate() throws SQLException {
        try (Connection connection = config.connect();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("""
                        SELECT aircraftId, COUNT(*) AS flight_count
                        FROM Flight
                        GROUP BY aircraftId
                        """)) {
            assertTrue(resultSet.next(), "Expected fleet dashboard flight rows");
            assertTrue(resultSet.getLong("flight_count") > 0L);
        }
    }

    private static int queryInt(Connection connection, String sql, String column) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            assertTrue(resultSet.next(), "Expected query to return one row: " + sql);
            return resultSet.getInt(column);
        }
    }

    private static long queryLong(Connection connection, String sql, String column) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            assertTrue(resultSet.next(), "Expected query to return one row: " + sql);
            return resultSet.getLong(column);
        }
    }
}
