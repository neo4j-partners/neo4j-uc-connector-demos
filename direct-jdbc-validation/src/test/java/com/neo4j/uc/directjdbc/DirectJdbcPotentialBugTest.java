package com.neo4j.uc.directjdbc;

import static com.neo4j.uc.directjdbc.JdbcTestSupport.assertAtLeastOneRow;
import static com.neo4j.uc.directjdbc.JdbcTestSupport.assertGroupedCount;
import static com.neo4j.uc.directjdbc.JdbcTestSupport.assertLimitedRows;
import static com.neo4j.uc.directjdbc.JdbcTestSupport.assertQuerySucceeds;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
final class DirectJdbcPotentialBugTest {

    private final DirectJdbcConfig config = DirectJdbcConfig.load();

    @Test
    void snakeCaseAircraftProjectionReturnsRealValues() throws SQLException {
        try (Connection connection = config.connect();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("""
                        SELECT aircraft_id, tail_number
                        FROM Aircraft
                        """)) {
            boolean foundExpectedAircraft = false;
            while (resultSet.next()) {
                String aircraftId = resultSet.getString("aircraft_id");
                if ("AC1001".equals(aircraftId)) {
                    foundExpectedAircraft = true;
                    assertEquals("N95040A", resultSet.getString("tail_number"));
                }
            }
            assertTrue(foundExpectedAircraft, "Expected snake_case aircraft_id projection to include AC1001");
        }
    }

    @Test
    void snakeCaseAircraftAggregatesReturnRealValues() throws SQLException {
        try (Connection connection = config.connect();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("""
                        SELECT MIN(aircraft_id) AS first_id,
                               MAX(aircraft_id) AS last_id
                        FROM Aircraft
                        """)) {
            assertTrue(resultSet.next());
            assertEquals("AC1001", resultSet.getString("first_id"));
            assertEquals("AC1020", resultSet.getString("last_id"));
        }
    }

    @Test
    void snakeCaseMaintenanceProjectionReturnsRealValues() throws SQLException {
        try (Connection connection = config.connect();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("""
                        SELECT aircraft_id, reported_at, corrective_action
                        FROM MaintenanceEvent
                        """)) {
            assertTrue(resultSet.next(), "Expected maintenance rows");
            String aircraftId = resultSet.getString("aircraft_id");
            String reportedAt = resultSet.getString("reported_at");
            String correctiveAction = resultSet.getString("corrective_action");
            assertNotNull(aircraftId, "Expected snake_case aircraft_id projection to return a value");
            assertNotNull(reportedAt, "Expected reported_at projection to return a value");
            assertNotNull(correctiveAction, "Expected corrective_action projection to return a value");
            assertFalse(aircraftId.isBlank());
            assertTrue(reportedAt.contains("T"));
            assertFalse(correctiveAction.isBlank());
        }
    }

    @Test
    void snakeCaseMaintenanceWherePredicateReturnsRows() throws SQLException {
        try (Connection connection = config.connect()) {
            assertAtLeastOneRow(connection, """
                    SELECT aircraft_id, severity, fault
                    FROM MaintenanceEvent
                    WHERE aircraft_id = 'AC1002'
                    """);
        }
    }

    @Test
    void projectedMultiHopNaturalJoinReturnsTopologyRows() throws SQLException {
        try (Connection connection = config.connect();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("""
                        SELECT a.aircraftId AS aircraftId,
                               sys.systemId AS systemId,
                               s.sensorId AS sensorId
                        FROM Aircraft a
                        NATURAL JOIN HAS_SYSTEM r1
                        NATURAL JOIN System sys
                        NATURAL JOIN HAS_SENSOR r2
                        NATURAL JOIN Sensor s
                        """)) {
            int rows = 0;
            while (resultSet.next()) {
                rows++;
                assertFalse(resultSet.getString("aircraftId").isBlank());
                assertFalse(resultSet.getString("systemId").isBlank());
                assertFalse(resultSet.getString("sensorId").isBlank());
            }
            assertEquals(160, rows);
        }
    }

    @Test
    void multiColumnGroupByOnMaintenanceEvents() throws SQLException {
        try (Connection connection = config.connect()) {
            assertGroupedCount(connection, """
                    SELECT aircraftId, severity, COUNT(*) AS cnt
                    FROM MaintenanceEvent
                    GROUP BY aircraftId, severity
                    """, "cnt", 300L, 20);
        }
    }

    @Test
    void multiColumnGroupByOnNaturalJoin() throws SQLException {
        try (Connection connection = config.connect()) {
            assertGroupedCount(connection, """
                    SELECT a.aircraftId, a.model, sys.type, COUNT(*) AS cnt
                    FROM Aircraft a
                    NATURAL JOIN HAS_SYSTEM rel
                    NATURAL JOIN System sys
                    GROUP BY a.aircraftId, a.model, sys.type
                    """, "cnt", 80L, 40);
        }
    }

    @Test
    void nonAggregateWhereOrderByLimitReturnsSortedRows() throws SQLException {
        try (Connection connection = config.connect();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("""
                        SELECT eventId, aircraftId, reported_at
                        FROM MaintenanceEvent
                        WHERE severity = 'CRITICAL'
                        ORDER BY reported_at DESC
                        LIMIT 10
                        """)) {
            int rows = 0;
            String previous = null;
            while (resultSet.next()) {
                rows++;
                String current = resultSet.getString("reported_at");
                assertFalse(resultSet.getString("eventId").isBlank());
                assertFalse(resultSet.getString("aircraftId").isBlank());
                if (previous != null) {
                    assertTrue(previous.compareTo(current) >= 0, "Expected reported_at to be sorted descending");
                }
                previous = current;
            }
            assertTrue(rows > 0 && rows <= 10, "Expected 1-10 critical maintenance rows");
        }
    }

    @Test
    void havingOrderByLimitOffsetIsolated() throws SQLException {
        try (Connection connection = config.connect()) {
            assertLimitedRows(connection, """
                    SELECT severity, COUNT(*) AS cnt
                    FROM MaintenanceEvent
                    GROUP BY severity
                    HAVING COUNT(*) > 5
                    ORDER BY cnt DESC
                    LIMIT 10 OFFSET 0
                    """, 10);
        }
    }

    @Test
    void numericAggregatesOverDelayMinutes() throws SQLException {
        try (Connection connection = config.connect();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("""
                        SELECT cause,
                               AVG(minutes) AS avg_minutes,
                               SUM(minutes) AS total_minutes,
                               MIN(minutes) AS min_minutes,
                               MAX(minutes) AS max_minutes
                        FROM Delay
                        GROUP BY cause
                        """)) {
            assertTrue(resultSet.next(), "Expected delay cause groups");
            assertFalse(resultSet.getString("cause").isBlank());
            assertTrue(resultSet.getDouble("avg_minutes") > 0.0d);
            assertTrue(resultSet.getLong("total_minutes") > 0L);
            assertTrue(resultSet.getLong("max_minutes") >= resultSet.getLong("min_minutes"));
        }
    }

    @Test
    void caseExpressionInsideAggregates() throws SQLException {
        try (Connection connection = config.connect();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("""
                        SELECT aircraftId,
                               COUNT(*) AS total_events,
                               SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) AS critical
                        FROM MaintenanceEvent
                        GROUP BY aircraftId
                        """)) {
            long totalEvents = 0L;
            long criticalEvents = 0L;
            int aircraft = 0;
            while (resultSet.next()) {
                aircraft++;
                long total = resultSet.getLong("total_events");
                long critical = resultSet.getLong("critical");
                assertTrue(total > 0L);
                assertTrue(critical >= 0L && critical <= total);
                totalEvents += total;
                criticalEvents += critical;
            }
            assertEquals(20, aircraft);
            assertEquals(300L, totalEvents);
            assertTrue(criticalEvents > 0L);
        }
    }

    @Test
    void sparkProbeWrapperForHavingOnly() throws SQLException {
        try (Connection connection = config.connect()) {
            assertQuerySucceeds(connection, """
                    SELECT *
                    FROM (
                        SELECT operator, COUNT(*) AS cnt
                        FROM Flight
                        GROUP BY operator
                        HAVING COUNT(*) > 20
                    ) SPARK_GEN_SUBQ_0
                    WHERE 1=0
                    """);
        }
    }

    @Test
    void sparkProbeWrapperForDistinctGroupBy() throws SQLException {
        try (Connection connection = config.connect()) {
            assertQuerySucceeds(connection, """
                    SELECT *
                    FROM (
                        SELECT DISTINCT operator, COUNT(*) AS cnt
                        FROM Flight
                        GROUP BY operator
                    ) SPARK_GEN_SUBQ_0
                    WHERE 1=0
                    """);
        }
    }

    @Test
    void sparkProbeWrapperForLimitOffset() throws SQLException {
        try (Connection connection = config.connect()) {
            assertQuerySucceeds(connection, """
                    SELECT *
                    FROM (
                        SELECT operator, COUNT(*) AS cnt
                        FROM Flight
                        GROUP BY operator
                        ORDER BY cnt DESC
                        LIMIT 3 OFFSET 1
                    ) SPARK_GEN_SUBQ_0
                    WHERE 1=0
                    """);
        }
    }

    @Test
    void sparkProbeWrapperForMultiKeyGroupBy() throws SQLException {
        try (Connection connection = config.connect()) {
            assertQuerySucceeds(connection, """
                    SELECT *
                    FROM (
                        SELECT aircraftId, severity, COUNT(*) AS cnt
                        FROM MaintenanceEvent
                        GROUP BY aircraftId, severity
                    ) SPARK_GEN_SUBQ_0
                    WHERE 1=0
                    """);
        }
    }

    @Test
    void sparkProbeWrapperForNonAggregateSelect() throws SQLException {
        try (Connection connection = config.connect()) {
            assertQuerySucceeds(connection, """
                    SELECT *
                    FROM (
                        SELECT aircraftId, severity, fault
                        FROM MaintenanceEvent
                    ) SPARK_GEN_SUBQ_0
                    WHERE 1=0
                    """);
        }
    }
}
