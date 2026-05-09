package com.neo4j.uc.directjdbc;

import static com.neo4j.uc.directjdbc.JdbcTestSupport.assertAtLeastOneRow;
import static com.neo4j.uc.directjdbc.JdbcTestSupport.assertGroupedCount;
import static com.neo4j.uc.directjdbc.JdbcTestSupport.assertLimitedRows;
import static com.neo4j.uc.directjdbc.JdbcTestSupport.assertQuerySucceeds;
import static com.neo4j.uc.directjdbc.JdbcTestSupport.assertSortedDescending;
import static com.neo4j.uc.directjdbc.JdbcTestSupport.queryLong;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
final class DirectJdbcAdvancedSqlTest {

    private final DirectJdbcConfig config = DirectJdbcConfig.load();

    // Advanced GROUP BY patterns executed directly through Neo4j JDBC without
    // Databricks remote_query().

    @Test
    void groupByProjectedKey() throws SQLException {
        try (Connection connection = config.connect()) {
            assertGroupedCount(connection,
                    "SELECT severity, COUNT(*) AS cnt FROM MaintenanceEvent GROUP BY severity",
                    "cnt",
                    300L,
                    3);
        }
    }

    @Test
    void groupByNonProjectedKey() throws SQLException {
        try (Connection connection = config.connect()) {
            assertGroupedCount(connection,
                    "SELECT COUNT(*) AS cnt FROM MaintenanceEvent GROUP BY severity",
                    "cnt",
                    300L,
                    3);
        }
    }

    @Test
    void groupByMultipleAggregates() throws SQLException {
        try (Connection connection = config.connect()) {
            assertAtLeastOneRow(connection, """
                    SELECT operator, COUNT(*) AS flights,
                           COUNT(DISTINCT origin) AS origins,
                           COUNT(DISTINCT destination) AS destinations
                    FROM Flight
                    GROUP BY operator
                    """);
        }
    }

    @Test
    void havingSimpleAlias() throws SQLException {
        try (Connection connection = config.connect()) {
            assertAtLeastOneRow(connection,
                    "SELECT operator, COUNT(*) AS cnt FROM Flight GROUP BY operator HAVING cnt > 20");
        }
    }

    @Test
    void havingNonProjectedAggregate() throws SQLException {
        try (Connection connection = config.connect()) {
            assertAtLeastOneRow(connection,
                    "SELECT severity FROM MaintenanceEvent GROUP BY severity HAVING COUNT(*) > 10");
        }
    }

    @Test
    void havingCompoundCondition() throws SQLException {
        try (Connection connection = config.connect()) {
            assertAtLeastOneRow(connection, """
                    SELECT operator, COUNT(*) AS cnt
                    FROM Flight
                    GROUP BY operator
                    HAVING COUNT(*) > 10 AND COUNT(DISTINCT origin) > 2
                    """);
        }
    }

    @Test
    void orderByAggregateAlias() throws SQLException {
        try (Connection connection = config.connect()) {
            assertSortedDescending(connection,
                    "SELECT severity, COUNT(*) AS cnt FROM MaintenanceEvent GROUP BY severity ORDER BY cnt DESC",
                    "cnt");
        }
    }

    @Test
    void orderByMultiKey() throws SQLException {
        try (Connection connection = config.connect()) {
            assertAtLeastOneRow(connection, """
                    SELECT operator, COUNT(*) AS cnt, COUNT(DISTINCT origin) AS routes
                    FROM Flight
                    GROUP BY operator
                    ORDER BY cnt DESC, routes
                    """);
        }
    }

    @Test
    void distinctGroupBy() throws SQLException {
        try (Connection connection = config.connect()) {
            assertAtLeastOneRow(connection,
                    "SELECT DISTINCT operator, COUNT(*) AS cnt FROM Flight GROUP BY operator");
        }
    }

    @Test
    void limitOffset() throws SQLException {
        try (Connection connection = config.connect()) {
            assertLimitedRows(connection, """
                    SELECT operator, COUNT(*) AS cnt
                    FROM Flight
                    GROUP BY operator
                    ORDER BY cnt DESC
                    LIMIT 3 OFFSET 1
                    """, 3);
        }
    }

    @Test
    void allClausesCombined() throws SQLException {
        try (Connection connection = config.connect()) {
            assertLimitedRows(connection, """
                    SELECT DISTINCT severity, COUNT(*) AS cnt, MAX(fault) AS last_fault
                    FROM MaintenanceEvent
                    WHERE severity IS NOT NULL
                    GROUP BY severity
                    HAVING COUNT(*) > 1
                    ORDER BY cnt DESC
                    LIMIT 10 OFFSET 0
                    """, 10);
        }
    }

    @Test
    void likeLiteralRemainsQuotedWhenCalledDirectly() throws SQLException {
        try (Connection connection = config.connect()) {
            assertAtLeastOneRow(connection, """
                    SELECT severity, COUNT(*) AS cnt
                    FROM MaintenanceEvent
                    WHERE aircraftId LIKE 'AC%'
                    GROUP BY severity
                    HAVING COUNT(*) > 1
                    ORDER BY cnt DESC
                    """);
        }
    }

    @Test
    void federatedGroupByNeo4jSide() throws SQLException {
        try (Connection connection = config.connect()) {
            assertGroupedCount(connection,
                    "SELECT aircraftId, COUNT(*) AS maint_count FROM MaintenanceEvent GROUP BY aircraftId",
                    "maint_count",
                    300L,
                    20);
        }
    }

    @Test
    void federatedHavingNeo4jSide() throws SQLException {
        try (Connection connection = config.connect()) {
            assertAtLeastOneRow(connection, """
                    SELECT operator, COUNT(*) AS flight_count
                    FROM Flight
                    GROUP BY operator
                    HAVING COUNT(*) > 20
                    """);
        }
    }

    @Test
    void run06MaintenanceCountsByAircraft() throws SQLException {
        try (Connection connection = config.connect()) {
            assertGroupedCount(connection,
                    "SELECT aircraftId, COUNT(*) AS total_events FROM MaintenanceEvent GROUP BY aircraftId",
                    "total_events",
                    300L,
                    20);
        }
    }

    @Test
    void run06CriticalMaintenanceCountsByAircraft() throws SQLException {
        try (Connection connection = config.connect()) {
            long totalCritical = queryLong(connection,
                    "SELECT COUNT(*) AS cnt FROM MaintenanceEvent WHERE severity = 'CRITICAL'",
                    "cnt");
            assertTrue(totalCritical > 0L, "Expected critical maintenance events");

            assertGroupedCount(connection, """
                    SELECT aircraftId, COUNT(*) AS critical
                    FROM MaintenanceEvent
                    WHERE severity = 'CRITICAL'
                    GROUP BY aircraftId
                    """, "critical", totalCritical, 1);
        }
    }

    @Test
    void joinGroupByNonProjectedReturnsMultipleAirportGroups() throws SQLException {
        try (Connection connection = config.connect()) {
            assertGroupedCount(connection, """
                    SELECT COUNT(*) AS flight_count
                    FROM Flight f
                    NATURAL JOIN DEPARTS_FROM r
                    NATURAL JOIN Airport a
                    GROUP BY a.iata
                    """, "flight_count", 800L, 2);
        }
    }

    @Test
    void joinGroupByProjectedReturnsMultipleAirportGroups() throws SQLException {
        try (Connection connection = config.connect()) {
            assertGroupedCount(connection, """
                    SELECT a.iata, COUNT(*) AS flight_count
                    FROM Flight f
                    NATURAL JOIN DEPARTS_FROM r
                    NATURAL JOIN Airport a
                    GROUP BY a.iata
                    """, "flight_count", 800L, 2);
        }
    }

    @Test
    void havingWithoutGroupBy() throws SQLException {
        try (Connection connection = config.connect()) {
            assertAtLeastOneRow(connection,
                    "SELECT COUNT(*) AS cnt FROM MaintenanceEvent HAVING COUNT(*) > 5");
        }
    }

    @Test
    void whereGroupByComparison() throws SQLException {
        try (Connection connection = config.connect()) {
            assertAtLeastOneRow(connection, """
                    SELECT cause, COUNT(*) AS cnt
                    FROM Delay
                    WHERE minutes > 30
                    GROUP BY cause
                    ORDER BY cnt DESC
                    """);
        }
    }

    @Test
    void whereGroupByIsNotNull() throws SQLException {
        try (Connection connection = config.connect()) {
            assertAtLeastOneRow(connection, """
                    SELECT cause, COUNT(*) AS cnt
                    FROM Delay
                    WHERE cause IS NOT NULL
                    GROUP BY cause
                    """);
        }
    }

    @Test
    void stDevAggregate() throws SQLException {
        try (Connection connection = config.connect()) {
            assertAtLeastOneRow(connection,
                    "SELECT cause, stDev(minutes) AS stddev_minutes FROM Delay GROUP BY cause");
        }
    }

    @Test
    void stDevPAggregate() throws SQLException {
        try (Connection connection = config.connect()) {
            assertAtLeastOneRow(connection,
                    "SELECT cause, stDevP(minutes) AS stddevp_minutes FROM Delay GROUP BY cause");
        }
    }

    @Test
    void sparkProbeWrapperForGroupByProjectedKey() throws SQLException {
        try (Connection connection = config.connect()) {
            assertQuerySucceeds(connection, """
                    SELECT *
                    FROM (
                        SELECT severity, COUNT(*) AS cnt
                        FROM MaintenanceEvent
                        GROUP BY severity
                    ) SPARK_GEN_SUBQ_0
                    WHERE 1=0
                    """);
        }
    }

    @Test
    void sparkProbeWrapperForHavingOrderLimitOffset() throws SQLException {
        try (Connection connection = config.connect()) {
            assertQuerySucceeds(connection, """
                    SELECT *
                    FROM (
                        SELECT severity, COUNT(*) AS cnt
                        FROM MaintenanceEvent
                        GROUP BY severity
                        HAVING COUNT(*) > 5
                        ORDER BY cnt DESC
                        LIMIT 10 OFFSET 0
                    ) SPARK_GEN_SUBQ_0
                    WHERE 1=0
                    """);
        }
    }

    @Test
    void sparkProbeWrapperForJoinGroupByProjectedKey() throws SQLException {
        try (Connection connection = config.connect()) {
            assertQuerySucceeds(connection, """
                    SELECT *
                    FROM (
                        SELECT a.iata, COUNT(*) AS flight_count
                        FROM Flight f
                        NATURAL JOIN DEPARTS_FROM r
                        NATURAL JOIN Airport a
                        GROUP BY a.iata
                    ) SPARK_GEN_SUBQ_0
                    WHERE 1=0
                    """);
        }
    }
}
