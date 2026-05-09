package com.neo4j.uc.directjdbc;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
final class DirectJdbcKnownLimitationsTest {

    private final DirectJdbcConfig config = DirectJdbcConfig.load();

    @Test
    void singleArgumentPercentileContFailsDirectly() throws SQLException {
        try (Connection connection = config.connect()) {
            assertThrows(SQLException.class, () -> execute(connection,
                    "SELECT cause, percentileCont(minutes) AS p50 FROM Delay GROUP BY cause"));
        }
    }

    @Test
    void singleArgumentPercentileDiscFailsDirectly() throws SQLException {
        try (Connection connection = config.connect()) {
            assertThrows(SQLException.class, () -> execute(connection,
                    "SELECT cause, percentileDisc(minutes) AS p50 FROM Delay GROUP BY cause"));
        }
    }

    @Test
    void arbitraryDerivedTableWithAggregateFailsDirectly() throws SQLException {
        try (Connection connection = config.connect()) {
            assertThrows(SQLException.class, () -> execute(connection, """
                    SELECT *
                    FROM (
                        SELECT severity, COUNT(*) AS cnt
                        FROM MaintenanceEvent
                        GROUP BY severity
                    ) t
                    """));
        }
    }

    @Test
    void unquotedLikeLiteralFailsDirectly() throws SQLException {
        try (Connection connection = config.connect()) {
            assertThrows(SQLException.class, () -> execute(connection, """
                    SELECT severity, COUNT(*) AS cnt
                    FROM MaintenanceEvent
                    WHERE aircraftId LIKE AC%
                    GROUP BY severity
                    """));
        }
    }

    private static void execute(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
