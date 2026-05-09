package com.neo4j.uc.directjdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

final class JdbcTestSupport {

    private JdbcTestSupport() {
    }

    static long queryLong(Connection connection, String sql, String column) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            assertTrue(resultSet.next(), "Expected query to return one row: " + sql);
            return resultSet.getLong(column);
        }
    }

    static void assertQuerySucceeds(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet ignored = statement.executeQuery(sql)) {
            assertTrue(true);
        }
    }

    static void assertAtLeastOneRow(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            assertTrue(resultSet.next(), "Expected query to return rows: " + sql);
        }
    }

    static void assertGroupedCount(Connection connection, String sql, String countColumn, long expectedTotal,
            int minimumGroups) throws SQLException {
        long total = 0L;
        int groups = 0;
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                groups++;
                total += resultSet.getLong(countColumn);
            }
        }

        assertTrue(groups >= minimumGroups,
                "Expected at least " + minimumGroups + " groups, but found " + groups + ": " + sql);
        assertEquals(expectedTotal, total, "Grouped counts should add back to the expected total");
    }

    static void assertSortedDescending(Connection connection, String sql, String column) throws SQLException {
        boolean sawRow = false;
        long previous = Long.MAX_VALUE;
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                sawRow = true;
                long current = resultSet.getLong(column);
                assertTrue(current <= previous, "Expected " + column + " to be sorted descending");
                previous = current;
            }
        }
        assertTrue(sawRow, "Expected query to return rows: " + sql);
    }

    static void assertLimitedRows(Connection connection, String sql, int maximumRows) throws SQLException {
        int rows = 0;
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                rows++;
            }
        }
        assertTrue(rows <= maximumRows, "Expected at most " + maximumRows + " rows, but found " + rows);
        assertFalse(rows == 0, "Expected LIMIT/OFFSET query to return at least one row");
    }
}
