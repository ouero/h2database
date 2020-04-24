package org.h2.server.pg;

import org.h2.jdbc.JdbcPreparedStatement;
/**
 * Represents a PostgreSQL Prepared object.
 */
public class Prepared {
    /**
     * The object name.
     */
    String name;

    /**
     * The SQL statement.
     */
    String sql;

    /**
     * The prepared statement.
     */
    JdbcPreparedStatement prep;

    /**
     * The list of parameter types (if set).
     */
    int[] paramType;
}
