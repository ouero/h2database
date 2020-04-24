package org.h2.server.pg;

/**
 * Represents a PostgreSQL Portal object.
 */
public class Portal {

    /**
     * The portal name.
     */
    String name;

    /**
     * The format used in the result set columns (if set).
     */
    int[] resultColumnFormat;

    /**
     * The prepared object.
     */
    Prepared prep;
}
