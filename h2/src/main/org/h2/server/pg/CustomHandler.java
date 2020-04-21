package org.h2.server.pg;

import org.h2.jdbc.JdbcPreparedStatement;

import java.io.IOException;

public abstract class CustomHandler {
    protected PgServerThread pgServerThread;

    protected boolean isFilter;

    public CustomHandler(PgServerThread pgServerThread) {
        this.pgServerThread = pgServerThread;
    }

    public boolean isFilter() {
        return isFilter;
    }

    public void setFilter(boolean filter) {
        isFilter = filter;
    }

    public abstract boolean filter(int x) throws IOException;

    public abstract void setIsFilter(JdbcPreparedStatement prep);
}
