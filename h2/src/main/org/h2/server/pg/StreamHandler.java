package org.h2.server.pg;

import com.opencsv.CSVReader;
import org.h2.command.CommandContainer;
import org.h2.command.query.Select;
import org.h2.expression.Expression;
import org.h2.expression.ValueExpression;
import org.h2.jdbc.JdbcPreparedStatement;
import org.h2.jdbc.JdbcResultSet;

import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class StreamHandler extends CustomHandler {
    public static String STREAM_SUFFIX = "_stream";

    private String tableName;

    private long cursor;

    public StreamHandler(PgServerThread pgServerThread) {
        super(pgServerThread);
    }

    @Override
    public boolean filter(int x) throws IOException {
        if (!isFilter) {
            return false;
        }
        boolean filtered = false;
        switch (x) {
            case 'E': {
                String name = pgServerThread.readString();
                pgServerThread.server.trace("Execute");
                PgServerThread.Portal p = pgServerThread.portals.get(name);
                if (p == null) {
                    pgServerThread.sendErrorResponse("Portal not found: " + name);
                    break;
                }
                PgServerThread.Prepared prepared = p.prep;
                JdbcPreparedStatement prep = prepared.prep;
                pgServerThread.server.trace(prepared.sql);
                //this is bug,ref "driver org.postgresql.core.v3.QueryExecutorImpl.sendExecute()"
//                int maxRows = readShort();
                int maxRows = pgServerThread.readInt();
                //bug fix end

                Expression limitExpression = ((Select) ((CommandContainer) prep.getCommand()).getPrepared()).getLimit();
                long limit = 0;
                if (limitExpression != null && limitExpression instanceof ValueExpression) {
                    limit = ((ValueExpression) limitExpression).getValue(null).getLong();
                }
                CSVReader reader = null;
                try {
                    prep.setMaxRows(1);//we only need rs.ResultSetMetaData
                    pgServerThread.setActiveRequest(prep);
                    prep.execute();
                    try {
                        JdbcResultSet rs = (JdbcResultSet) prep.getResultSet();
                        // the meta-data is sent in the prior 'Describe'
                        String fileName = tableName + ".csv";
                        reader = new CSVReader(new FileReader(fileName));
                        long resultRows = 0;
                        if (maxRows != 0 && limit != 0) {
                            resultRows = Math.min(maxRows, limit);
                        } else if (limit != 0) {
                            resultRows = limit;
                        } else if (maxRows != 0) {
                            resultRows = maxRows;
                        } else {
                            resultRows = Integer.MAX_VALUE;
                        }
                        String[] nextLine;
                        for (long i = cursor; i < resultRows; i++) {
                            nextLine = reader.readNext();
                            if (nextLine == null) {
                                reader.close();
                                reader = new CSVReader(new FileReader(fileName));
                                nextLine = reader.readNext();
                            }
                            sendDataRow(rs, p.resultColumnFormat, nextLine);
                        }
                        if(cursor>=resultRows) {
                            pgServerThread.sendCommandComplete(prep, 0);
                        }else {
                            pgServerThread.sendPortalSuspended();
                        }
                    } catch (Exception e) {
                        pgServerThread.sendErrorResponse(e);
                    }
                } catch (Exception e) {
                    if (prep.isCancelled()) {
                        pgServerThread.sendCancelQueryResponse();
                    } else {
                        pgServerThread.sendErrorResponse(e);
                    }
                } finally {
                    pgServerThread.setActiveRequest(null);
                    if (reader != null) {
                        reader.close();
                    }
                }
                filtered = true;
                break;
            }
        }
        return filtered;
    }

    @Override
    public void setIsFilter(JdbcPreparedStatement prep) {
        if (isSelectCommand(prep)) {
            String table = ((Select) ((CommandContainer) prep.getCommand()).getPrepared()).getTopTableFilter().getTable().getName();
            tableName = table;
            if (table.endsWith(StreamHandler.STREAM_SUFFIX)) {
                setFilter(true);
            } else {
                setFilter(false);
            }
        }
    }

    public boolean isSelectCommand(JdbcPreparedStatement prep) {
        return prep.getCommand() instanceof CommandContainer && ((CommandContainer) prep.getCommand()).getPrepared() instanceof Select;
    }

    public void sendDataRow(JdbcResultSet rs, int[] formatCodes, String[] nextLine) throws IOException, SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columns = metaData.getColumnCount();
        pgServerThread.startMessage('D');
        pgServerThread.writeShort(columns);
        for (int i = 1; i <= columns; i++) {
            int pgType = PgServer.convertType(metaData.getColumnType(i));
            boolean text = pgServerThread.formatAsText(pgType, formatCodes, i - 1);
            writeDataColumn(nextLine, i, pgType, text);
        }
        pgServerThread.sendMessage();
    }


    public void writeDataColumn(String[] nextline, int column, int pgType, boolean text) throws IOException {
        String v = nextline[column - 1];
        if (v == null || v.isEmpty()) {
            pgServerThread.writeInt(-1);
            return;
        }
        if (text) {
            // plain text
            switch (pgType) {
                case PgServer.PG_TYPE_BOOL:
                    pgServerThread.writeInt(1);
                    pgServerThread.dataOut.writeByte(v.equalsIgnoreCase("TRUE") ? 't' : 'f');
                    break;
                case PgServer.PG_TYPE_BYTEA: {
                    throw new IllegalStateException("unsupport data type PG_TYPE_BYTEA");
                }
                default:
                    byte[] data = v.getBytes(pgServerThread.getEncoding());
                    pgServerThread.writeInt(data.length);
                    pgServerThread.write(data);
            }
        } else {
            throw new IllegalStateException("unsupport data type");
        }
    }

}
