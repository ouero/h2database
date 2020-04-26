/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.pg;

import org.h2.command.CommandInterface;
import org.h2.engine.ConnectionInfo;
import org.h2.engine.Constants;
import org.h2.engine.SysProperties;
import org.h2.jdbc.JdbcConnection;
import org.h2.jdbc.JdbcPreparedStatement;
import org.h2.jdbc.JdbcResultSet;
import org.h2.jdbc.JdbcStatement;
import org.h2.message.DbException;
import org.h2.util.*;
import org.h2.value.*;

import java.io.*;
import java.net.Socket;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

/**
 * One server thread is opened for each client.
 */
public class PgServerThread implements Runnable, ReadWriteAble {
    private static final boolean INTEGER_DATE_TYPES = false;

    public final PgServer server;
    private Socket socket;
    private Connection conn;
    private boolean stop;
    private DataInputStream dataInRaw;
    private DataInputStream dataIn;
    private OutputStream out;
    private int messageType;
    private ByteArrayOutputStream outBuffer;
    public DataOutputStream dataOut;
    private Thread thread;
    private boolean initDone;
    private String userName;
    private String databaseName;
    private int processId;
    private final int secret;
    private JdbcStatement activeRequest;
    private String clientEncoding = SysProperties.PG_DEFAULT_CLIENT_ENCODING;
    private String dateStyle = "ISO, MDY";
    private final HashMap<String, Prepared> prepared =
            new CaseInsensitiveMap<>();
    public final HashMap<String, Portal> portals =
            new CaseInsensitiveMap<>();
    private BlackHoleHandler blackHoleHandler = new BlackHoleHandler(this);
    private StreamHandler streamHandler = new StreamHandler(this);


    PgServerThread(Socket socket, PgServer server) {
        this.server = server;
        this.socket = socket;
        this.secret = (int) MathUtils.secureRandomLong();
    }

    @Override
    public void run() {
        try {
            server.trace("Connect");
            InputStream ins = socket.getInputStream();
            out = socket.getOutputStream();
            dataInRaw = new DataInputStream(ins);
            while (!stop) {
                process();
                out.flush();
            }
        } catch (EOFException e) {
            // more or less normal disconnect
        } catch (Exception e) {
            server.traceError(e);
        } finally {
            server.trace("Disconnect");
            close();
        }
    }

    public String readString() throws IOException {
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        while (true) {
            int x = dataIn.read();
            if (x <= 0) {
                break;
            }
            buff.write(x);
        }
        return new String(buff.toByteArray(), getEncoding());
    }

    public int readInt() throws IOException {
        return dataIn.readInt();
    }

    private short readShort() throws IOException {
        return dataIn.readShort();
    }

    private byte readByte() throws IOException {
        return dataIn.readByte();
    }

    private void readFully(byte[] buff) throws IOException {
        dataIn.readFully(buff);
    }

    private void process() throws IOException {
        int x;
        if (initDone) {
            x = dataInRaw.read();//read the head,this is the command type
//            System.out.println(LocalDateTime.now().toString() + " " + (char) x);
            if (x < 0) {
                stop = true;
                return;
            }
        } else {
            x = 0;
        }
        int len = dataInRaw.readInt();//get the data package length
        len -= 4;
        byte[] data = Utils.newBytes(len);
        dataInRaw.readFully(data, 0, len);//read the data package
        if (blackHoleHandler.filter(x)) {
            return;
        }
        dataIn = new DataInputStream(new ByteArrayInputStream(data, 0, len));//copy data to new stream for read
        if (streamHandler.filter(x)) {
            return;
        }
        switch (x) {
            case 0://init
                server.trace("Init");
                int version = readInt();
                if (version == 80877102) {
                    server.trace("CancelRequest");
                    int pid = readInt();
                    int key = readInt();
                    PgServerThread c = server.getThread(pid);
                    if (c != null && key == c.secret) {
                        c.cancelRequest();
                    } else {
                        // According to the PostgreSQL documentation, when canceling
                        // a request, if an invalid secret is provided then no
                        // exception should be sent back to the client.
                        server.trace("Invalid CancelRequest: pid=" + pid + ", key=" + key);
                    }
                    close();
                } else if (version == 80877103) {
                    server.trace("SSLRequest");
                    out.write('N');
                } else {
                    server.trace("StartupMessage");
                    server.trace(" version " + version +
                            " (" + (version >> 16) + "." + (version & 0xff) + ")");
                    while (true) {
                        String param = readString();
                        if (param.isEmpty()) {
                            break;
                        }
                        String value = readString();
                        if ("user".equals(param)) {
                            this.userName = value;
                        } else if ("database".equals(param)) {
                            this.databaseName = server.checkKeyAndGetDatabaseName(value);
                        } else if ("client_encoding".equals(param)) {
                            // node-postgres will send "'utf-8'"
                            int length = value.length();
                            if (length >= 2 && value.charAt(0) == '\''
                                    && value.charAt(length - 1) == '\'') {
                                value = value.substring(1, length - 1);
                            }
                            // UTF8
                            clientEncoding = value;
                        } else if ("DateStyle".equals(param)) {
                            if (value.indexOf(',') < 0) {
                                value += ", MDY";
                            }
                            dateStyle = value;
                        }
                        // extra_float_digits 2
                        // geqo on (Genetic Query Optimization)
                        server.trace(" param " + param + "=" + value);
                    }
                    sendAuthenticationCleartextPassword();
                    initDone = true;
                }
                break;
            case 'p': {//check pawword
                server.trace("PasswordMessage");
                String password = readString();
                try {
                    Properties info = new Properties();
                    info.put("MODE", "PostgreSQL");
                    info.put("DATABASE_TO_LOWER", "TRUE");
                    info.put("USER", userName);
                    info.put("PASSWORD", password);
                    String url = "jdbc:h2:" + databaseName;
                    ConnectionInfo ci = new ConnectionInfo(url, info);
                    String baseDir = server.getBaseDir();
                    if (baseDir == null) {
                        baseDir = SysProperties.getBaseDir();
                    }
                    if (baseDir != null) {
                        ci.setBaseDir(baseDir);
                    }
                    if (server.getIfExists()) {
                        ci.setProperty("FORBID_CREATION", "TRUE");
                    }
                    ci.setNetworkConnectionInfo(new NetworkConnectionInfo( //
                            NetUtils.ipToShortForm(new StringBuilder("pg://"), //
                                    socket.getLocalAddress().getAddress(), true) //
                                    .append(':').append(socket.getLocalPort()).toString(), //
                            socket.getInetAddress().getAddress(), socket.getPort(), null));
                    conn = new JdbcConnection(ci, false);
                    // can not do this because when called inside
                    // DriverManager.getConnection, a deadlock occurs
                    // conn = DriverManager.getConnection(url, userName, password);
                    initDb();
                    sendAuthenticationOk();
                } catch (Exception e) {
                    e.printStackTrace();
                    stop = true;
                }
                break;
            }
            case 'P': {//
                server.trace("Parse");
                Prepared p = new Prepared();
                p.name = readString();
                p.sql = getSQL(readString());
                int paramTypesCount = readShort();
                int[] paramTypes = null;
                if (paramTypesCount > 0) {
                    paramTypes = new int[paramTypesCount];
                    for (int i = 0; i < paramTypesCount; i++) {
                        paramTypes[i] = readInt();
                    }
                }
                try {
                    p.prep = (JdbcPreparedStatement) conn.prepareStatement(p.sql);
                    //add is black hole
                    blackHoleHandler.setIsFilter(p.prep);
                    //add the stream
                    streamHandler.setIsFilter(p.prep);

                    ParameterMetaData meta = p.prep.getParameterMetaData();
                    p.paramType = new int[meta.getParameterCount()];
                    for (int i = 0; i < p.paramType.length; i++) {
                        int type;
                        if (i < paramTypesCount && paramTypes[i] != 0) {
                            type = paramTypes[i];
                            server.checkType(type);
                        } else {
                            type = PgServer.convertType(meta.getParameterType(i + 1));
                        }
                        p.paramType[i] = type;
                    }
                    prepared.put(p.name, p);
                    sendParseComplete();
                } catch (Exception e) {
                    sendErrorResponse(e);
                }
                break;
            }
            case 'B': {
                server.trace("Bind");
                Portal portal = new Portal();
                portal.name = readString();
                String prepName = readString();
                Prepared prep = prepared.get(prepName);
                if (prep == null) {
                    sendErrorResponse("Prepared not found");
                    break;
                }
                portal.prep = prep;
                portals.put(portal.name, portal);
                int formatCodeCount = readShort();
                int[] formatCodes = new int[formatCodeCount];
                for (int i = 0; i < formatCodeCount; i++) {
                    formatCodes[i] = readShort();
                }
                int paramCount = readShort();
                try {
                    for (int i = 0; i < paramCount; i++) {
                        setParameter(prep.prep, prep.paramType[i], i, formatCodes);
                    }
                } catch (Exception e) {
                    sendErrorResponse(e);
                    break;
                }
                int resultCodeCount = readShort();
                portal.resultColumnFormat = new int[resultCodeCount];
                for (int i = 0; i < resultCodeCount; i++) {
                    portal.resultColumnFormat[i] = readShort();
                }
                sendBindComplete();
                break;
            }
            case 'C': {
                char type = (char) readByte();
                String name = readString();
                server.trace("Close");
                if (type == 'S') {
                    Prepared p = prepared.remove(name);
                    if (p != null) {
                        JdbcUtils.closeSilently(p.prep);
                    }
                } else if (type == 'P') {
                    portals.remove(name);
                } else {
                    server.trace("expected S or P, got " + type);
                    sendErrorResponse("expected S or P");
                    break;
                }
                sendCloseComplete();
                break;
            }
            case 'D': {
                char type = (char) readByte();
                String name = readString();
                server.trace("Describe");
                if (type == 'S') {
                    Prepared p = prepared.get(name);
                    if (p == null) {
                        sendErrorResponse("Prepared not found: " + name);
                    } else {
                        try {
                            sendParameterDescription(p.prep.getParameterMetaData(), p.paramType);
                            sendRowDescription(p.prep.getMetaData(), null);
                        } catch (Exception e) {
                            sendErrorResponse(e);
                        }
                    }
                } else if (type == 'P') {
                    Portal p = portals.get(name);
                    if (p == null) {
                        sendErrorResponse("Portal not found: " + name);
                    } else {
                        PreparedStatement prep = p.prep.prep;
                        try {
                            ResultSetMetaData meta = prep.getMetaData();
                            sendRowDescription(meta, p.resultColumnFormat);
                        } catch (Exception e) {
                            sendErrorResponse(e);
                        }
                    }
                } else {
                    server.trace("expected S or P, got " + type);
                    sendErrorResponse("expected S or P");
                }
                break;
            }
            case 'E': {
                String name = readString();
                server.trace("Execute");
                Portal p = portals.get(name);
                if (p == null) {
                    sendErrorResponse("Portal not found: " + name);
                    break;
                }
                Prepared prepared = p.prep;
                JdbcPreparedStatement prep = prepared.prep;
                server.trace(prepared.sql);
                //this is bug,ref "driver org.postgresql.core.v3.QueryExecutorImpl.sendExecute()"
//                int maxRows = readShort();
                int maxRows = readInt();
                //bug fix end
                try {
                    prep.setMaxRows(maxRows);
                    setActiveRequest(prep);
                    boolean result = prep.execute();
                    if (result) {
                        try {
                            JdbcResultSet rs = (JdbcResultSet) prep.getResultSet();
                            // the meta-data is sent in the prior 'Describe'
                            while (rs.next()) {
                                sendDataRow(rs, p.resultColumnFormat);
                            }
                            sendCommandComplete(prep, 0);
                        } catch (Exception e) {
                            sendErrorResponse(e);
                        }
                    } else {
                        sendCommandComplete(prep, prep.getUpdateCount());
                    }

                } catch (Exception e) {
                    if (prep.isCancelled()) {
                        sendCancelQueryResponse();
                    } else {
                        sendErrorResponse(e);
                    }
                } finally {
                    setActiveRequest(null);
                }
                break;
            }
            case 'S': {
                server.trace("Sync");
                sendReadyForQuery();
                break;
            }
            case 'Q': {
                server.trace("Query");
                String query = readString();
                ScriptReader reader = new ScriptReader(new StringReader(query));
                while (true) {
                    JdbcStatement stat = null;
                    try {
                        String s = reader.readStatement();
                        if (s == null) {
                            break;
                        }
                        s = getSQL(s);
                        stat = (JdbcStatement) conn.createStatement();
                        setActiveRequest(stat);
                        boolean result = stat.execute(s);
                        if (result) {
                            JdbcResultSet rs = (JdbcResultSet) stat.getResultSet();
                            ResultSetMetaData meta = rs.getMetaData();
                            try {
                                sendRowDescription(meta, null);
                                while (rs.next()) {
                                    sendDataRow(rs, null);
                                }
                                sendCommandComplete(stat, 0);
                            } catch (Exception e) {
                                sendErrorResponse(e);
                                break;
                            }
                        } else {
                            sendCommandComplete(stat, stat.getUpdateCount());
                        }
                    } catch (SQLException e) {
                        if (stat != null && stat.isCancelled()) {
                            sendCancelQueryResponse();
                        } else {
                            sendErrorResponse(e);
                        }
                        break;
                    } finally {
                        JdbcUtils.closeSilently(stat);
                        setActiveRequest(null);
                    }
                }
                sendReadyForQuery();
                break;
            }
            case 'X': {
                server.trace("Terminate");
                close();
                break;
            }
            default:
                server.trace("Unsupported: " + x + " (" + (char) x + ")");
                break;
        }
    }


    private String getSQL(String s) {
        String lower = StringUtils.toLowerEnglish(s);
        if (lower.startsWith("show max_identifier_length")) {
            s = "CALL 63";
        } else if (lower.startsWith("set client_encoding to")) {
            s = "set DATESTYLE ISO";
        }
        // s = StringUtils.replaceAll(s, "i.indkey[ia.attnum-1]", "0");
        if (server.getTrace()) {
            server.trace(s + ";");
        }
        return s;
    }

    public void sendCommandComplete(JdbcStatement stat, int updateCount)
            throws IOException {
        startMessage('C');
        switch (stat.getLastExecutedCommandType()) {
            case CommandInterface.INSERT:
                writeStringPart("INSERT 0 ");
                writeString(Integer.toString(updateCount));
                break;
            case CommandInterface.UPDATE:
                writeStringPart("UPDATE ");
                writeString(Integer.toString(updateCount));
                break;
            case CommandInterface.DELETE:
                writeStringPart("DELETE ");
                writeString(Integer.toString(updateCount));
                break;
            case CommandInterface.SELECT:
            case CommandInterface.CALL:
                writeString("SELECT");
                break;
            case CommandInterface.BEGIN:
                writeString("BEGIN");
                break;
            default:
                server.trace("check CommandComplete tag for command " + stat);
                writeStringPart("UPDATE ");
                writeString(Integer.toString(updateCount));
        }
        sendMessage();
    }

    public void sendCommandComplete(int commandType, int updateCount)
            throws IOException {
        startMessage('C');
        switch (commandType) {
            case CommandInterface.INSERT:
                writeStringPart("INSERT 0 ");
                writeString(Integer.toString(updateCount));
                break;
            case CommandInterface.UPDATE:
                writeStringPart("UPDATE ");
                writeString(Integer.toString(updateCount));
                break;
            case CommandInterface.DELETE:
                writeStringPart("DELETE ");
                writeString(Integer.toString(updateCount));
                break;
            case CommandInterface.SELECT:
            case CommandInterface.CALL:
                writeString("SELECT");
                break;
            case CommandInterface.BEGIN:
                writeString("BEGIN");
                break;
            default:
                server.trace("check CommandComplete tag for command " + commandType);
                writeStringPart("UPDATE ");
                writeString(Integer.toString(updateCount));
        }
        sendMessage();
    }

    public void sendPortalSuspended() throws IOException {
        startMessage('s');
        sendMessage();
    }

    public void sendDataRow(JdbcResultSet rs, int[] formatCodes) throws IOException, SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columns = metaData.getColumnCount();
        startMessage('D');
        writeShort(columns);
        for (int i = 1; i <= columns; i++) {
            int pgType = PgServer.convertType(metaData.getColumnType(i));
            boolean text = formatAsText(pgType, formatCodes, i - 1);
            writeDataColumn(rs, i, pgType, text);
        }
        sendMessage();
    }

    public static long toPostgreDays(long dateValue) {
        return DateTimeUtils.absoluteDayFromDateValue(dateValue) - 10_957;
    }

    public void writeDataColumn(JdbcResultSet rs, int column, int pgType, boolean text) throws IOException {
        Value v = rs.get(column);
        writeDataColumn(v, column, pgType, text);
    }

    public void writeDataColumn(Value v, int column, int pgType, boolean text) throws IOException {
        if (v == ValueNull.INSTANCE) {
            writeInt(-1);
            return;
        }
        if (text) {
            // plain text
            switch (pgType) {
                case PgServer.PG_TYPE_BOOL:
                    writeInt(1);
                    writeByte(v.getBoolean() ? 't' : 'f');
                    break;
                case PgServer.PG_TYPE_BYTEA: {
                    byte[] bytes = v.getBytesNoCopy();
                    int length = bytes.length;
                    int cnt = length;
                    for (int i = 0; i < length; i++) {
                        byte b = bytes[i];
                        if (b < 32 || b > 126) {
                            cnt += 3;
                        } else if (b == 92) {
                            cnt++;
                        }
                    }
                    byte[] data = new byte[cnt];
                    for (int i = 0, j = 0; i < length; i++) {
                        byte b = bytes[i];
                        if (b < 32 || b > 126) {
                            data[j++] = '\\';
                            data[j++] = (byte) (((b >>> 6) & 3) + '0');
                            data[j++] = (byte) (((b >>> 3) & 7) + '0');
                            data[j++] = (byte) ((b & 7) + '0');
                        } else if (b == 92) {
                            data[j++] = '\\';
                            data[j++] = '\\';
                        } else {
                            data[j++] = b;
                        }
                    }
                    writeInt(data.length);
                    write(data);
                    break;
                }
                default:
                    byte[] data = v.getString().getBytes(getEncoding());
                    writeInt(data.length);
                    write(data);
            }
        } else {
            // binary
            switch (pgType) {
                case PgServer.PG_TYPE_INT2:
                    writeInt(2);
                    writeShort(v.getShort());
                    break;
                case PgServer.PG_TYPE_INT4:
                    writeInt(4);
                    writeInt(v.getInt());
                    break;
                case PgServer.PG_TYPE_INT8:
                    writeInt(8);
                    dataOut.writeLong(v.getLong());
                    break;
                case PgServer.PG_TYPE_FLOAT4:
                    writeInt(4);
                    dataOut.writeFloat(v.getFloat());
                    break;
                case PgServer.PG_TYPE_FLOAT8:
                    writeInt(8);
                    dataOut.writeDouble(v.getDouble());
                    break;
                case PgServer.PG_TYPE_BYTEA: {
                    byte[] data = v.getBytesNoCopy();
                    writeInt(data.length);
                    write(data);
                    break;
                }
                case PgServer.PG_TYPE_DATE:
                    writeInt(4);
                    writeInt((int) (toPostgreDays(((ValueDate) v).getDateValue())));
                    break;
                case PgServer.PG_TYPE_TIME:
                    writeTimeBinary(((ValueTime) v).getNanos(), 8);
                    break;
                case PgServer.PG_TYPE_TIMETZ: {
                    ValueTimeTimeZone t = (ValueTimeTimeZone) v;
                    long m = t.getNanos();
                    writeTimeBinary(m, 12);
                    dataOut.writeInt(-t.getTimeZoneOffsetSeconds());
                    break;
                }
                case PgServer.PG_TYPE_TIMESTAMP: {
                    ValueTimestamp t = (ValueTimestamp) v;
                    long m = toPostgreDays(t.getDateValue()) * 86_400;
                    long nanos = t.getTimeNanos();
                    writeTimestampBinary(m, nanos);
                    break;
                }
                case PgServer.PG_TYPE_TIMESTAMPTZ: {
                    ValueTimestampTimeZone t = (ValueTimestampTimeZone) v;
                    long m = toPostgreDays(t.getDateValue()) * 86_400;
                    long nanos = t.getTimeNanos() - t.getTimeZoneOffsetSeconds() * 1_000_000_000L;
                    if (nanos < 0L) {
                        m--;
                        nanos += DateTimeUtils.NANOS_PER_DAY;
                    }
                    writeTimestampBinary(m, nanos);
                    break;
                }
                default:
                    throw new IllegalStateException("output binary format is undefined");
            }
        }
    }

    public void writeTimeBinary(long m, int numBytes) throws IOException {
        writeInt(numBytes);
        if (INTEGER_DATE_TYPES) {
            // long format
            m /= 1_000;
        } else {
            // double format
            m = Double.doubleToLongBits(m * 0.000_000_001);
        }
        dataOut.writeLong(m);
    }

    public void writeTimestampBinary(long m, long nanos) throws IOException {
        writeInt(8);
        if (INTEGER_DATE_TYPES) {
            // long format
            m = m * 1_000_000 + nanos / 1_000;
        } else {
            // double format
            m = Double.doubleToLongBits(m + nanos * 0.000_000_001);
        }
        dataOut.writeLong(m);
    }

    public Charset getEncoding() {
        if ("UNICODE".equals(clientEncoding)) {
            return StandardCharsets.UTF_8;
        }
        return Charset.forName(clientEncoding);
    }

    private void setParameter(PreparedStatement prep,
                              int pgType, int i, int[] formatCodes) throws SQLException, IOException {
        boolean text = (i >= formatCodes.length) || (formatCodes[i] == 0);
        int col = i + 1;
        int paramLen = readInt();
        if (paramLen == -1) {
            prep.setNull(col, Types.NULL);
        } else if (text) {
            // plain text
            byte[] data = Utils.newBytes(paramLen);
            readFully(data);
            String str = new String(data, getEncoding());
            switch (pgType) {
                case PgServer.PG_TYPE_DATE: {
                    // Strip timezone offset
                    int idx = str.indexOf(' ');
                    if (idx > 0) {
                        str = str.substring(0, idx);
                    }
                    break;
                }
                case PgServer.PG_TYPE_TIME: {
                    // Strip timezone offset
                    int idx = str.indexOf('+');
                    if (idx <= 0) {
                        idx = str.indexOf('-');
                    }
                    if (idx > 0) {
                        str = str.substring(0, idx);
                    }
                    break;
                }
            }
            prep.setString(col, str);
        } else {
            // binary
            switch (pgType) {
                case PgServer.PG_TYPE_INT2:
                    checkParamLength(2, paramLen);
                    prep.setShort(col, readShort());
                    break;
                case PgServer.PG_TYPE_INT4:
                    checkParamLength(4, paramLen);
                    prep.setInt(col, readInt());
                    break;
                case PgServer.PG_TYPE_INT8:
                    checkParamLength(8, paramLen);
                    prep.setLong(col, dataIn.readLong());
                    break;
                case PgServer.PG_TYPE_FLOAT4:
                    checkParamLength(4, paramLen);
                    prep.setFloat(col, dataIn.readFloat());
                    break;
                case PgServer.PG_TYPE_FLOAT8:
                    checkParamLength(8, paramLen);
                    prep.setDouble(col, dataIn.readDouble());
                    break;
                case PgServer.PG_TYPE_BYTEA:
                    byte[] d1 = Utils.newBytes(paramLen);
                    readFully(d1);
                    prep.setBytes(col, d1);
                    break;
                default:
                    server.trace("Binary format for type: " + pgType + " is unsupported");
                    byte[] d2 = Utils.newBytes(paramLen);
                    readFully(d2);
                    prep.setString(col, new String(d2, getEncoding()));
            }
        }
    }

    private static void checkParamLength(int expected, int got) {
        if (expected != got) {
            throw DbException.getInvalidValueException("paramLen", got);
        }
    }

    public void sendErrorResponse(Exception re) throws IOException {
        SQLException e = DbException.toSQLException(re);
        server.traceError(e);
        startMessage('E');
        write('S');
        writeString("ERROR");
        write('C');
        writeString(e.getSQLState());
        write('M');
        writeString(e.getMessage());
        write('D');
        writeString(e.toString());
        write(0);
        sendMessage();
    }

    public void sendCancelQueryResponse() throws IOException {
        server.trace("CancelSuccessResponse");
        startMessage('E');
        write('S');
        writeString("ERROR");
        write('C');
        writeString("57014");
        write('M');
        writeString("canceling statement due to user request");
        write(0);
        sendMessage();
    }

    private void sendParameterDescription(ParameterMetaData meta,
                                          int[] paramTypes) throws Exception {
        int count = meta.getParameterCount();
        startMessage('t');
        writeShort(count);
        for (int i = 0; i < count; i++) {
            int type;
            if (paramTypes != null && paramTypes[i] != 0) {
                type = paramTypes[i];
            } else {
                type = PgServer.PG_TYPE_VARCHAR;
            }
            server.checkType(type);
            writeInt(type);
        }
        sendMessage();
    }

    public void sendNoData() throws IOException {
        startMessage('n');
        sendMessage();
    }

    private void sendRowDescription(ResultSetMetaData meta, int[] formatCodes) throws IOException, SQLException {
        if (meta == null) {
            sendNoData();
        } else {
            int columns = meta.getColumnCount();
            int[] types = new int[columns];
            int[] precision = new int[columns];
            String[] names = new String[columns];
            for (int i = 0; i < columns; i++) {
                String name = meta.getColumnName(i + 1);
                names[i] = name;
                int type = meta.getColumnType(i + 1);
                int pgType = PgServer.convertType(type);
                // the ODBC client needs the column pg_catalog.pg_index
                // to be of type 'int2vector'
                // if (name.equalsIgnoreCase("indkey") &&
                //         "pg_index".equalsIgnoreCase(
                //         meta.getTableName(i + 1))) {
                //     type = PgServer.PG_TYPE_INT2VECTOR;
                // }
                precision[i] = meta.getColumnDisplaySize(i + 1);
                if (type != Types.NULL) {
                    server.checkType(pgType);
                }
                types[i] = pgType;
            }
            startMessage('T');
            writeShort(columns);
            for (int i = 0; i < columns; i++) {
                writeString(StringUtils.toLowerEnglish(names[i]));
                // object ID
                writeInt(0);
                // attribute number of the column
                writeShort(0);
                // data type
                writeInt(types[i]);
                // pg_type.typlen
                writeShort(getTypeSize(types[i], precision[i]));
                // pg_attribute.atttypmod
                writeInt(-1);
                // the format type: text = 0, binary = 1
                writeShort(formatAsText(types[i], formatCodes, i) ? 0 : 1);
            }
            sendMessage();
        }
    }

    /**
     * Check whether the given type should be formatted as text.
     *
     * @param pgType      data type
     * @param formatCodes format codes, or {@code null}
     * @param column      0-based column number
     * @return true for text
     */
    public boolean formatAsText(int pgType, int[] formatCodes, int column) {
        boolean text = true;
        if (formatCodes != null && formatCodes.length > 0) {
            if (formatCodes.length == 1) {
                text = formatCodes[0] == 0;
            } else if (column < formatCodes.length) {
                text = formatCodes[column] == 0;
            }
        }
        return text;
    }

    private static int getTypeSize(int pgType, int precision) {
        switch (pgType) {
            case PgServer.PG_TYPE_BOOL:
                return 1;
            case PgServer.PG_TYPE_VARCHAR:
                return Math.max(255, precision + 10);
            default:
                return precision + 4;
        }
    }

    public void sendErrorResponse(String message) throws IOException {
        server.trace("Exception: " + message);
        startMessage('E');
        write('S');
        writeString("ERROR");
        write('C');
        // PROTOCOL VIOLATION
        writeString("08P01");
        write('M');
        writeString(message);
        sendMessage();
    }

    private void sendParseComplete() throws IOException {
        startMessage('1');
        sendMessage();
    }

    public void sendBindComplete() throws IOException {
        startMessage('2');
        sendMessage();
    }

    private void sendCloseComplete() throws IOException {
        startMessage('3');
        sendMessage();
    }

    private void initDb() throws SQLException {
        Statement stat = conn.createStatement();
        try {
            stat = conn.createStatement();
            stat.execute("set search_path = PUBLIC, pg_catalog");
            HashSet<Integer> typeSet = server.getTypeSet();
            if (typeSet.isEmpty()) {
                try (ResultSet rs = stat.executeQuery("select oid from pg_catalog.pg_type")) {
                    while (rs.next()) {
                        typeSet.add(rs.getInt(1));
                    }
                }
            }
        } finally {
            JdbcUtils.closeSilently(stat);
        }
    }

    /**
     * Close this connection.
     */
    void close() {
        try {
            stop = true;
            JdbcUtils.closeSilently(conn);
            if (socket != null) {
                socket.close();
            }
            server.trace("Close");
            streamHandler.close();
        } catch (Exception e) {
            server.traceError(e);
        }
        conn = null;
        socket = null;
        server.remove(this);
    }

    private void sendAuthenticationCleartextPassword() throws IOException {
        startMessage('R');
        writeInt(3);
        sendMessage();
    }

    private void sendAuthenticationOk() throws IOException {
        startMessage('R');
        writeInt(0);
        sendMessage();
        sendParameterStatus("client_encoding", clientEncoding);
        sendParameterStatus("DateStyle", dateStyle);
        sendParameterStatus("integer_datetimes", "off");
        sendParameterStatus("is_superuser", "off");
        sendParameterStatus("server_encoding", "SQL_ASCII");
        sendParameterStatus("server_version", Constants.PG_VERSION);
        sendParameterStatus("session_authorization", userName);
        sendParameterStatus("standard_conforming_strings", "off");
        // TODO PostgreSQL TimeZone
        sendParameterStatus("TimeZone", "CET");
        sendParameterStatus("integer_datetimes", INTEGER_DATE_TYPES ? "on" : "off");
        sendBackendKeyData();
        sendReadyForQuery();
    }

    private void sendReadyForQuery() throws IOException {
        startMessage('Z');
        char c;
        try {
            if (conn.getAutoCommit()) {
                // idle
                c = 'I';
            } else {
                // in a transaction block
                c = 'T';
            }
        } catch (SQLException e) {
            // failed transaction block
            c = 'E';
        }
        write((byte) c);
        sendMessage();
    }

    private void sendBackendKeyData() throws IOException {
        startMessage('K');
        writeInt(processId);
        writeInt(secret);
        sendMessage();
    }

    private void writeString(String s) throws IOException {
        writeStringPart(s);
        write(0);
    }

    private void writeStringPart(String s) throws IOException {
        write(s.getBytes(getEncoding()));
    }

    public void writeInt(int i) throws IOException {
        dataOut.writeInt(i);
    }

    public void writeShort(int i) throws IOException {
        dataOut.writeShort(i);
    }

    public void writeByte(char c) throws IOException {
        dataOut.writeByte(c);
    }

    public void write(byte[] data) throws IOException {
        dataOut.write(data);
    }

    private void write(int b) throws IOException {
        dataOut.write(b);
    }

    @Override
    public void startMessage(int newMessageType) {
        this.messageType = newMessageType;
        outBuffer = new ByteArrayOutputStream();
        dataOut = new DataOutputStream(outBuffer);
    }

    public void sendMessage() throws IOException {
        dataOut.flush();
        byte[] buff = outBuffer.toByteArray();
        int len = buff.length;
        dataOut = new DataOutputStream(out);
        dataOut.write(messageType);
        dataOut.writeInt(len + 4);
        dataOut.write(buff);
        dataOut.flush();
    }

    private void sendParameterStatus(String param, String value)
            throws IOException {
        startMessage('S');
        writeString(param);
        writeString(value);
        sendMessage();
    }

    void setThread(Thread thread) {
        this.thread = thread;
    }

    Thread getThread() {
        return thread;
    }

    void setProcessId(int id) {
        this.processId = id;
    }

    int getProcessId() {
        return this.processId;
    }

    public synchronized void setActiveRequest(JdbcStatement statement) {
        activeRequest = statement;
    }

    /**
     * Kill a currently running query on this thread.
     */
    private synchronized void cancelRequest() {
        if (activeRequest != null) {
            try {
                activeRequest.cancel();
                activeRequest = null;
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
        }
    }


    public Portal getPortal(String name) {
        return portals.get(name);
    }
}
