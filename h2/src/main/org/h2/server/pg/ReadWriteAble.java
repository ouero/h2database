package org.h2.server.pg;

import org.h2.jdbc.JdbcPreparedStatement;
import org.h2.jdbc.JdbcStatement;

import java.io.IOException;
import java.nio.charset.Charset;

public interface ReadWriteAble {
    void sendParseComplete() throws IOException;

    void sendBindComplete() throws IOException;

    void sendNoData() throws IOException;

    void sendCommandComplete(int insert, int i) throws IOException;

    void sendCommandComplete(JdbcStatement jdbcStatement, int i) throws IOException;

    void writeInt(int i) throws IOException;

    Charset getEncoding();

    void write(byte[] data) throws IOException;

    void sendMessage() throws IOException;

    boolean formatAsText(int pgType, int[] formatCodes, int i);

    void writeShort(int columns) throws IOException;

    void startMessage(int messageType);

    void  setActiveRequest(JdbcStatement jdbcStatement);

    void sendErrorResponse(Exception e) throws IOException;

    void sendErrorResponse(String s) throws IOException;

    void sendCancelQueryResponse() throws IOException;

    void sendPortalSuspended() throws IOException;

    int readInt() throws IOException;

    Portal getPortal(String name);

    String readString() throws IOException;

    void writeByte(char aTrue) throws IOException;

    void sendCloseComplete() throws IOException;
}
