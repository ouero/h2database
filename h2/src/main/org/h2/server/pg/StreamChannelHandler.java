package org.h2.server.pg;

import org.h2.command.CommandContainer;
import org.h2.command.query.Select;
import org.h2.expression.Expression;
import org.h2.expression.ValueExpression;
import org.h2.jdbc.JdbcPreparedStatement;
import org.h2.jdbc.JdbcResultSet;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.Queue;

public class StreamChannelHandler extends StreamHandler {
    private SocketChannel socketChannel;
    private DataInputStream indexIn;
    private RandomAccessFile randomAccessFile;
    private FileChannel channel;
    private Queue<FilePosition> indexQueue = new LinkedList();
    private int indexQueueSize = 50;
    String indexName;
    private long currentIndexPosition = 0;

    public StreamChannelHandler(ReadWriteAble readWriteAble) {
        super(readWriteAble);
    }

    @Override
    public boolean filter(int x) throws IOException {
        if (!isFilter) {
            return false;
        }
        boolean filtered = false;
        switch (x) {
            case 'E': {
                String name = readWriteAble.readString();
                Portal p = readWriteAble.getPortal(name);
                if (p == null) {
                    readWriteAble.sendErrorResponse("Portal not found: " + name);
                    break;
                }
                Prepared prepared = p.prep;
                JdbcPreparedStatement prep = prepared.prep;
                //this is bug,ref "driver org.postgresql.core.v3.QueryExecutorImpl.sendExecute()"
//                int maxRows = readShort();
                int fetchSize = readWriteAble.readInt();
                //bug fix end

                Expression limitExpression = ((Select) ((CommandContainer) prep.getCommand()).getPrepared()).getLimit();
                long limit = 0;
                if (limitExpression != null && limitExpression instanceof ValueExpression) {
                    limit = ((ValueExpression) limitExpression).getValue(null).getLong();
                }
                try {
                    prep.setMaxRows(1);//we only need rs.ResultSetMetaData
                    readWriteAble.setActiveRequest(prep);
                    if (isFirstSelect()) {
                        prep.execute();
                    }
                    try {
                        if (isFirstSelect()) {
                            rs = (JdbcResultSet) prep.getResultSet();
                            fileName = tableName + ".data";
                            randomAccessFile = new RandomAccessFile(fileName, "r");
                            indexName = tableName + ".index";
                            indexIn = new DataInputStream(new FileInputStream(indexName));
                            channel = randomAccessFile.getChannel();
                            totalResultRows = 0;
                            if (limit != 0) {
                                totalResultRows = limit;
                            } else {
                                totalResultRows = 100_0000_0000L;
                            }
                        }
                        if (indexQueue.isEmpty()) {
                            while (indexQueue.size() < indexQueueSize) {
                                int batchlength = 0;
                                for (int i = 0; i < fetchSize; i++) {
                                    short aShort = 0;
                                    try {
                                        aShort = indexIn.readShort();
                                    } catch (EOFException e) {
                                        indexIn = new DataInputStream(new FileInputStream(indexName));
                                        currentIndexPosition = 0;
                                        aShort = indexIn.readShort();
                                    }
                                    batchlength = batchlength + aShort + 32768;
                                }
                                FilePosition filePosition = new FilePosition(currentIndexPosition, batchlength);
                                currentIndexPosition = currentIndexPosition + batchlength;
                                indexQueue.add(filePosition);
                            }
                        }
                        long thisFetch = Math.min(cursor + fetchSize, totalResultRows);
                        FilePosition filePosition = indexQueue.poll();
                        channel.transferTo(filePosition.position, filePosition.len, socketChannel);
                        cursor = cursor + fetchSize;
                        if (cursor >= totalResultRows) {
                            close();
                            readWriteAble.sendCommandComplete(prep, 0);
                        } else {
                            readWriteAble.sendPortalSuspended();
                        }
                    } catch (Exception e) {
                        close();
                        readWriteAble.sendErrorResponse(e);
                    }
                } catch (Exception e) {
                    close();
                    if (prep.isCancelled()) {
                        readWriteAble.sendCancelQueryResponse();
                    } else {
                        readWriteAble.sendErrorResponse(e);
                    }
                } finally {
                    readWriteAble.setActiveRequest(null);

                }
                filtered = true;
                break;
            }
        }
        return filtered;
    }

    @Override
    protected boolean isFirstSelect() {
        return super.isFirstSelect();
    }

    @Override
    public void setIsFilter(JdbcPreparedStatement prep) {
        super.setIsFilter(prep);
    }

    @Override
    public boolean isSelectCommand(JdbcPreparedStatement prep) {
        return super.isSelectCommand(prep);
    }

    @Override
    public void close() throws IOException {
        super.close();
        if (randomAccessFile != null) {
            randomAccessFile.close();
        }
        if (indexIn != null) {
            indexIn.close();
        }
    }

    public void setSocketChannel(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
    }

    static class FilePosition {
        long position;
        long len;

        public FilePosition(long position, long len) {
            this.position = position;
            this.len = len;
        }
    }
}
