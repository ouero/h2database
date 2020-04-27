package org.h2.server.pg;

import org.h2.command.CommandContainer;
import org.h2.command.CommandInterface;
import org.h2.command.dml.Insert;
import org.h2.jdbc.JdbcPreparedStatement;

import java.io.IOException;

public class BlackHoleHandler extends CustomHandler {
    public static String BLACK_HOLE_SUFFIX = "black_hole";

    public BlackHoleHandler(ReadWriteAble readWriteAble) {
        super(readWriteAble);
    }


    @Override
    public boolean filter(int x) throws IOException {
        if (!isFilter) {
            return false;
        }
        boolean filtered = false;
        switch (x) {
            case 'P':
                readWriteAble.sendParseComplete();
                filtered = true;
                break;
            case 'B':
                readWriteAble.sendBindComplete();
                filtered = true;
                break;
            case 'D':
                readWriteAble.sendNoData();
                filtered = true;
                break;
            case 'E':
                //if insert black hole,do not real handle data,direct return true
                readWriteAble.sendCommandComplete(CommandInterface.INSERT, 1);
                filtered = true;
                break;
            case 'C':
                readWriteAble.sendCloseComplete();
                filtered = true;
                break;
        }
        return filtered;
    }

    @Override
    public void setIsFilter(JdbcPreparedStatement prep){
        if (isInsertCommand(prep)) {
            String table = ((Insert) ((CommandContainer) prep.getCommand()).getPrepared()).getTable().getName();
            if (table.endsWith(BlackHoleHandler.BLACK_HOLE_SUFFIX)) {
               setFilter(true);
            }
        } else {
            setFilter(false);
        }
    }

    private boolean isInsertCommand(JdbcPreparedStatement prep) {
        return prep.getCommand() instanceof CommandContainer && ((CommandContainer) prep.getCommand()).getPrepared() instanceof Insert;
    }

}
