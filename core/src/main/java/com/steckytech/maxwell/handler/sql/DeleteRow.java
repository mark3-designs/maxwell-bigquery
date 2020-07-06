package com.steckytech.maxwell.handler.sql;

import com.steckytech.maxwell.CDCEvent;
import com.steckytech.maxwell.cdc.MessageType;
import com.steckytech.maxwell.cdc.Processed;
import com.steckytech.maxwell.consumer.Handler;
import org.ncr.SqlSanitizer;

import static org.ncr.SqlSanitizer.escapeAndQuote;

public class DeleteRow extends Handler<CDCEvent> {

    public DeleteRow() {
        super(MessageType.delete);
    }

    @Override
    public Processed apply(CDCEvent e) {
        String key = e.partition;

        StringBuilder statement = new StringBuilder("delete from `"+ e.table +"` where ");

        String and = "";
        for (String column : e.getPrimaryKey()) {
            Object value = e.row.get(column);
            statement.append(and).append("`").append(column).append("`")
                    .append("=").append(SqlSanitizer.escapeAndQuote(value));
            and = " and ";

        }

        System.out.println(statement);

        return new Processed(e);
    }

}
