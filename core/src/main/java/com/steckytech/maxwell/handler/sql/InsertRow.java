package com.steckytech.maxwell.handler.sql;

import com.steckytech.maxwell.CDCEvent;
import com.steckytech.maxwell.cdc.MessageType;
import com.steckytech.maxwell.cdc.Processed;
import com.steckytech.maxwell.consumer.Handler;

import static org.ncr.SqlSanitizer.escapeAndQuote;

public class InsertRow extends Handler<CDCEvent> {

    public InsertRow() {
        super(MessageType.insert);
    }

    @Override
    public Processed apply(CDCEvent e) {

        StringBuilder statement = new StringBuilder("insert into `"+ e.table +"` (%columns) values (%values);");

        String comma = "";
        for (String column : e.row.keySet()) {
            Object value = e.row.get(column);
            statement.append(comma).append("`").append(column).append("`")
                    .append("=").append(escapeAndQuote(value));
            comma = ", ";

        }

        System.out.println(statement);

        return new Processed(e);
    }

}
