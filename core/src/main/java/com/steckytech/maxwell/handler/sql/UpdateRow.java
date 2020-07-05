package com.steckytech.maxwell.handler.sql;

import com.steckytech.maxwell.CDCEvent;
import com.steckytech.maxwell.cdc.MessageType;
import com.steckytech.maxwell.cdc.Processed;
import com.steckytech.maxwell.consumer.Handler;
import org.ncr.SqlSanitizer;

import static org.ncr.SqlSanitizer.escapeAndQuote;

public class UpdateRow extends Handler<CDCEvent> {

    public UpdateRow() {
        super(MessageType.update);
    }

    @Override
    public Processed apply(CDCEvent e) {
        StringBuilder statement = new StringBuilder("update `"+ e.table +"` set %new_values where");

        String and = "";
        for (String column : e.row.keySet()) {
            Object value = e.row.get(column);
            statement.append(and).append("`").append(column).append("`")
                    .append("=").append(SqlSanitizer.escapeAndQuote(value));
            and = " and ";

        }

        System.out.println(statement);
        return new Processed(e);
    }

}
