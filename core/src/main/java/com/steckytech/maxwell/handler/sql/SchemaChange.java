package com.steckytech.maxwell.handler.sql;

import com.steckytech.maxwell.DDLEvent;
import com.steckytech.maxwell.cdc.MessageType;
import com.steckytech.maxwell.cdc.Processed;
import com.steckytech.maxwell.consumer.Handler;

public class SchemaChange extends Handler<DDLEvent> {

    public SchemaChange() {
        super(MessageType.ddl);
    }

    @Override
    public Processed apply(DDLEvent e) {
        System.out.println(e);
        return new Processed(e);
    }

}
