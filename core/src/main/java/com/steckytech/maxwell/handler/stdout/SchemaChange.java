package com.steckytech.maxwell.handler.stdout;

import com.steckytech.maxwell.cdc.MessageType;
import com.steckytech.maxwell.consumer.Handler;
import com.steckytech.maxwell.DDLEvent;
import com.steckytech.maxwell.cdc.Processed;

public class SchemaChange extends Handler<DDLEvent> {

    public SchemaChange() {
        super(MessageType.ddl);
    }

    @Override
    public Processed apply(DDLEvent e) {
        System.out.println(e.toJSON());
        return new Processed(e);
    }

}
