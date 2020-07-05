package com.steckytech.maxwell.handler.stdout;

import com.steckytech.maxwell.CDCEvent;
import com.steckytech.maxwell.consumer.Handler;
import com.steckytech.maxwell.cdc.Processed;
import com.steckytech.maxwell.cdc.MessageType;

public class InsertRow extends Handler<CDCEvent> {

    public InsertRow() {
        super(MessageType.insert);
    }

    @Override
    public Processed apply(CDCEvent e) {
        System.out.println(e.toJSON());
        return new Processed(e);
    }

}
