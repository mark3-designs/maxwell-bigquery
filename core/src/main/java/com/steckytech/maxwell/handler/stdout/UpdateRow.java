package com.steckytech.maxwell.handler.stdout;

import com.steckytech.maxwell.CDCEvent;
import com.steckytech.maxwell.consumer.Handler;
import com.steckytech.maxwell.cdc.Processed;
import com.steckytech.maxwell.cdc.MessageType;

public class UpdateRow extends Handler<CDCEvent> {

    public UpdateRow() {
        super(MessageType.update);
    }

    @Override
    public Processed apply(CDCEvent e) {
        System.out.println(e.toJSON());
        return new Processed(e);
    }

}
