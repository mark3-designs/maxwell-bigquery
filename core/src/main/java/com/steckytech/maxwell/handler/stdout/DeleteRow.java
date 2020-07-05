package com.steckytech.maxwell.handler.stdout;

import com.steckytech.maxwell.CDCEvent;
import com.steckytech.maxwell.consumer.Handler;
import com.steckytech.maxwell.cdc.Processed;
import com.steckytech.maxwell.cdc.MessageType;

public class DeleteRow extends Handler<CDCEvent> {

    public DeleteRow() {
        super(MessageType.delete);
    }

    @Override
    public Processed apply(CDCEvent e) {
        String key = e.partition;
        System.out.println("partition: "+ key);
        System.out.println(getClass().getSimpleName() +"\n"+ e);
        return new Processed(e);
    }

}
