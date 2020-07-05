package com.steckytech.maxwell.producer;

import com.steckytech.maxwell.CDCEvent;
import com.steckytech.maxwell.DDLEvent;
import com.steckytech.maxwell.DatasetMap;
import com.steckytech.maxwell.cdc.MessageHandler;
import com.steckytech.maxwell.consumer.Collector;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.AbstractAsyncProducer;
import com.zendesk.maxwell.row.HeartbeatRowMap;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.ddl.DDLMap;

import javax.annotation.PreDestroy;
import java.util.Observable;
import java.util.Observer;

public class Stream extends AbstractAsyncProducer implements Observer {

    private DatasetMap config;
    private final Collector collector;


    public Stream(MaxwellContext context, DatasetMap config, MessageHandler handler) {
        super(context);
        this.config = config;
        this.collector = new Collector(context, handler);
        context.getHeartbeatNotifier().addObserver(this);
    }


    @PreDestroy
    private void destroy() {
        collector.stop();
    }

    @Override
    public void sendAsync(RowMap r, CallbackCompleter callback) throws Exception {

        String targetDataset = config.datasetFor(r.getDatabase());

        // enqueue new event to handle asynchronously
        if (r.getTable() != null) {
            r.setPartitionString(r.getDatabase() + "." + r.getTable());
        } else {
            r.setPartitionString(r.getDatabase());
        }
        if (r instanceof DDLMap) {
            collector.collect(new DDLEvent(context, targetDataset, (DDLMap)r, callback));
        } else {
            collector.collect(new CDCEvent(context, targetDataset, r, callback));
        }

    }

    @Override
    public void update(Observable o, Object arg) {
        HeartbeatRowMap hb = (HeartbeatRowMap) arg;
        System.out.println("Heartbeat: "+ hb);
    }
}
