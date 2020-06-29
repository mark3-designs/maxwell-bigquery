package com.stekytech.maxwell.producer;

import com.stekytech.maxwell.DatasetMap;
import com.stekytech.maxwell.CDCEvent;
import com.stekytech.maxwell.microbatch.Collector;
import com.stekytech.maxwell.microbatch.Processor;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.AbstractAsyncProducer;
import com.zendesk.maxwell.row.RowMap;

import java.util.ArrayList;
import java.util.List;

public class BigQuery extends AbstractAsyncProducer {

    private DatasetMap config;
    private final Collector collector;
    private List<Processor> processors = new ArrayList<>();


    public BigQuery(MaxwellContext context, DatasetMap config) {
        super(context);
        this.config = config;
        this.collector = new Collector(1, 5);
    }


    @Override
    public void sendAsync(RowMap r, CallbackCompleter callback) throws Exception {

        String targetDataset = config.datasetFor(r.getDatabase());

        // enqueue new CDC event to handle asynchronously
        collector.collect(new CDCEvent(targetDataset, r, callback));

    }
}
