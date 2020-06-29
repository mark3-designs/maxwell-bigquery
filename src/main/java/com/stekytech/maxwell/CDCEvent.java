package com.stekytech.maxwell;

import com.zendesk.maxwell.producer.AbstractAsyncProducer;
import com.zendesk.maxwell.row.RowMap;

public class CDCEvent {

    public final String dataset;
    public final RowMap data;
    public final AbstractAsyncProducer.CallbackCompleter completer;

    public CDCEvent(String dataset, RowMap data, AbstractAsyncProducer.CallbackCompleter completer) {
        this.dataset = dataset;
        this.data = data;
        this.completer = completer;
    }

    public void commit() {
        completer.markCompleted();
    }

}
